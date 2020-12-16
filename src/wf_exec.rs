//! Manages execution of a workflow

use crate::WfAction;
use crate::WfContext;
use crate::WfError;
use crate::WfId;
use crate::WfLog;
use crate::WfOutput;
use crate::WfResult;
use crate::Workflow;
use anyhow::anyhow;
use core::future::Future;
use core::pin::Pin;
use core::task::Context;
use core::task::Poll;
use futures::future::BoxFuture;
use futures::FutureExt;
use petgraph::graph::NodeIndex;
use petgraph::Graph;
use petgraph::Incoming;
use petgraph::Outgoing;
use std::collections::BTreeMap;
use std::sync::Arc;
use uuid::Uuid;

/**
 * Executes a workflow
 *
 * `WfExecutor` implements `Future`.  To execute a workflow, use
 * [`WfExecutor::new`] and then `await` the resulting Future.
 */
/*
 * TODO Lots more could be said here, but the basic idea matches distributed
 * sagas.
 * This will be a good place to put things like concurrency limits, canarying,
 * etc.
 */
pub struct WfExecutor {
    /* See `Workflow` */
    graph: Graph<String, ()>,
    launchers: BTreeMap<NodeIndex, Box<dyn WfAction>>,
    node_names: BTreeMap<NodeIndex, String>,
    root: NodeIndex,

    /** Unique identifier for this execution */
    // TODO The nomenclature is problematic here.  This is really a workflow
    // _execution_ id.  Or maybe Workflows are really WorkflowTemplates?  Either
    // way, this identifies something different than what we currently call
    // Workflows.
    workflow_id: WfId,
    /** Persistent state */
    wflog: WfLog,

    /** Futures associated with currently-running nodes */
    running: BTreeMap<NodeIndex, BoxFuture<'static, WfResult>>,
    /** Outputs stored for nodes that have finished already */
    finished: BTreeMap<NodeIndex, WfOutput>,
    /** Nodes that have not started but whose dependencies are satisfied */
    ready: Vec<NodeIndex>,
    /** Last node that completed. */
    /*
     * TODO This is really janky.  It's here just to have a way to get the
     * output of the workflow as a whole based on the output of the last node
     * completed.
     */
    last_finished: NodeIndex,

    /** First error produced by a node, if any */
    /* TODO probably better as a state enum.  See poll(). */
    error: Option<WfError>,
}

impl WfExecutor {
    /** Create an executor to run the given workflow. */
    pub fn new(w: Workflow) -> WfExecutor {
        let workflow_id = Uuid::new_v4();
        // TODO "myself" here should be a hostname or other identifier for this
        // instance.
        let wflog = WfLog::new("myself", workflow_id);

        WfExecutor {
            graph: w.graph,
            launchers: w.launchers,
            node_names: w.node_names,
            root: w.root,
            workflow_id,
            wflog,
            last_finished: w.root,
            running: BTreeMap::new(),
            finished: BTreeMap::new(),
            ready: vec![w.root],
            error: None,
        }
    }

    /**
     * Builds the "ancestor tree" for a node whose dependencies have all
     * completed.
     *
     * The ancestor tree for a node is a map whose keys are strings that
     * identify ancestor nodes in the graph and whose values represent the
     * outputs from those nodes.  This is used by [`WfContext::lookup`].  See
     * where we use this function in poll() for more details.
     */
    fn make_ancestor_tree(
        &self,
        tree: &mut BTreeMap<String, WfOutput>,
        node: NodeIndex,
    ) {
        let ancestors = self.graph.neighbors_directed(node, Incoming);
        for ancestor in ancestors {
            self.make_ancestor_tree_node(tree, ancestor);
        }
    }

    fn make_ancestor_tree_node(
        &self,
        tree: &mut BTreeMap<String, WfOutput>,
        node: NodeIndex,
    ) {
        if node == self.root {
            return;
        }

        let name = self.node_names[&node].to_string();
        let output = self
            .finished
            .get(&node)
            .expect("cannot make ancestor tree on unfinished node");
        tree.insert(name, Arc::clone(output));
        self.make_ancestor_tree(tree, node);
    }
}

impl Future for WfExecutor {
    type Output = WfResult;

    fn poll<'a>(
        mut self: Pin<&'a mut WfExecutor>,
        cx: &'a mut Context<'_>,
    ) -> Poll<WfResult> {
        let mut recheck = false;

        assert!(self.finished.len() <= self.graph.node_count());

        if let Some(_) = &self.error {
            // TODO We'd like to emit the error that we saved here but we still
            // hold a reference to it.  Maybe use take()?  But that leaves the
            // internal state rather confused.  Maybe there should be a separate
            // boolean for whether an error has been recorded.
            return Poll::Ready(Err(anyhow!("workflow failed")));
        }

        if self.finished.len() == self.graph.node_count() {
            // TODO Besides being janky, this will probably blow up for the case
            // of an empty workflow.
            return Poll::Ready(Ok(Arc::clone(
                &self.finished[&self.last_finished],
            )));
        }

        /*
         * If there's nothing running and nothing ready to run and we're still
         * not finished and haven't encountered an error, something has gone
         * seriously wrong.
         */
        if self.running.is_empty() && self.ready.is_empty() {
            panic!("workflow came to rest without having finished");
        }

        /*
         * If any of the tasks we currently think are running have now finished,
         * walk their dependents and potentially mark them ready to run.
         * TODO Is polling on _everything_ again really the right way to do
         * this?  I'm basically following what futures::join! does.
         */
        let newly_finished = {
            let mut newly_finished = Vec::new();

            for (node, fut) in &mut self.running {
                if let Poll::Ready(result) = fut.poll_unpin(cx) {
                    recheck = true;
                    newly_finished.push((*node, result));
                }
            }

            newly_finished
        };

        for (node, result) in newly_finished {
            self.running.remove(&node);

            if let Err(error) = result {
                /*
                 * We currently assume errors are fatal.  That's not
                 * necessarily right.
                 * TODO how do we end right now?
                 * TODO we'll need to clean up too!
                 * TODO want to record which node generated this error.
                 */
                self.error = Some(error)
            } else {
                let output = result.unwrap();
                self.finished
                    .insert(node, output)
                    .expect_none("node finished twice");
                let mut newly_ready = Vec::new();

                for depnode in self.graph.neighbors_directed(node, Outgoing) {
                    /*
                     * Check whether all of this node's incoming edges are
                     * now satisfied.
                     */
                    let mut okay = true;
                    for upstream in
                        self.graph.neighbors_directed(depnode, Incoming)
                    {
                        if !self.finished.contains_key(&upstream) {
                            okay = false;
                            break;
                        }
                    }

                    if okay {
                        newly_ready.push(depnode);
                    }
                }

                // TODO It'd be nice to do this inline above, but we cannot
                // borrow self.graph in order to iterate over
                // neighbors_directed() while also borrowing self.ready mutably.
                for depnode in newly_ready {
                    self.ready.push(depnode);
                }

                self.last_finished = node;
            }
        }

        if self.error.is_none() {
            let to_schedule = self.ready.drain(..).collect::<Vec<NodeIndex>>();
            for node in to_schedule {
                let wfaction = self
                    .launchers
                    .remove(&node)
                    .expect("missing action for node");
                // TODO we could be much more efficient without copying this
                // tree each time.
                let mut ancestor_tree = BTreeMap::new();
                self.make_ancestor_tree(&mut ancestor_tree, node);

                let fut = wfaction.do_it(WfContext {
                    ancestor_tree,
                });
                let boxed = fut.boxed();
                self.running.insert(node, boxed);
                recheck = true;
            }
        }

        /*
         * If we completed any outstanding work, we need to re-check the end
         * conditions.  If we dispatched any new work, we need to poll on those
         * futures.  We could do either of those right here, but we'd have to
         * duplicate code above.  It's easier to just invoke ourselves again
         * with a tail call.  Of course, we don't want to do this if nothing
         * changed, or we'll recurse indefinitely!
         * TODO This isn't ideal, since we'll wind up polling again on anything
         * that was already running.
         */
        if recheck {
            self.poll(cx)
        } else {
            Poll::Pending
        }
    }
}
