//! Manages execution of a workflow

use crate::wf_log::WfNodeEventType;
use crate::wf_log::WfNodeLoadStatus;
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
use futures::channel::mpsc;
use futures::future::BoxFuture;
use futures::lock::Mutex;
use futures::FutureExt;
use futures::StreamExt;
use petgraph::graph::NodeIndex;
use petgraph::visit::Topo;
use petgraph::visit::Walker;
use petgraph::Graph;
use petgraph::Incoming;
use petgraph::Outgoing;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use uuid::Uuid;

/**
 * Execution state for a workflow node
 * TODO ASCII version of the pencil-and-paper diagram?
 * TODO There are several substates not currently used because we have no way to
 * store them in the place they would go.  For example, we want to use
 * "Starting" when we spawn the tokio task to process an action.  And we want to
 * transition to "Running" when we've successfully recorded this action and
 * kicked it off.  However, the state is only stored in the WfExecutor, and we
 * can't mutate that (or even reference it) from the spawned task.  We could
 * send a message over the channel, but then the state update is async and may
 * not reflect reality.  We could use a Mutex, but it feels heavyweight.  Still,
 * that may be the way to go.
 * TODO Several other states are currently unused because we haven't implemented
 * unwinding yet.
 */
#[derive(Debug, Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
#[allow(dead_code)]
enum WfNodeState {
    Blocked,
    Ready,
    Starting,
    Running,
    Finishing,
    Done,
    Failing,
    Failed,
    StartingCancel,
    Cancelling,
    FinishingCancel,
    Cancelled,
}

/**
 * Execution state for the workflow overall
 */
#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
enum WfState {
    Running,
    #[allow(dead_code)] // TODO Not yet implemented
    Unwinding,
    Done,
}

impl fmt::Display for WfNodeState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            WfNodeState::Blocked => "blocked",
            WfNodeState::Ready => "ready",
            WfNodeState::Starting => "starting",
            WfNodeState::Running => "running",
            WfNodeState::Finishing => "finishing",
            WfNodeState::Done => "done",
            WfNodeState::Failing => "failing",
            WfNodeState::Failed => "failed",
            WfNodeState::StartingCancel => "starting_cancel",
            WfNodeState::Cancelling => "cancelling",
            WfNodeState::FinishingCancel => "finishing_cancel",
            WfNodeState::Cancelled => "cancelled",
        })
    }
}

struct TaskCompletion {
    node_id: NodeIndex,
    result: WfResult,
}

/**
 * Executes a workflow
 *
 * Call `WfExecutor.wait_for_finish()` to get a Future.  You must `await` this
 * Future to actually execute the workflow.
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
    wflog: Arc<Mutex<WfLog>>,

    /** Overall execution state */
    exec_state: WfState,
    /** Execution state for each node in the graph */
    node_states: BTreeMap<NodeIndex, WfNodeState>,
    /** Outstanding tasks for each node in the graph */
    node_tasks: BTreeMap<NodeIndex, JoinHandle<()>>,
    /** Nodes that have not started but whose dependencies are satisfied */
    ready: Vec<NodeIndex>,
    /** Outputs saved by completed actions. */
    node_outputs: BTreeMap<NodeIndex, WfOutput>,

    /** Channel for receiving completion messages from nodes. */
    tx: mpsc::Sender<TaskCompletion>,
    /** Channel for receiving completion messages from nodes. */
    rx: mpsc::Receiver<TaskCompletion>,

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

    /** Channel for monitoring execution completion */
    finish_tx: broadcast::Sender<WfResult>,

    /** Channel for monitoring state changes */
    state_change_tx: broadcast::Sender<()>,
    /** Channel for monitoring state changes */
    state_change_rx: broadcast::Receiver<()>,
}

impl WfExecutor {
    /** Create an executor to run the given workflow. */
    pub fn new(w: Workflow) -> WfExecutor {
        let workflow_id = Uuid::new_v4();
        // TODO "myself" here should be a hostname or other identifier for this
        // instance.
        let wflog = Arc::new(Mutex::new(WfLog::new("myself", workflow_id)));
        let mut node_states = BTreeMap::new();

        /*
         * In practice, each node can enqueue only two messages in its lifetime:
         * one for completion of the action, and one for completion of the
         * compensating action.  We bound this channel's size at twice the graph
         * node count for this worst case.
         */
        let (tx, rx) = mpsc::channel(2 * w.graph.node_count());
        let (finish_tx, _) = broadcast::channel(1);
        let (state_change_tx, state_change_rx) = broadcast::channel(1);

        node_states.insert(w.root, WfNodeState::Ready);

        WfExecutor {
            graph: w.graph,
            launchers: w.launchers,
            node_names: w.node_names,
            root: w.root,
            workflow_id,
            wflog,
            exec_state: WfState::Running,
            node_states,
            node_tasks: BTreeMap::new(),
            node_outputs: BTreeMap::new(),
            ready: vec![w.root],
            tx,
            rx,
            last_finished: w.root,
            error: None,
            finish_tx,
            state_change_tx,
            state_change_rx,
        }
    }

    /**
     * Create an executor to run the given workflow that may have already
     * started, using the given log events.
     */
    // TODO the panics and assertions in this function should be operational
    // errors instead.
    pub fn new_recover(w: Workflow, wflog: WfLog) -> WfExecutor {
        let workflow_id = wflog.workflow_id;
        let mut node_states = BTreeMap::new();
        let topo_visitor = Topo::new(&w.graph);
        let mut done = false;
        let mut ready = Vec::new();
        let mut node_outputs = BTreeMap::new();
        let mut last_finished = w.root;
        let mut nunfinished = 0;

        for node in topo_visitor.iter(&w.graph) {
            assert!(!node_states.contains_key(&node));
            assert!(!node_outputs.contains_key(&node));

            let node_status = wflog.load_status_for_node(node.index() as u64);
            if let WfNodeLoadStatus::NeverStarted = node_status {
                nunfinished += 1;
                done = true;
                continue;
            }

            if done {
                panic!(
                    "encountered node in invalid state topologically \
                    after one that never started (state={:?})",
                    node_status
                );
            }

            match node_status {
                WfNodeLoadStatus::NeverStarted => (), /* handled earlier */
                WfNodeLoadStatus::Started => {
                    // TODO-correctness should not log another "start" record
                    // TODO-robustness check that parents have been satisfied
                    /* XXX This would be a good place for a debug log. */
                    nunfinished += 1;
                    node_states.insert(node, WfNodeState::Ready);
                    ready.push(node);
                }
                WfNodeLoadStatus::Succeeded(o) => {
                    /* XXX This would be a good place for a debug log. */
                    node_states.insert(node, WfNodeState::Done);
                    node_outputs.insert(node, Arc::clone(o));
                    last_finished = node;
                }
                _ => {
                    // nunfinished += 1; // XXX re-insert when we remove todo!
                    todo!("handle node state");
                }
            }
        }

        let wflog = Arc::new(Mutex::new(wflog));
        let exec_state = if nunfinished == 0 {
            assert_eq!(node_outputs.len(), w.graph.node_count());
            assert_eq!(node_states.len(), w.graph.node_count());
            assert!(ready.is_empty());
            WfState::Done
        } else {
            WfState::Running
        };

        /* See new(). */
        let (tx, rx) = mpsc::channel(2 * w.graph.node_count());
        let (finish_tx, _) = broadcast::channel(1);
        let (state_change_tx, state_change_rx) = broadcast::channel(1);

        WfExecutor {
            graph: w.graph,
            launchers: w.launchers,
            node_names: w.node_names,
            root: w.root,
            workflow_id,
            wflog,
            exec_state,
            node_states,
            node_tasks: BTreeMap::new(),
            node_outputs,
            ready,
            tx,
            rx,
            last_finished,
            error: None,
            finish_tx,
            state_change_tx,
            state_change_rx,
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
        let node_state =
            self.node_states.get(&node).unwrap_or(&WfNodeState::Blocked);
        /*
         * If we're in this function, it's because we're looking at the ancestor
         * of a node that's currently "Running".  All such ancestors must be
         * "Done".  If they had never reached "Done", then we should never have
         * started working on the current node.  If they were "Done" but moved
         * on to "StartingCancel" or later, then that implies we've already
         * finished cancelling descendants, which would include the current
         * node.
         */
        assert_eq!(*node_state, WfNodeState::Done);
        let output = &self.node_outputs[&node];
        tree.insert(name, Arc::clone(output));
        self.make_ancestor_tree(tree, node);
    }

    /**
     * Wrapper for WfLog.record_now() that maps internal node indexes to stable
     * node ids.
     */
    // TODO Consider how we do map internal node indexes to stable node ids.
    // TODO clean up this interface
    // TODO Decide what we want to do if this actually fails and handle it
    // properly.
    fn record_now(
        wflog: Arc<Mutex<WfLog>>,
        node: NodeIndex,
        event_type: WfNodeEventType,
    ) -> BoxFuture<'static, ()> {
        let node_id = node.index() as u64;
        async move {
            let mut wflog = wflog.lock().await;
            wflog.record_now(node_id, event_type).await.unwrap()
        }
        .boxed()
    }

    /**
     * Returns a copy of the current log for this workflow.  This is intended
     * for testing recovery.
     */
    async fn snapshot(&self) -> WfLog {
        let wflog = self.wflog.lock().await;
        wflog.clone()
    }

    /**
     * Advances execution of the WfExecutor.
     */
    fn pump() {}

    /**
     * Returns a Future that waits for this executor to finish.
     */
    // TODO-cleanup Is this the idiomatic way to do this?  It seems like people
    // generally return a specific struct that impl's Future, but that seems a
    // lot harder to do here.
    fn wait_for_finish(&self) -> impl Future<Output = WfResult> {
        let mut rx = self.finish_tx.subscribe();
        async move {
            rx.recv().await.expect(
                "unexpected receive error on workflow completion channel",
            )
        }
    }
}

impl Future for WfExecutor {
    type Output = WfResult;

    fn poll<'a>(
        mut self: Pin<&'a mut WfExecutor>,
        cx: &'a mut Context<'_>,
    ) -> Poll<WfResult> {
        if let Some(_) = &self.error {
            // TODO We'd like to emit the error that we saved here but we still
            // hold a reference to it.  Maybe use take()?  But that leaves the
            // internal state rather confused.  Maybe there should be a separate
            // boolean for whether an error has been recorded.
            return Poll::Ready(Err(Arc::new(anyhow!(
                "workflow {} failed",
                self.workflow_id
            ))));
        }

        if let WfState::Done = self.exec_state {
            // TODO Besides being janky, this will probably blow up for the case
            // of an empty workflow.
            let node = &self.last_finished;
            let node_state = &self.node_states[node];
            assert_eq!(*node_state, WfNodeState::Done);
            let output = &self.node_outputs[&node];
            return Poll::Ready(Ok(Arc::clone(output)));
        }

        /*
         * Process any messages available on our channel.
         * TODO it seems like we should be writing this Future as an async
         * function or the like instead of using this interface.
         */
        assert_eq!(self.exec_state, WfState::Running);
        let mut newly_ready = Vec::new();
        while let Poll::Ready(maybe_message) = self.rx.poll_next_unpin(cx) {
            /*
             * It shouldn't be possible to get None back here.  That would mean
             * that all of the consumers have closed their ends, but we still
             * have a consumer of our own in self.tx.
             */
            assert!(maybe_message.is_some());
            let message = maybe_message.unwrap();
            let node = message.node_id;
            let old_state = self
                .node_states
                .remove(&node)
                .expect("node finished that was not running");
            assert_eq!(old_state, WfNodeState::Running);
            /*
             * It would be nice to join on this task here, but we don't know for
             * sure it's completed yet.  (It should be imminently, but we can't
             * wait in this context.)
             */
            self.node_tasks.remove(&node).expect("no task for completed node");

            let new_state = if let Ok(output) = message.result {
                self.node_outputs.insert(node, Arc::clone(&output));
                WfNodeState::Done
            } else {
                self.error = Some(message.result.unwrap_err());
                todo!(); // TODO trigger unwind!
                         // WfNodeState::Failed
            };

            self.node_states.insert(node, new_state);
            self.last_finished = node;

            if new_state == WfNodeState::Failed {
                continue;
            }

            assert_eq!(new_state, WfNodeState::Done);

            for depnode in self.graph.neighbors_directed(node, Outgoing) {
                /*
                 * Check whether all of this node's incoming edges are now
                 * satisfied.
                 */
                let mut okay = true;
                for upstream in self.graph.neighbors_directed(depnode, Incoming)
                {
                    let node_state = self.node_states[&upstream];
                    // XXX more general in the case of failure?
                    if node_state != WfNodeState::Done {
                        okay = false;
                        break;
                    }
                }

                if okay {
                    newly_ready.push(depnode);
                }
            }
        }

        /*
         * Kick off any nodes that are ready to run.  (Right now, we kick off
         * everything, so it might seem unnecessary to store this vector in
         * "self" to begin with.  However, the intent is to add capacity limits,
         * in which case we may return without having scheduled everything, and
         * we want to track whatever's still ready to go.)
         * TODO revisit dance with the vec to satisfy borrow rules
         * TODO implement unwinding
         */
        for node in newly_ready {
            self.node_states
                .insert(node, WfNodeState::Ready)
                .expect_none("node already had state");
            self.ready.push(node);
        }
        let ready_to_run = self.ready.clone();
        self.ready = Vec::new();
        for node in ready_to_run {
            assert_eq!(self.node_states[&node], WfNodeState::Ready);

            let wfaction =
                self.launchers.remove(&node).expect("missing action for node");
            // TODO we could be much more efficient without copying this tree
            // each time.
            let mut ancestor_tree = BTreeMap::new();
            self.make_ancestor_tree(&mut ancestor_tree, node);
            let mut node_done_tx = self.tx.clone();
            // XXX should be Starting, but we have no way to change to Running
            self.node_states.insert(node, WfNodeState::Running);
            let wflog = Arc::clone(&self.wflog);
            let task = tokio::spawn(async move {
                let wflog1 = Arc::clone(&wflog);
                WfExecutor::record_now(wflog1, node, WfNodeEventType::Started)
                    .await;
                let exec_future = wfaction.do_it(WfContext { ancestor_tree });
                let result = exec_future.await;
                let event_type = if let Ok(ref output) = result {
                    WfNodeEventType::Succeeded(Arc::clone(&output))
                } else {
                    WfNodeEventType::Failed
                };
                WfExecutor::record_now(wflog, node, event_type).await;
                node_done_tx
                    .try_send(TaskCompletion { node_id: node, result })
                    .expect("unexpected channel failure");
            });
            self.node_tasks.insert(node, task);
        }

        // TODO this condition needs work.  It doesn't account for failed nodes
        // or unwinding or anything.
        if self.graph.node_count() == self.node_outputs.len() {
            self.exec_state = WfState::Done;
            return self.poll(cx);
        }

        Poll::Pending
    }
}
