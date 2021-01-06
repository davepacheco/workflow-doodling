//! Manages execution of a workflow

use crate::wf_log::WfNodeEventType;
use crate::wf_log::WfNodeLoadStatus;
use crate::WfAction;
use crate::WfContext;
use crate::WfId;
use crate::WfLog;
use crate::WfOutput;
use crate::WfResult;
use crate::Workflow;
use core::future::Future;
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
    node_names: BTreeMap<NodeIndex, String>,
    root: NodeIndex,

    /** Channel for monitoring execution completion */
    finish_tx: broadcast::Sender<WfResult>,

    /** Unique identifier for this execution */
    // TODO The nomenclature is problematic here.  This is really a workflow
    // _execution_ id.  Or maybe Workflows are really WorkflowTemplates?  Either
    // way, this identifies something different than what we currently call
    // Workflows.
    workflow_id: WfId,
    /** Persistent state */
    // XXX put into live_state without Arc+Mutex?
    wflog: Arc<Mutex<WfLog>>,

    live_state: Mutex<WfExecLiveState>,
}

struct WfExecLiveState {
    /* See `Workflow` */
    launchers: BTreeMap<NodeIndex, Box<dyn WfAction>>,

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

    /** Final result of the workflow */
    result: Option<WfResult>,
}

impl WfExecutor {
    /** Create an executor to run the given workflow. */
    pub fn new(w: Workflow) -> WfExecutor {
        let workflow_id = Uuid::new_v4();
        // TODO "myself" here should be a hostname or other identifier for this
        // instance.
        let wflog = Arc::new(Mutex::new(WfLog::new("myself", workflow_id)));
        let mut node_states = BTreeMap::new();
        let (finish_tx, _) = broadcast::channel(1);

        node_states.insert(w.root, WfNodeState::Ready);

        WfExecutor {
            graph: w.graph,
            node_names: w.node_names,
            root: w.root,
            workflow_id,
            wflog,
            finish_tx,

            live_state: Mutex::new(WfExecLiveState {
                launchers: w.launchers,
                exec_state: WfState::Running,
                node_states,
                node_tasks: BTreeMap::new(),
                node_outputs: BTreeMap::new(),
                ready: vec![w.root],
                result: None,
            }),
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
        let (finish_tx, _) = broadcast::channel(1);

        WfExecutor {
            graph: w.graph,
            node_names: w.node_names,
            root: w.root,
            workflow_id,
            wflog,
            finish_tx,
            live_state: Mutex::new(WfExecLiveState {
                launchers: w.launchers,
                exec_state,
                node_states,
                node_tasks: BTreeMap::new(),
                node_outputs,
                ready,
                result: None,
            }),
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
        live_state: &WfExecLiveState,
        node: NodeIndex,
    ) {
        let ancestors = self.graph.neighbors_directed(node, Incoming);
        for ancestor in ancestors {
            self.make_ancestor_tree_node(tree, live_state, ancestor);
        }
    }

    fn make_ancestor_tree_node(
        &self,
        tree: &mut BTreeMap<String, WfOutput>,
        live_state: &WfExecLiveState,
        node: NodeIndex,
    ) {
        if node == self.root {
            return;
        }

        let name = self.node_names[&node].to_string();
        let node_state =
            live_state.node_states.get(&node).unwrap_or(&WfNodeState::Blocked);
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
        let output = &live_state.node_outputs[&node];
        tree.insert(name, Arc::clone(output));
        self.make_ancestor_tree(tree, live_state, node);
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
    // TODO design notes XXX working here XXX XXX
    // - this should be an async function.  We want to go that model rather than
    //   implementing poll() and calling poll() on subfutures.
    // - thus: the structure must be that it's just a loop reading messages
    //   off of its channel
    // - we want to expose state and control to consumers:
    //   - current status
    //   - pause/unpause/abort
    //   - snapshot log
    //   How can we do that?  This function needs an exclusive reference to
    //   "self" in order to do a lot of its work.  We must decompose the state:
    //   - actual execution state: we need exclusive access for most of work.
    //     We could drop that while we're blocked.
    //     option: mutex to protect it
    //     option: provide a separate view of the state for consumers and have
    //     _that_ protected by a lock (or atomic).  Would need a similar
    //     mechanism for control parameters.
    // Proposal 1:
    // - WfExecutor has a few kinds of state:
    //   - immutable: the stuff from Workflow (e.g., "graph", "node_names",
    //     "root")
    //   - view-and-control state:
    //     - protected by lock
    //     - at least: result
    //     - problem: depending on how detailed we want the status to be, this
    //       could be a complete copy of the live execution state
    //   - live execution state
    //     - wflog, exec_state, node_states, node_tasks, ready, node_outputs,
    //       launchers
    // - run_workflow() has:
    //   - shared reference to immutable parameters
    //   - reference to the _lock_ used for view-and-control state, a lock that
    //     it takes only briefly to check if it's been paused/aborted and to
    //     update the view state
    //   - exclusive access to the live execution state
    //
    // Proposal 2:
    // - We don't distinguish so carefully between view-and-control and live
    //   execution state, on the grounds that consumers might want an
    //   arbitrarily deep view of the current state.
    // - run_workflow() explicitly takes and drops the lock protecting live
    //   execution state when it updates it.
    async fn run_workflow(&self) {
        // XXX how to check this?  do we want to take the lock?  Note that it's
        // not obvious it will be Running here -- consider the recovery case.
        // assert_eq!(self.exec_state, WfState::Running);

        /*
         * In practice, each node can enqueue only two messages in its lifetime:
         * one for completion of the action, and one for completion of the
         * compensating action.  We bound this channel's size at twice the graph
         * node count for this worst case.
         */
        let (tx, mut rx) = mpsc::channel(2 * self.graph.node_count());

        /*
         * Kick off any nodes that are ready to run already.
         */
        self.kick_off_ready(&tx).await;

        /*
         * Process any messages available on our channel.
         *
         * It shouldn't be possible to get None back here.  That would mean that
         * all of the consumers have closed their ends, but we still have a
         * consumer of our own in "tx".
         */
        loop {
            let message = rx.next().await.expect("broken tx");
            let node = message.node_id;

            let (task, new_state) = {
                let mut live_state = self.live_state.lock().await;
                let old_state = live_state
                    .node_states
                    .remove(&node)
                    .expect("node finished that was not running");
                assert_eq!(old_state, WfNodeState::Running);

                /*
                 * It would be nice to join on this task here, but we don't know for
                 * sure it's completed yet.  (It should be imminently, but we can't
                 * wait in this context.)
                 */
                let task = live_state
                    .node_tasks
                    .remove(&node)
                    .expect("no task for completed node");

                let new_state = if let Ok(ref output) = *message.result {
                    live_state.node_outputs.insert(node, Arc::clone(&output));
                    WfNodeState::Done
                } else {
                    todo!(); // TODO trigger unwind!
                             // WfNodeState::Failed
                };

                live_state.node_states.insert(node, new_state);
                live_state.result = Some(Arc::clone(&message.result));

                (task, new_state)
            };

            /*
             * This should really not take long, as there's nothing else this
             * task does after sending the message that we just received.  It's
             * good to wait here to make sure things are cleaned up.
             * TODO-robustness can we enforce this?
             */
            task.await.expect("node task failed unexpectedly");

            if new_state == WfNodeState::Failed {
                continue;
            }

            assert_eq!(new_state, WfNodeState::Done);

            {
                let mut live_state = self.live_state.lock().await;

                // TODO this condition needs work.  It doesn't account for failed nodes
                // or unwinding or anything.
                if self.graph.node_count() == live_state.node_outputs.len() {
                    live_state.exec_state = WfState::Done;
                    self.finish_tx
                        .send(Arc::clone(
                            &live_state.result.as_ref().expect(
                                "workflow ofinished without any result",
                            ),
                        ))
                        .expect("failed to send finish message");
                    break;
                }

                for depnode in self.graph.neighbors_directed(node, Outgoing) {
                    /*
                     * Check whether all of this node's incoming edges are now
                     * satisfied.
                     */
                    let mut okay = true;
                    for upstream in
                        self.graph.neighbors_directed(depnode, Incoming)
                    {
                        let node_state = live_state.node_states[&upstream];
                        // XXX more general in the case of failure?
                        if node_state != WfNodeState::Done {
                            okay = false;
                            break;
                        }
                    }

                    if okay {
                        live_state
                            .node_states
                            .insert(depnode, WfNodeState::Ready)
                            .expect_none("node already had state");
                        live_state.ready.push(depnode);
                    }
                }
            }

            self.kick_off_ready(&tx).await;
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
    async fn kick_off_ready(&self, tx: &mpsc::Sender<TaskCompletion>) {
        let mut live_state = self.live_state.lock().await;

        let ready_to_run = live_state.ready.clone();
        live_state.ready = Vec::new();

        for node in ready_to_run {
            assert_eq!(live_state.node_states[&node], WfNodeState::Ready);

            let wfaction = live_state
                .launchers
                .remove(&node)
                .expect("missing action for node");
            // TODO we could be much more efficient without copying this tree
            // each time.
            let mut ancestor_tree = BTreeMap::new();
            self.make_ancestor_tree(&mut ancestor_tree, &live_state, node);
            let mut node_done_tx = tx.clone();
            // XXX should be Starting, but we have no way to change to Running
            live_state.node_states.insert(node, WfNodeState::Running);
            let wflog = Arc::clone(&self.wflog);
            let task = tokio::spawn(async move {
                let wflog1 = Arc::clone(&wflog);
                WfExecutor::record_now(wflog1, node, WfNodeEventType::Started)
                    .await;
                let exec_future = wfaction.do_it(WfContext { ancestor_tree });
                let result = exec_future.await;
                let event_type = if let Ok(ref output) = *result {
                    WfNodeEventType::Succeeded(Arc::clone(&output))
                } else {
                    WfNodeEventType::Failed
                };
                WfExecutor::record_now(wflog, node, event_type).await;
                node_done_tx
                    .try_send(TaskCompletion { node_id: node, result })
                    .expect("unexpected channel failure");
            });
            live_state.node_tasks.insert(node, task);
        }
    }

    pub fn run(&self) -> impl Future<Output = WfResult> + '_ {
        let mut rx = self.finish_tx.subscribe();

        async move {
            self.run_workflow().await;
            rx.recv().await.expect("failed to receive finish message")
        }
    }

    /**
     * Returns a Future that waits for this executor to finish.
     */
    // TODO-cleanup Is this the idiomatic way to do this?  It seems like people
    // generally return a specific struct that impl's Future, but that seems a
    // lot harder to do here.
    // TODO It might be nice if the returned future didn't have an implicit
    // reference (by lifetime?) to "self"?  This currently isn't possible
    // because we need to take the lock to check the state.  This might be
    // possible if instead of making this a function you could call at any old
    // time, we made this a Future returned by a start() method that kicks off
    // execution.
    pub fn wait_for_finish(&self) -> impl Future<Output = WfResult> + '_ {
        /*
         * It's important that we subscribe here, before we check self.state.
         * This way, if the workflow is currently running but is done by the
         * time we try to recv() on this channel, we're guaranteed that there
         * will be a message there for us.  If we subscribed after the check,
         * there would be a window in which we see the workflow running, but it
         * completes before we subscribe, and we'd miss a wakeup.
         * TODO-cleanup Is the above true?  Or is it impossible for the state to
         * change because we have a reference to `self`?  If the above is true,
         * it's somewhat surprising that this is so easy to get wrong.  Is there
         * something more deeply wrong with our approach?
         */
        let mut rx = self.finish_tx.subscribe();

        async move {
            {
                let live_state = self.live_state.lock().await;

                if let WfState::Done = live_state.exec_state {
                    assert!(live_state.result.is_some()); // XXX give WfState enum data instead?
                    if let Some(ref result) = live_state.result {
                        return Arc::clone(result);
                    } else {
                        panic!("workflow done with no result");
                    }
                }
                assert!(live_state.result.is_none()); // XXX give WfState enum data instead?
            }

            rx.recv().await.expect(
                "unexpected receive error on workflow completion channel",
            )
        }
    }
}
