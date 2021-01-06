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
use anyhow::anyhow;
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

struct TaskParams {
    live_state: Arc<Mutex<WfExecLiveState>>,
    wflog: Arc<Mutex<WfLog>>,
    node_id: NodeIndex,
    done_tx: mpsc::Sender<TaskCompletion>,
    ancestor_tree: BTreeMap<String, WfOutput>,
    action: Box<dyn WfAction>,
}

/**
 * Executes a workflow
 *
 * Call `WfExecutor.run()` to get a Future.  You must `await` this Future to
 * actually execute the workflow.
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
    finish_tx: broadcast::Sender<()>,

    /** Unique identifier for this execution */
    // TODO The nomenclature is problematic here.  This is really a workflow
    // _execution_ id.  Or maybe Workflows are really WorkflowTemplates?  Either
    // way, this identifies something different than what we currently call
    // Workflows.
    workflow_id: WfId,
    /** Persistent state */
    // XXX put into live_state without Arc+Mutex?
    wflog: Arc<Mutex<WfLog>>,

    live_state: Arc<Mutex<WfExecLiveState>>,
}

/**
 * Encapsulates the (mutable) execution state of a workflow
 */
/*
 * This is linked to a `WfExecutor` and protected by a Mutex.  The state is
 * mainly modified by [`WfExecutor::run_workflow`].  We may add methods for
 * controlling the workflow (e.g., pausing), which would modify this as well.
 * We also intend to add methods for viewing workflow state, which will take the
 * lock to read state.
 *
 * If the view of a workflow were just (1) that it's running, and maybe (2) a
 * set of outstanding actions, then we might take a pretty different approach
 * here.  We might create a read-only view object that's populated periodically
 * by the workflow executor.  This still might be the way to go, but at the
 * moment we anticipate wanting pretty detailed debug information (like what
 * outputs were produced by what steps), so the view would essentially be a
 * whole copy of this object.
 */
struct WfExecLiveState {
    /** See [`Workflow::launchers`] */
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

            live_state: Arc::new(Mutex::new(WfExecLiveState {
                launchers: w.launchers,
                exec_state: WfState::Running,
                node_states,
                node_tasks: BTreeMap::new(),
                node_outputs: BTreeMap::new(),
                ready: vec![w.root],
                result: None,
            })),
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
            live_state: Arc::new(Mutex::new(WfExecLiveState {
                launchers: w.launchers,
                exec_state,
                node_states,
                node_tasks: BTreeMap::new(),
                node_outputs,
                ready,
                result: None,
            })),
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
     * XXX Now that WfExecLiveState is protected by a Mutex, maybe we don't need
     * this to be async and take another lock?
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
    async fn run_workflow(&self) {
        // XXX how to check this?  do we want to take the lock?  Note that it's
        // not obvious it will be Running here -- consider the recovery case.
        // TODO It would also be nice every time we block on the channel to
        // assert that there are outstanding operations.
        // assert_eq!(self.exec_state, WfState::Running);

        /*
         * Allocate the channel used for node tasks to tell us when they've
         * completed.  In practice, each node can enqueue only two messages in
         * its lifetime: one for completion of the action, and one for
         * completion of the compensating action.  We bound this channel's size
         * at twice the graph node count for this worst case.
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
                assert_eq!(old_state, WfNodeState::Finishing);

                /*
                 * It would be nice to join on this task here, but we don't know for
                 * sure it's completed yet.  (It should be imminently, but we can't
                 * wait in this context.)
                 */
                let task = live_state
                    .node_tasks
                    .remove(&node)
                    .expect("no task for completed node");

                let new_state = if let Ok(ref output) = message.result {
                    live_state.node_outputs.insert(node, Arc::clone(&output));
                    WfNodeState::Done
                } else {
                    todo!(); // TODO trigger unwind!
                             // WfNodeState::Failed
                };

                live_state.node_states.insert(node, new_state);
                live_state.result = Some(message.result);

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
                        .send(())
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

        for node_id in ready_to_run {
            assert_eq!(live_state.node_states[&node_id], WfNodeState::Ready);

            let wfaction = live_state
                .launchers
                .remove(&node_id)
                .expect("missing action for node");
            // TODO we could be much more efficient without copying this tree
            // each time.
            let mut ancestor_tree = BTreeMap::new();
            self.make_ancestor_tree(&mut ancestor_tree, &live_state, node_id);

            let task_params = TaskParams {
                live_state: Arc::clone(&self.live_state),
                wflog: Arc::clone(&self.wflog),
                node_id,
                done_tx: tx.clone(),
                ancestor_tree,
                action: wfaction,
            };

            live_state.node_states.insert(node_id, WfNodeState::Starting);
            let task = tokio::spawn(self.exec_node(task_params));
            live_state.node_tasks.insert(node_id, task);
        }
    }

    /**
     * Body of a (tokio) task that executes an action.
     */
    fn exec_node(
        &self,
        mut task_params: TaskParams,
    ) -> impl Future<Output = ()> {
        async move {
            let node_id = task_params.node_id;
            let wflog1 = Arc::clone(&task_params.wflog);
            WfExecutor::record_now(wflog1, node_id, WfNodeEventType::Started)
                .await;

            {
                let mut live_state = task_params.live_state.lock().await;
                live_state.node_states.insert(node_id, WfNodeState::Running);
            }

            let exec_future = task_params
                .action
                .do_it(WfContext { ancestor_tree: task_params.ancestor_tree });
            let result = exec_future.await;
            let (event_type, next_state) = if let Ok(ref output) = result {
                (
                    WfNodeEventType::Succeeded(Arc::clone(&output)),
                    WfNodeState::Finishing,
                )
            } else {
                (WfNodeEventType::Failed, WfNodeState::Failing)
            };

            {
                let mut live_state = task_params.live_state.lock().await;
                live_state.node_states.insert(node_id, next_state);
            }

            WfExecutor::record_now(task_params.wflog, node_id, event_type)
                .await;

            task_params
                .done_tx
                .try_send(TaskCompletion { node_id, result })
                .expect("unexpected channel failure");
        }
    }

    pub fn run(&self) -> impl Future<Output = ()> + '_ {
        let mut rx = self.finish_tx.subscribe();

        async move {
            self.run_workflow().await;
            rx.recv().await.expect("failed to receive finish message")
        }
    }

    // XXX janky
    pub async fn consume_result(self) -> WfResult {
        let live_state = self.live_state.lock().await;
        let maybe_result = live_state.result.as_ref();
        let result = maybe_result.unwrap();
        match result {
            Ok(output) => Ok(Arc::clone(output)),
            // XXX Want to preserve the error, not this.
            Err(_) => Err(anyhow!("workflow failed")),
        }
    }
}
