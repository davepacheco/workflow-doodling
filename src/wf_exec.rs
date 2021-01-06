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
use futures::channel::mpsc;
use futures::lock::Mutex;
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

/**
 * Message sent from (tokio) task that executes an action to the executor
 * indicating that the action has completed
 */
struct TaskCompletion {
    node_id: NodeIndex,
    result: WfResult,
}

/**
 * Context provided to the (tokio) task that executes an action
 */
struct TaskParams {
    /**
     * Handle to the workflow's live state
     *
     * This is used only to update state for status purposes.  We want to avoid
     * any tight coupling between this task and the internal state.
     */
    live_state: Arc<Mutex<WfExecLiveState>>,

    /** id of the graph node whose action we're running */
    node_id: NodeIndex,
    /** channel over which to send completion message */
    done_tx: mpsc::Sender<TaskCompletion>,
    /** Ancestor tree for this node.  See [`WfContext`]. */
    ancestor_tree: BTreeMap<String, WfOutput>,
    /** The action itself that we're executing. */
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

    live_state: Arc<Mutex<WfExecLiveState>>,
}

impl WfExecutor {
    /** Create an executor to run the given workflow. */
    pub fn new(w: Workflow) -> WfExecutor {
        let workflow_id = Uuid::new_v4();
        // TODO "myself" here should be a hostname or other identifier for this
        // instance.
        let wflog = WfLog::new("myself", workflow_id);
        let (finish_tx, _) = broadcast::channel(1);
        let mut node_states = BTreeMap::new();
        node_states.insert(w.root, WfNodeState::Ready);

        WfExecutor {
            graph: w.graph,
            node_names: w.node_names,
            root: w.root,
            workflow_id,
            finish_tx,

            live_state: Arc::new(Mutex::new(WfExecLiveState {
                launchers: w.launchers,
                exec_state: WfState::Running,
                node_states,
                node_tasks: BTreeMap::new(),
                node_outputs: BTreeMap::new(),
                ready: vec![w.root],
                wflog,
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
            finish_tx,
            live_state: Arc::new(Mutex::new(WfExecLiveState {
                launchers: w.launchers,
                exec_state,
                node_states,
                node_tasks: BTreeMap::new(),
                node_outputs,
                ready,
                wflog,
            })),
        }
    }

    /**
     * Builds the "ancestor tree" for a node whose dependencies have all
     * completed
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
    async fn record_now(
        wflog: &mut WfLog,
        node: NodeIndex,
        event_type: WfNodeEventType,
    ) {
        let node_id = node.index() as u64;
        wflog.record_now(node_id, event_type).await.unwrap();
    }

    /**
     * Runs the workflow
     *
     * This might be running a workflow that has never been started before or
     * one that has been recovered from persistent state.
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
         */
        loop {
            /*
             * It shouldn't be possible to get None back here.  That would mean
             * that all of the consumers have closed their ends, but we still
             * have a consumer of our own in "tx".
             */
            let message = rx.next().await.expect("broken tx");
            let node_id = message.node_id;

            let (task, new_state) = {
                let mut live_state = self.live_state.lock().await;
                let old_state = *live_state
                    .node_states
                    .get(&node_id)
                    .expect("node finished that was not running");
                let task = live_state
                    .node_tasks
                    .remove(&node_id)
                    .expect("no task for completed node");

                if old_state == WfNodeState::Done {
                    let output = message.result.unwrap();
                    live_state.node_outputs.insert(node_id, output);
                } else {
                    assert_eq!(old_state, WfNodeState::Failed);
                }

                (task, old_state)
            };

            /*
             * This should really not take long, as there's nothing else this
             * task does after sending the message that we just received.  It's
             * good to wait here to make sure things are cleaned up.
             * TODO-robustness can we enforce that this won't take long?
             */
            task.await.expect("node task failed unexpectedly");

            if new_state == WfNodeState::Failed {
                todo!(); // TODO trigger unwind!
            }

            assert_eq!(new_state, WfNodeState::Done);

            {
                let mut live_state = self.live_state.lock().await;

                // TODO this condition needs work.  It doesn't account for
                // failed nodes or unwinding or anything.
                if self.graph.node_count() == live_state.node_outputs.len() {
                    live_state.exec_state = WfState::Done;
                    self.finish_tx
                        .send(())
                        .expect("failed to send finish message");
                    break;
                }

                for depnode in self.graph.neighbors_directed(node_id, Outgoing)
                {
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
                node_id,
                done_tx: tx.clone(),
                ancestor_tree,
                action: wfaction,
            };

            live_state.node_states.insert(node_id, WfNodeState::Starting);
            let task = tokio::spawn(WfExecutor::exec_node(task_params));
            live_state.node_tasks.insert(node_id, task);
        }
    }

    /**
     * Body of a (tokio) task that executes an action.
     */
    async fn exec_node(mut task_params: TaskParams) {
        let node_id = task_params.node_id;

        {
            /*
             * TODO-liveness We don't want to hold this lock across a call
             * to the database.  It's fair to say that if the database
             * hangs, the saga's corked anyway, but we shoudl at least be
             * able to view its state, and we can't do that with this
             * design.
             */
            let mut live_state = task_params.live_state.lock().await;
            WfExecutor::record_now(
                &mut live_state.wflog,
                node_id,
                WfNodeEventType::Started,
            )
            .await;
            live_state.node_states.insert(node_id, WfNodeState::Running);
        }

        let exec_future = task_params
            .action
            .do_it(WfContext { ancestor_tree: task_params.ancestor_tree });
        let result = exec_future.await;
        let (event_type, next_state, next_next) = if let Ok(ref output) = result
        {
            (
                WfNodeEventType::Succeeded(Arc::clone(&output)),
                WfNodeState::Finishing,
                WfNodeState::Done,
            )
        } else {
            (WfNodeEventType::Failed, WfNodeState::Failing, WfNodeState::Failed)
        };

        {
            let mut live_state = task_params.live_state.lock().await;
            live_state.node_states.insert(node_id, next_state);
            WfExecutor::record_now(&mut live_state.wflog, node_id, event_type)
                .await;
            live_state.node_states.insert(node_id, next_next);
        }

        task_params
            .done_tx
            .try_send(TaskCompletion { node_id, result })
            .expect("unexpected channel failure");
    }

    pub fn run(&self) -> impl Future<Output = ()> + '_ {
        let mut rx = self.finish_tx.subscribe();

        async move {
            self.run_workflow().await;
            rx.recv().await.expect("failed to receive finish message")
        }
    }

    // TODO better than this would be if the result of executing the workflow is
    // a WfSummary struct that provides access to information about each node
    // (timing, output, error).  This struct would be immutable.  Then this
    // wouldn't need to be async because we wouldn't need to take a lock.
    pub async fn lookup_output<T: Send + Sync + 'static>(
        &self,
        name: &str,
    ) -> Result<Arc<T>, WfError> {
        let live_state = self.live_state.lock().await;
        /* XXX This is awful */
        for node_id in live_state.node_outputs.keys() {
            let node_name = self.node_names.get(node_id);
            if node_name.is_none() {
                assert_eq!(node_id.index() as u64, 0);
                continue;
            }

            if node_name.unwrap() == name {
                let item = live_state.node_outputs.get(&node_id).unwrap();
                let specific_item = Arc::clone(item)
                    .downcast::<T>()
                    .expect(&format!("node {} produced unexpected type", name));
                return Ok(specific_item);
            }
        }

        // XXX Should be a graceful error if the node is valid in the workflow
        panic!(
            "node {} not part of workflow or did not finish successfully",
            name
        );
    }
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
    // XXX may as well store errors too
    node_outputs: BTreeMap<NodeIndex, WfOutput>,

    /** Persistent state */
    wflog: WfLog,
}

// impl WfExecLiveState {
//     // mark ready (set state = ready, append to ready queue)
//     // set node state (only allowed for a handful of states)
//     // set starting (sets state, consumes task handle)
//     // set finishing (sets state, removes and returns task, records result)
//     // propagate_ready
//     pub fn new() -> WfExecLiveState {
//
//     }
// }
