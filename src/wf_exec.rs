//! Manages execution of a workflow

use crate::wf_log::WfNodeEventType;
use crate::wf_log::WfNodeLoadStatus;
use crate::WfAction;
use crate::WfError;
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
use petgraph::algo::toposort;
use petgraph::graph::NodeIndex;
use petgraph::visit::Topo;
use petgraph::visit::Walker;
use petgraph::Direction;
use petgraph::Graph;
use petgraph::Incoming;
use petgraph::Outgoing;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::io;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use uuid::Uuid;

/*
 * XXX Working here
 * I'm working on a rewrite using the typestate pattern described by Cliff's
 * blog, also similar to what's described by the hoverbear blog post on state
 * machines.
 * XXX Next steps:
 * - updating recovery process -- see new_recover().
 * - update state printing?
 * XXX Should we go even further and say that each node is its own struct with
 * incoming channels from parents (to notify when done), from children (to
 * notify when cancelled), and to each direction as well?  Then the whole thing
 * is a message passing exercise?
 */
struct WfnsBlocked;
struct WfnsReady;
struct WfnsStarting;
struct WfnsRunning;
struct WfnsFinishing;
struct WfnsDone(WfOutput);
struct WfnsFailing;
struct WfnsFailed(WfError);
struct WfnsStartingCancel;
struct WfnsCancelling;
struct WfnsFinishingCancel;
struct WfnsCancelled;

struct WfNode<S: WfNodeStateType> {
    node_id: NodeIndex,
    state: S,
}

trait WfNodeStateType {}
impl WfNodeStateType for WfnsBlocked {}
impl WfNodeStateType for WfnsReady {}
impl WfNodeStateType for WfnsStarting {}
impl WfNodeStateType for WfnsRunning {}
impl WfNodeStateType for WfnsFinishing {}
impl WfNodeStateType for WfnsDone {}
impl WfNodeStateType for WfnsFailing {}
impl WfNodeStateType for WfnsFailed {}
impl WfNodeStateType for WfnsStartingCancel {}
impl WfNodeStateType for WfnsCancelling {}
impl WfNodeStateType for WfnsFinishingCancel {}
impl WfNodeStateType for WfnsCancelled {}

trait WfNodeRest: Send + Sync {
    fn propagate(&self, exec: &WfExecutor, live_state: &mut WfExecLiveState);
    fn log_event(&self) -> WfNodeEventType;
}

impl WfNodeRest for WfNode<WfnsDone> {
    fn log_event(&self) -> WfNodeEventType {
        WfNodeEventType::Succeeded(Arc::clone(&self.state.0))
    }

    fn propagate(&self, exec: &WfExecutor, live_state: &mut WfExecLiveState) {
        let graph = &exec.workflow.graph;
        live_state
            .node_outputs
            .insert(self.node_id, Arc::clone(&self.state.0))
            .expect_none("node finished twice (storing output)");

        if self.node_id == exec.workflow.end_node {
            /*
             * If we've completed the last node, the workflow is done.
             */
            assert_eq!(live_state.exec_state, WfState::Running);
            assert_eq!(graph.node_count(), live_state.node_outputs.len());
            live_state.mark_workflow_done();
            return;
        }

        if live_state.exec_state == WfState::Unwinding {
            /*
             * If the workflow is currently unwinding, then this node finishing
             * doesn't unblock any other nodes.  However, it potentially
             * unblocks undoing itself.  We'll only proceed if all of our child
             * nodes are "cancelled" already.
             */
            if neighbors_all(graph, &self.node_id, Outgoing, |child| {
                live_state.nodes_cancelled.contains(child)
            }) {
                live_state.queue_undo.push(self.node_id);
            }
            return;
        }

        /*
         * Under normal execution, this node's completion means it's time to
         * check dependent nodes to see if they're now runnable.
         */
        for child in graph.neighbors_directed(self.node_id, Outgoing) {
            if neighbors_all(graph, &child, Incoming, |parent| {
                live_state.node_outputs.contains_key(parent)
            }) {
                live_state.queue_todo.push(child);
            }
        }
    }
}

impl WfNodeRest for WfNode<WfnsFailed> {
    fn log_event(&self) -> WfNodeEventType {
        WfNodeEventType::Failed
    }

    fn propagate(&self, exec: &WfExecutor, live_state: &mut WfExecLiveState) {
        let graph = &exec.workflow.graph;

        if live_state.exec_state == WfState::Unwinding {
            /*
             * This node failed while we're already unwinding.  We don't
             * need to kick off unwinding again.  We could in theory
             * immediately move this node to "cancelled" and unblock its
             * dependents, but for consistency with a simpler algorithm,
             * we'll wait for cancellation to propagate from the end node.
             * If all of our children are already cancelled, however, we
             * must go ahead and mark ourselves cancelled and propagate
             * that.
             */
            if neighbors_all(graph, &self.node_id, Outgoing, |child| {
                live_state.nodes_cancelled.contains(child)
            }) {
                let new_node =
                    WfNode { node_id: self.node_id, state: WfnsCancelled };
                new_node.propagate(exec, live_state);
            }
        } else {
            /*
             * Begin the unwinding process.  Start with the end node: mark
             * it trivially "cancelled" and propagate that.
             */
            live_state.exec_state = WfState::Unwinding;
            assert_ne!(self.node_id, exec.workflow.end_node);
            let new_node = WfNode {
                node_id: exec.workflow.end_node,
                state: WfnsCancelled,
            };
            new_node.propagate(exec, live_state);
        }
    }
}

impl WfNodeRest for WfNode<WfnsCancelled> {
    fn log_event(&self) -> WfNodeEventType {
        WfNodeEventType::CancelFinished
    }

    fn propagate(&self, exec: &WfExecutor, live_state: &mut WfExecLiveState) {
        let graph = &exec.workflow.graph;
        assert_eq!(live_state.exec_state, WfState::Unwinding);
        /* TODO side effect in assertion? */
        assert!(live_state.nodes_cancelled.insert(self.node_id));

        if self.node_id == exec.workflow.start_node {
            /*
             * If we've cancelled the start node, the workflow is done.
             */
            live_state.mark_workflow_done();
            return;
        }

        /*
         * During unwinding, a node's cancellation means it's time to check
         * ancestor nodes to see if they're now cancellable.
         */
        for parent in graph.neighbors_directed(self.node_id, Incoming) {
            if neighbors_all(graph, &parent, Outgoing, |child| {
                live_state.nodes_cancelled.contains(child)
            }) {
                /*
                 * XXX problem here is that while we know that the next step is
                 * to cancel this node, we don't know whether we need to run the
                 * "undo" action or if we can just move it straight to
                 * "cancelled".  We haven't recorded state about this.
                 *
                 * One possibility is that we maintain a tree of nodes into
                 * which we insert a Cancelable trait when we start running them
                 * (or recover them in this state).  The "cancel" function does
                 * the appropriate thing.  If/when the thing finishes its
                 * action, we change the implementing struct to an
                 * implementation that runs the undo action.
                 */
                /*
                 * We're ready to cancel "parent".  We don't know whether it's
                 * finished running, on the todo queue, or currenting
                 * outstanding.  (It should not be on the undo queue!)
                 * XXX Here's an awful approach just intended to let us flesh
                 * out more of the rest of this to better understand how to
                 * manage state.
                 */
                match live_state.node_exec_state(&parent) {
                    WfNodeExecState::Blocked | WfNodeExecState::Failed => {
                        /*
                         * If the node never started or if it failed, we can
                         * just mark it cancelled without doing anything else.
                         */
                        let new_node =
                            WfNode { node_id: parent, state: WfnsCancelled };
                        new_node.propagate(exec, live_state);
                        continue;
                    }

                    WfNodeExecState::QueuedToRun
                    | WfNodeExecState::TaskInProgress => {
                        /*
                         * If we're running an action for this task, there's
                         * nothing we can do right now, but we'll handle it when
                         * it finishes.  We could do better with queued (and
                         * there's an XXX in kick_off_ready() to do so), but
                         * this isn't wrong as-is.
                         */
                        continue;
                    }

                    WfNodeExecState::Done => {
                        /*
                         * We have to actually run the undo action.
                         */
                        live_state.queue_undo.push(parent);
                    }

                    WfNodeExecState::QueuedToUndo
                    | WfNodeExecState::Cancelled => {
                        panic!(
                            "already cancelling or cancelled node \
                            whose child was just now cancelled"
                        );
                    }
                }
            }
        }
    }
}

// XXX
// trait WfNodeCancellable {
//     fn cancel(&self, exec: &WfExecutor, live_state: &mut WfExecLiveState);
// }
//
// impl WfNodeCancellable for WfNode<WfnsBlocked>
// impl WfNodeCancellable for WfNode<WfnsReady >
// impl WfNodeCancellable for WfNode<WfnsStarting >
// impl WfNodeCancellable for WfNode<WfnsRunning >
// impl WfNodeCancellable for WfNode<WfnsFinishing >
// impl WfNodeCancellable for WfNode<WfnsDone >
// impl WfNodeCancellable for WfNode<WfnsFailing >
// impl WfNodeCancellable for WfNode<WfnsFailed >
// impl WfNodeCancellable for WfNode<WfnsStartingCancel >
// impl WfNodeCancellable for WfNode<WfnsCancelling >
// impl WfNodeCancellable for WfNode<WfnsFinishingCancel >
// impl WfNodeCancellable for WfNode<WfnsCancelled >

// XXX
// /**
//  * Execution state for a workflow node
//  * TODO ASCII version of the pencil-and-paper diagram?
//  */
// #[derive(Debug, Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
// enum WfNodeState {
//     Blocked,
//     Ready,
//     Starting,
//     Running,
//     Finishing,
//     Done,
//     Failing,
//     Failed,
//     StartingCancel,
//     Cancelling,
//     FinishingCancel,
//     Cancelled,
// }

/**
 * Execution state for the workflow overall
 */
#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
enum WfState {
    Running,
    Unwinding,
    Done,
}

// XXX
// impl fmt::Display for WfNodeState {
//     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
//         f.write_str(match self {
//             WfNodeState::Blocked => "blocked",
//             WfNodeState::Ready => "ready",
//             WfNodeState::Starting => "starting",
//             WfNodeState::Running => "running",
//             WfNodeState::Finishing => "finishing",
//             WfNodeState::Done => "done",
//             WfNodeState::Failing => "failing",
//             WfNodeState::Failed => "failed",
//             WfNodeState::StartingCancel => "starting_cancel",
//             WfNodeState::Cancelling => "cancelling",
//             WfNodeState::FinishingCancel => "finishing_cancel",
//             WfNodeState::Cancelled => "cancelled",
//         })
//     }
// }

/**
 * Message sent from (tokio) task that executes an action to the executor
 * indicating that the action has completed
 */
struct TaskCompletion {
    /*
     * TODO-cleanup can this be removed? The node field is a WfNode, which has a
     * node_id.
     */
    node_id: NodeIndex,
    node: Box<dyn WfNodeRest>,
}

/**
 * Context provided to the (tokio) task that executes an action
 */
struct TaskParams {
    /** Handle to the workflow itself, used for metadata like the label */
    workflow: Arc<Workflow>,

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
    // TODO-cleanup there's no reason this should be an Arc.
    ancestor_tree: Arc<BTreeMap<String, WfOutput>>,
    /** The action itself that we're executing. */
    action: Arc<dyn WfAction>,
    /** Skip logging the "start" record (if this is a restart) */
    skip_log_start: bool,
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
#[derive(Debug)]
pub struct WfExecutor {
    // TODO This could probably be a reference instead.
    workflow: Arc<Workflow>,

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

#[derive(Debug)]
enum RecoveryDirection {
    Forward(bool),
    Unwind(bool),
}

impl WfExecutor {
    /** Create an executor to run the given workflow. */
    pub fn new(w: Arc<Workflow>) -> WfExecutor {
        let workflow_id = Uuid::new_v4();
        // TODO "myself" here should be a hostname or other identifier for this
        // instance.
        let wflog = WfLog::new("myself", workflow_id);
        WfExecutor::new_recover(w, wflog).unwrap()
    }

    /**
     * Create an executor to run the given workflow that may have already
     * started, using the given log events.
     */
    pub fn new_recover(
        workflow: Arc<Workflow>,
        wflog: WfLog,
    ) -> Result<WfExecutor, WfError> {
        /*
         * During recovery, there's a fine line between operational errors and
         * programmer errors.  If we discover semantically invalid workflow
         * state, that's an operational error that we must handle gracefully.
         * We use lots of assertions to check invariants about our own process
         * for loading the state.  We panic if those are violated.  For example,
         * if we find that we've loaded the same node twice, that's a bug in
         * this code right here (which walks each node of the graph exactly
         * once), not a result of corrupted database state.
         */
        let workflow_id = wflog.workflow_id;
        let forward = !wflog.unwinding;
        let mut live_state = WfExecLiveState {
            workflow: Arc::clone(&workflow),
            exec_state: if forward { WfState::Running } else { WfState::Done },
            queue_todo: Vec::new(),
            queue_undo: Vec::new(),
            node_tasks: BTreeMap::new(),
            node_outputs: BTreeMap::new(),
            nodes_cancelled: BTreeSet::new(),
            child_workflows: BTreeMap::new(),
            wflog: wflog,
        };
        let mut loaded = BTreeSet::new();

        /*
         * Iterate in the direction of current execution: for normal execution,
         * a standard topological sort.  For unwinding, reverse that.
         */
        let graph_nodes = {
            let mut nodes = toposort(&workflow.graph, None)
                .expect("workflow DAG had cycles");
            if !forward {
                nodes.reverse();
            }

            nodes
        };

        for node_id in graph_nodes {
            let node_status =
                live_state.wflog.load_status_for_node(node_id.index() as u64);

            /*
             * XXX XXX validate
             * Validate this node's state against its parent nodes' states.  By
             * induction, this validates everything in the graph from the start
             * node to the current node.
             */
            // for parent in w.graph.neighbors_directed(node_id, Incoming) {
            //     let parent_state = recovery.node_state(parent);
            //     if !recovery_validate_parent(parent_state, node_status) {
            //         return Err(anyhow!(
            //             "recovery for workflow {}: node {:?}: \
            //             load state is \"{:?}\", which is illegal for parent \"
            //             state \"{}\"",
            //             workflow_id,
            //             node_id,
            //             node_status,
            //             parent_state,
            //         ));
            //     }
            // }
            let graph = &workflow.graph;
            let direction = if forward {
                RecoveryDirection::Forward(neighbors_all(
                    graph,
                    &node_id,
                    Incoming,
                    |p| {
                        assert!(loaded.contains(p));
                        live_state.node_outputs.contains_key(p)
                    },
                ))
            } else {
                RecoveryDirection::Unwind(neighbors_all(
                    graph,
                    &node_id,
                    Outgoing,
                    |c| {
                        assert!(loaded.contains(c));
                        live_state.nodes_cancelled.contains(c)
                    },
                ))
            };

            match node_status {
                WfNodeLoadStatus::NeverStarted => {
                    match direction {
                        RecoveryDirection::Forward(true) => {
                            /*
                             * We're recovering a node in the forward direction
                             * where all parents completed successfully.  Add it
                             * to the ready queue.
                             */
                            live_state.queue_todo.push(node_id);
                        }
                        RecoveryDirection::Unwind(true) => {
                            /*
                             * We're recovering a node in the reverse direction
                             * (unwinding) whose children have all been
                             * cancelled and which has never started.  Just mark
                             * it cancelled.
                             * XXX Does this suggest a better way to do this
                             * might be to simply load all the state that we
                             * have into the WfExecLiveState and execute the
                             * workflow as normal, but have normal execution
                             * check for cached values instead of running
                             * actions?  In a sense, this makes the recovery
                             * path look like the normal path rather than having
                             * the normal path look like the recovery path.  On
                             * the other hand, it seems kind of nasty to have to
                             * hold onto the recovery state for the duration.
                             * It doesn't make it a whole lot easier to test or
                             * have fewer code paths, in a real sense.  It moves
                             * those code paths to normal execution, but they're
                             * still bifurcated from the case where we didn't
                             * recover the workflow.
                             */
                            live_state.nodes_cancelled.insert(node_id);
                        }
                        _ => (),
                    }
                }
                WfNodeLoadStatus::Started => {
                    /*
                     * Whether we're unwinding or not, we have to finish
                     * execution of this action.
                     * XXX need to record that the "start" log entry was already
                     * there.
                     */
                    live_state.queue_todo.push(node_id);
                }
                WfNodeLoadStatus::Succeeded(output) => {
                    /*
                     * If the node has finished executing and not started
                     * cancelling, and if we're unwinding and the children have
                     * all finished cancelling, then it's time to cancel this
                     * one.
                     */
                    live_state
                        .node_outputs
                        .insert(node_id, Arc::clone(output))
                        .expect_none("recovered node twice (success case)");
                    if let RecoveryDirection::Unwind(true) = direction {
                        live_state.queue_undo.push(node_id);
                    }
                }
                WfNodeLoadStatus::Failed => {
                    /*
                     * If the node failed, and we're unwinding, and the children
                     * have all been cancelled, it's time to cancel this one.
                     * But we just mark it cancelled -- we don't execute the
                     * undo action.
                     */
                    if let RecoveryDirection::Unwind(true) = direction {
                        live_state.nodes_cancelled.insert(node_id);
                    }
                }
                WfNodeLoadStatus::CancelStarted => {
                    /*
                     * We know we're unwinding. (Otherwise, we should have
                     * failed validation earlier.)  Execute the undo action.
                     * XXX need to record that the "start cancel" log entry was
                     * already there.
                     */
                    assert!(!forward);
                    live_state.queue_undo.push(node_id);
                }
                WfNodeLoadStatus::CancelFinished => {
                    /*
                     * Again, we know we're unwinding.  We've also finished
                     * cancelling this node.
                     */
                    assert!(!forward);
                    live_state.nodes_cancelled.insert(node_id);
                }
            }

            /*
             * TODO-correctness is it appropriate to have side effects in an
             * assertion here?
             */
            assert!(loaded.insert(node_id));
        }

        let (finish_tx, _) = broadcast::channel(1);

        Ok(WfExecutor {
            workflow,
            workflow_id,
            finish_tx,
            live_state: Arc::new(Mutex::new(live_state)),
        })
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
        include_self: bool,
    ) {
        if include_self {
            self.make_ancestor_tree_node(tree, live_state, node);
            return;
        }

        let ancestors = self.workflow.graph.neighbors_directed(node, Incoming);
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
        if node == self.workflow.start_node {
            return;
        }

        /*
         * If we're in this function, it's because we're looking at the ancestor
         * of a node that's currently "Running".  All such ancestors must be
         * "Done".  If they had never reached "Done", then we should never have
         * started working on the current node.  If they were "Done" but moved
         * on to "StartingCancel" or later, then that implies we've already
         * finished cancelling descendants, which would include the current
         * node.
         */
        let name = self.workflow.node_names[&node].to_string();
        let output = live_state.node_output(node);
        tree.insert(name, output);
        self.make_ancestor_tree(tree, live_state, node, false);
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
        // TODO how to check this?  do we want to take the lock?  Note that it's
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
        let (tx, mut rx) = mpsc::channel(2 * self.workflow.graph.node_count());

        loop {
            self.kick_off_ready(&tx).await;

            /*
             * Process any messages available on our channel.
             * It shouldn't be possible to get None back here.  That would mean
             * that all of the consumers have closed their ends, but we still
             * have a consumer of our own in "tx".
             */
            let message = rx.next().await.expect("broken tx");
            let task = {
                let mut live_state = self.live_state.lock().await;
                live_state.node_task_done(message.node_id)
            };

            /*
             * This should really not take long, as there's nothing else this
             * task does after sending the message that we just received.  It's
             * good to wait here to make sure things are cleaned up.
             * TODO-robustness can we enforce that this won't take long?
             */
            task.await.expect("node task failed unexpectedly");

            let mut live_state = self.live_state.lock().await;
            message.node.propagate(&self, &mut live_state);
            if live_state.exec_state == WfState::Done {
                break;
            }
        }

        /* XXX trylock, assert state == done */
        self.finish_tx.send(()).expect("failed to send finish message");
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

        /*
         * TODO is it possible to deadlock with a concurrency limit given that
         * we always do "todo" before "undo"?
         */

        let todo_queue = live_state.queue_todo.clone();
        live_state.queue_todo = Vec::new();

        for node_id in todo_queue {
            /*
             * XXX It would be good to check whether the workflow is
             * unwinding, and if so, whether this action has ever started
             * running before.  If not, then we can send this straight to
             * cancelling without doing any more work here.  What we're
             * doing here should also be safe, though.  We run the action
             * regardless, and when we complete it, we'll cancel it again.
             */
            /*
             * TODO we could be much more efficient without copying this tree
             * each time.
             */
            let mut ancestor_tree = BTreeMap::new();
            self.make_ancestor_tree(
                &mut ancestor_tree,
                &live_state,
                node_id,
                false,
            );

            let wfaction = live_state
                .workflow
                .launchers
                .get(&node_id)
                .expect("missing action for node");

            let task_params = TaskParams {
                workflow: Arc::clone(&self.workflow),
                live_state: Arc::clone(&self.live_state),
                node_id,
                done_tx: tx.clone(),
                ancestor_tree: Arc::new(ancestor_tree),
                action: Arc::clone(wfaction),
                /*
                 * XXX need to skip log start if we've ever logged that before
                 * This (and several related issues) might be addressed with a
                 * new node state and trait (like ExecStart) that would include
                 * this new state, the existing Ready, and analogous ones for
                 * Cancel.  Then we could pass a Box<dyn ExecStart> along, and
                 * it could provide a log_event() similar to what we do for
                 * finish_task().
                 */
                skip_log_start: false,
            };

            let task = tokio::spawn(WfExecutor::exec_node(task_params));
            live_state.node_task(node_id, task);
        }

        if live_state.exec_state == WfState::Done {
            assert!(live_state.queue_undo.is_empty());
            return;
        }

        let undo_queue = live_state.queue_undo.clone();
        live_state.queue_undo = Vec::new();

        for node_id in undo_queue {
            /*
             * TODO commonize with code above
             * TODO we could be much more efficient without copying this tree
             * each time.
             */
            let mut ancestor_tree = BTreeMap::new();
            self.make_ancestor_tree(
                &mut ancestor_tree,
                &live_state,
                node_id,
                true,
            );

            let wfaction = live_state
                .workflow
                .launchers
                .get(&node_id)
                .expect("missing action for node");

            let task_params = TaskParams {
                workflow: Arc::clone(&self.workflow),
                live_state: Arc::clone(&self.live_state),
                node_id,
                done_tx: tx.clone(),
                ancestor_tree: Arc::new(ancestor_tree),
                action: Arc::clone(wfaction),
                /*
                 * XXX need to skip log start if we've ever logged that before
                 */
                skip_log_start: false,
            };

            let task = tokio::spawn(WfExecutor::cancel_node(task_params));
            live_state.node_task(node_id, task);
        }
    }

    /**
     * Body of a (tokio) task that executes an action.
     */
    async fn exec_node(task_params: TaskParams) {
        let node_id = task_params.node_id;

        {
            /*
             * TODO-liveness We don't want to hold this lock across a call
             * to the database.  It's fair to say that if the database
             * hangs, the saga's corked anyway, but we should at least be
             * able to view its state, and we can't do that with this
             * design.
             */
            let mut live_state = task_params.live_state.lock().await;
            if !task_params.skip_log_start {
                WfExecutor::record_now(
                    &mut live_state.wflog,
                    node_id,
                    WfNodeEventType::Started,
                )
                .await;
            }
        }

        let exec_future = task_params.action.do_it(WfContext {
            ancestor_tree: Arc::clone(&task_params.ancestor_tree),
            node_id,
            live_state: Arc::clone(&task_params.live_state),
            workflow: Arc::clone(&task_params.workflow),
        });
        let result = exec_future.await;
        let node: Box<dyn WfNodeRest> = match result {
            Ok(output) => Box::new(WfNode { node_id, state: WfnsDone(output) }),
            Err(error) => {
                Box::new(WfNode { node_id, state: WfnsFailed(error) })
            }
        };

        WfExecutor::finish_task(task_params, node).await;
    }

    /**
     * Body of a (tokio) task that executes a compensation action.
     */
    /*
     * XXX This has a lot in common with exec_node(), but enough different that
     * it doesn't make sense to parametrize that one.  Still, it sure would be
     * nice to clean this up.
     */
    async fn cancel_node(task_params: TaskParams) {
        let node_id = task_params.node_id;

        {
            let mut live_state = task_params.live_state.lock().await;
            if !task_params.skip_log_start {
                WfExecutor::record_now(
                    &mut live_state.wflog,
                    node_id,
                    WfNodeEventType::CancelStarted,
                )
                .await;
            }
        }

        let exec_future = task_params.action.undo_it(WfContext {
            ancestor_tree: Arc::clone(&task_params.ancestor_tree),
            node_id,
            live_state: Arc::clone(&task_params.live_state),
            workflow: Arc::clone(&task_params.workflow),
        });
        /*
         * TODO-robustness We have to figure out what it means to fail here and
         * what we want to do about it.
         */
        exec_future.await.unwrap();
        let node = Box::new(WfNode { node_id, state: WfnsCancelled });
        WfExecutor::finish_task(task_params, node).await;
    }

    async fn finish_task(
        mut task_params: TaskParams,
        node: Box<dyn WfNodeRest>,
    ) {
        let node_id = task_params.node_id;
        let event_type = node.log_event();

        {
            let mut live_state = task_params.live_state.lock().await;
            WfExecutor::record_now(&mut live_state.wflog, node_id, event_type)
                .await;
        }

        task_params
            .done_tx
            .try_send(TaskCompletion { node_id, node })
            .expect("unexpected channel failure");
    }

    pub fn run(&self) -> impl Future<Output = ()> + '_ {
        let mut rx = self.finish_tx.subscribe();

        async move {
            self.run_workflow().await;
            rx.recv().await.expect("failed to receive finish message")
        }
    }

    pub fn result(&self) -> WfExecResult {
        /*
         * TODO-cleanup is there a way to make this safer?  If we could know
         * that there were no other references to the live_state (which should
         * be true, if we're done), then we could consume it, as well as "self",
         * and avoid several copies below.
         */
        let live_state = self
            .live_state
            .try_lock()
            .expect("attempted to get result while workflow still running?");
        assert_eq!(live_state.exec_state, WfState::Done);

        let mut node_results = BTreeMap::new();
        for (node_id, output) in &live_state.node_outputs {
            if *node_id == self.workflow.start_node
                || *node_id == self.workflow.end_node
            {
                continue;
            }

            let node_name = &self.workflow.node_names[node_id];
            node_results.insert(node_name.clone(), Ok(Arc::clone(output)));
        }

        WfExecResult {
            workflow_id: self.workflow_id,
            wflog: live_state.wflog.clone(),
            node_results,
            succeeded: true,
        }
    }

    // TODO-liveness does this writer need to be async?
    pub fn print_status<'a, 'b, 'c>(
        &'a self,
        out: &'b mut (dyn io::Write + Send),
        indent_level: usize,
    ) -> BoxFuture<'c, io::Result<()>>
    where
        'a: 'c,
        'b: 'c,
    {
        /* TODO-cleanup There must be a better way to do this. */
        let mut max_depth_of_node: BTreeMap<NodeIndex, usize> = BTreeMap::new();
        max_depth_of_node.insert(self.workflow.start_node, 0);

        let mut nodes_at_depth: BTreeMap<usize, Vec<NodeIndex>> =
            BTreeMap::new();

        let graph = &self.workflow.graph;
        let topo_visitor = Topo::new(graph);
        for node in topo_visitor.iter(graph) {
            if let Some(d) = max_depth_of_node.get(&node) {
                assert_eq!(*d, 0);
                assert_eq!(node, self.workflow.start_node);
                assert_eq!(max_depth_of_node.len(), 1);
                continue;
            }

            if node == self.workflow.end_node {
                continue;
            }

            let mut max_parent_depth: Option<usize> = None;
            for p in graph.neighbors_directed(node, Incoming) {
                let parent_depth = *max_depth_of_node.get(&p).unwrap();
                match max_parent_depth {
                    Some(x) if x >= parent_depth => (),
                    _ => max_parent_depth = Some(parent_depth),
                };
            }

            let depth = max_parent_depth.unwrap() + 1;
            max_depth_of_node.insert(node, depth);

            nodes_at_depth.entry(depth).or_insert(Vec::new()).push(node);
        }

        let big_indent = indent_level * 16;

        async move {
            let live_state = self.live_state.lock().await;

            write!(
                out,
                "{:width$}+ workflow execution: {}\n",
                "",
                self.workflow_id,
                width = big_indent
            )?;
            for (d, nodes) in nodes_at_depth {
                write!(
                    out,
                    "{:width$}+-- stage {:>2}: ",
                    "",
                    d,
                    width = big_indent
                )?;
                if nodes.len() == 1 {
                    let node = nodes[0];
                    let node_name = &self.workflow.node_names[&node];
                    let node_state = "unknown"; // XXX update
                    write!(out, "{}: {}\n", node_state, node_name)?;
                } else {
                    write!(out, "+ (actions in parallel)\n")?;
                    for node in nodes {
                        let node_name = &self.workflow.node_names[&node];
                        let node_state = "unknown"; // XXX update
                        let child_workflows =
                            live_state.child_workflows.get(&node);
                        let subworkflow_char =
                            if child_workflows.is_some() { '+' } else { '-' };

                        write!(
                            out,
                            "{:width$}{:>14}+-{} {}: {}\n",
                            "",
                            "",
                            subworkflow_char,
                            node_state,
                            node_name,
                            width = big_indent
                        )?;

                        if let Some(workflows) = child_workflows {
                            for c in workflows {
                                c.print_status(out, indent_level + 1).await?;
                            }
                        }
                    }
                }
            }

            Ok(())
        }
        .boxed()
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
 * TODO This would be a good place for a debug log.
 */
#[derive(Debug)]
struct WfExecLiveState {
    workflow: Arc<Workflow>,
    /** Overall execution state */
    exec_state: WfState,

    /** Queue of nodes that have not started but whose deps are satisfied */
    queue_todo: Vec<NodeIndex>,
    /** Queue of nodes whose undo action needs to be run. */
    queue_undo: Vec<NodeIndex>,

    /** Outstanding tokio tasks for each node in the graph */
    node_tasks: BTreeMap<NodeIndex, JoinHandle<()>>,

    /** Outputs saved by completed actions. */
    // TODO may as well store errors too
    node_outputs: BTreeMap<NodeIndex, WfOutput>,
    /** Set of cancelled nodes. */
    nodes_cancelled: BTreeSet<NodeIndex>,

    /** Child workflows created by a node (for status and control) */
    child_workflows: BTreeMap<NodeIndex, Vec<Arc<WfExecutor>>>,

    /** Persistent state */
    wflog: WfLog,
}

#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
enum WfNodeExecState {
    Blocked,
    QueuedToRun,
    TaskInProgress,
    Done,
    Failed,
    QueuedToUndo,
    Cancelled,
}

impl WfExecLiveState {
    /* XXX This is an awful function, I think. */
    fn node_exec_state(&self, node_id: &NodeIndex) -> WfNodeExecState {
        /*
         * This seems like overkill but it seems helpful to validate state.
         */
        let mut set: BTreeSet<WfNodeExecState> = BTreeSet::new();
        if self.nodes_cancelled.contains(node_id) {
            set.insert(WfNodeExecState::Cancelled);
        }
        if self.node_outputs.contains_key(node_id) {
            set.insert(WfNodeExecState::Done);
        }
        if self.node_tasks.contains_key(node_id) {
            set.insert(WfNodeExecState::TaskInProgress);
        }
        if self.queue_todo.contains(node_id) {
            set.insert(WfNodeExecState::QueuedToRun);
        }
        if self.queue_undo.contains(node_id) {
            set.insert(WfNodeExecState::QueuedToUndo);
        }
        /* XXX This is janky. */
        let load_status =
            self.wflog.load_status_for_node(node_id.index() as u64);
        if let WfNodeLoadStatus::Failed = load_status {
            set.insert(WfNodeExecState::Failed);
        }
        if let Some(the_state) = set.pop_first() {
            assert!(set.is_empty());
            the_state
        } else {
            if let WfNodeLoadStatus::NeverStarted = load_status {
                WfNodeExecState::Blocked
            } else {
                panic!("could not determine node state");
            }
        }
    }

    fn mark_workflow_done(&mut self) {
        assert!(self.queue_todo.is_empty());
        assert!(self.queue_undo.is_empty());
        assert!(
            self.exec_state == WfState::Running
                || self.exec_state == WfState::Unwinding
        );
        self.exec_state = WfState::Done;
    }

    fn node_task(&mut self, node_id: NodeIndex, task: JoinHandle<()>) {
        self.node_tasks.insert(node_id, task);
    }

    fn node_task_done(&mut self, node_id: NodeIndex) -> JoinHandle<()> {
        self.node_tasks
            .remove(&node_id)
            .expect("processing task completion with no task present")
    }

    fn node_output(&self, node_id: NodeIndex) -> WfOutput {
        let output =
            self.node_outputs.get(&node_id).expect("node has no output");
        Arc::clone(&output)
    }
}

/**
 * Summarizes the final state of a workflow execution.
 */
pub struct WfExecResult {
    pub workflow_id: WfId,
    pub wflog: WfLog,
    pub node_results: BTreeMap<String, WfResult>,
    succeeded: bool,
}

impl WfExecResult {
    pub fn lookup_output<T: Send + Sync + 'static>(
        &self,
        name: &str,
    ) -> Result<Arc<T>, WfError> {
        if !self.succeeded {
            return Err(anyhow!(
                "fetch output \"{}\" from workflow execution \
                \"{}\": workflow did not complete successfully",
                name,
                self.workflow_id
            ));
        }

        let result = self.node_results.get(name).expect(&format!(
            "node with name \"{}\" is not part of this workflow",
            name
        ));
        let item = result.as_ref().expect(&format!(
            "node with name \"{}\" failed and did not produce an output",
            name
        ));
        Ok(Arc::clone(&item).downcast::<T>().expect(&format!(
            "requested wrong type for output of node with name \"{}\"",
            name
        )))
    }
}

/* TODO */
fn neighbors_all<F>(
    graph: &Graph<String, ()>,
    node_id: &NodeIndex,
    direction: Direction,
    test: F,
) -> bool
where
    F: Fn(&NodeIndex) -> bool,
{
    for p in graph.neighbors_directed(*node_id, direction) {
        if !test(&p) {
            return false;
        }
    }

    return true;
}

// /**
//  * Returns true if the parent node's state is valid for the given child node's
//  * load status.
//  */
// fn recovery_validate_parent(
//     parent_state: WfNodeState,
//     child_load_status: &WfNodeLoadStatus,
// ) -> bool {
//     match child_load_status {
//         /*
//          * If the child node has started, finished successfully, finished with
//          * an error, or even started cancelling, the only allowed state for the
//          * parent node is "done".  The states prior to "done" are ruled out
//          * because we execute nodes in dependency order.  "failing" and "failed"
//          * are ruled out because we do not execute nodes whose parents failed.
//          * The cancelling states are ruled out because we cancel in
//          * reverse-dependency order, so we cannot have started cancelling the
//          * parent if the child node has not finished cancelling.  (A subtle but
//          * important implementation detail is that we do not cancel a node that
//          * has not started execution.  If we did, then the "cancel started" load
//          * state could be associated with a parent that failed.)
//          */
//         WfNodeLoadStatus::Started => parent_state == WfNodeState::Done,
//         WfNodeLoadStatus::Succeeded(_) => parent_state == WfNodeState::Done,
//         WfNodeLoadStatus::Failed => parent_state == WfNodeState::Done,
//         WfNodeLoadStatus::CancelStarted => parent_state == WfNodeState::Done,
//
//         /*
//          * If we've finished cancelling the child node, then the parent must be
//          * either "done" or one of the cancelling states.
//          */
//         WfNodeLoadStatus::CancelFinished => match parent_state {
//             WfNodeState::Done => true,
//             WfNodeState::StartingCancel => true,
//             WfNodeState::Cancelling => true,
//             WfNodeState::FinishingCancel => true,
//             WfNodeState::Cancelled => true,
//             _ => false,
//         },
//
//         /*
//          * If a node has never started, the only illegal states for a parent are
//          * those associated with cancelling, since the child must be cancelled
//          * first.
//          */
//         WfNodeLoadStatus::NeverStarted => match parent_state {
//             WfNodeState::Blocked => true,
//             WfNodeState::Ready => true,
//             WfNodeState::Starting => true,
//             WfNodeState::Running => true,
//             WfNodeState::Finishing => true,
//             WfNodeState::Done => true,
//             WfNodeState::Failing => true,
//             WfNodeState::Failed => true,
//             _ => false,
//         },
//     }
// }

/**
 * Action's handle to the workflow subsystem
 *
 * Any APIs that are useful for actions should hang off this object.  It should
 * have enough state to know which node is invoking the API.
 */
pub struct WfContext {
    ancestor_tree: Arc<BTreeMap<String, WfOutput>>,
    node_id: NodeIndex,
    workflow: Arc<Workflow>,
    live_state: Arc<Mutex<WfExecLiveState>>,
}

impl WfContext {
    /**
     * Retrieves a piece of data stored by a previous (ancestor) node in the
     * current workflow.  The data is identified by `name`.
     *
     * Data is returned as `Arc<T>` (as opposed to `T`) because all descendant
     * nodes have access to the data stored by a node (so there may be many
     * references).  Once stored, a given piece of data is immutable.
     *
     *
     * # Panics
     *
     * This function panics if there was no data previously stored with name
     * `name` or if the type of that data was not `T`.  (Data is stored as
     * `Arc<dyn Any>` and downcast to `T` here.)  The assumption here is actions
     * within a workflow are tightly coupled, so the caller knows exactly what
     * the previous action stored.  We would enforce this at compile time if we
     * could.
     */
    pub fn lookup<T: Send + Sync + 'static>(
        &self,
        name: &str,
    ) -> Result<Arc<T>, WfError> {
        let item = self
            .ancestor_tree
            .get(name)
            .expect(&format!("no ancestor called \"{}\"", name));
        let specific_item = Arc::clone(item)
            .downcast::<T>()
            .expect(&format!("ancestor \"{}\" produced unexpected type", name));
        Ok(specific_item)
    }

    /**
     * Execute a new workflow `wf` and wait for it to complete.  `wf` is
     * considered a "child" workflow of the current workflow.
     * TODO Is there some way to prevent people from instantiating their own
     * WfExecutor by mistake instead?  Even better: if they do that, can we
     * detect that they're part of a workflow already somehow and make the new
     * one a child workflow?
     * TODO Would this be better done by having a WfActionWorkflow that executed
     * a workflow as an action?  This way we would know when the Workflow was
     * constructed what the whole graph looks like, instead of only knowing
     * about child workflows once we start executing the node that creates them.
     */
    pub async fn child_workflow(&self, wf: Arc<Workflow>) -> Arc<WfExecutor> {
        /*
         * TODO Really we want this to reach into the parent WfExecutor and make
         * a record about this new execution.  This is mostly for observability
         * and control: if someone asks for the status of the parent workflow,
         * we'd like to give details on this child workflow.  Similarly if they
         * pause the parent, that should pause the child one.
         */
        let e = Arc::new(WfExecutor::new(wf));
        /* TODO-correctness Prove the lock ordering is okay here .*/
        self.live_state
            .lock()
            .await
            .child_workflows
            .entry(self.node_id)
            .or_insert(Vec::new())
            .push(Arc::clone(&e));
        e
    }

    pub fn node_label(&self) -> &str {
        self.workflow.node_names.get(&self.node_id).unwrap()
    }
}
