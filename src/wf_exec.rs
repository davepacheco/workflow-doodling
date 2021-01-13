//! Manages execution of a workflow

use crate::wf_log::WfNodeEventType;
use crate::wf_log::WfNodeLoadStatus;
use crate::WfAction;
use crate::WfActionInjectError;
use crate::WfActionResult;
use crate::WfError;
use crate::WfActionOutput;
use crate::WfId;
use crate::WfLog;
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
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::fmt;
use std::io;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use uuid::Uuid;

/*
 * TODO-design Should we go even further and say that each node is its own
 * struct with incoming channels from parents (to notify when done), from
 * children (to notify when undone), and to each direction as well?  Then the
 * whole thing is a message passing exercise?
 */
struct WfnsDone(Arc<JsonValue>);
struct WfnsFailed(WfError);
struct WfnsUndone;

struct WfNode<S: WfNodeStateType> {
    node_id: NodeIndex,
    state: S,
}

trait WfNodeStateType {}
impl WfNodeStateType for WfnsDone {}
impl WfNodeStateType for WfnsFailed {}
impl WfNodeStateType for WfnsUndone {}

/* TODO-design Is this right?  Is the trait supposed to be empty? */
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
             * nodes are "undone" already.
             */
            if neighbors_all(graph, &self.node_id, Outgoing, |child| {
                live_state.nodes_undone.contains(child)
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
             * immediately move this node to "undone" and unblock its
             * dependents, but for consistency with a simpler algorithm,
             * we'll wait for unwinding to propagate from the end node.
             * If all of our children are already undone, however, we
             * must go ahead and mark ourselves undone and propagate
             * that.
             */
            if neighbors_all(graph, &self.node_id, Outgoing, |child| {
                live_state.nodes_undone.contains(child)
            }) {
                let new_node =
                    WfNode { node_id: self.node_id, state: WfnsUndone };
                new_node.propagate(exec, live_state);
            }
        } else {
            /*
             * Begin the unwinding process.  Start with the end node: mark
             * it trivially "undone" and propagate that.
             */
            live_state.exec_state = WfState::Unwinding;
            assert_ne!(self.node_id, exec.workflow.end_node);
            let new_node =
                WfNode { node_id: exec.workflow.end_node, state: WfnsUndone };
            new_node.propagate(exec, live_state);
        }
    }
}

impl WfNodeRest for WfNode<WfnsUndone> {
    fn log_event(&self) -> WfNodeEventType {
        WfNodeEventType::UndoFinished
    }

    fn propagate(&self, exec: &WfExecutor, live_state: &mut WfExecLiveState) {
        let graph = &exec.workflow.graph;
        assert_eq!(live_state.exec_state, WfState::Unwinding);
        /* TODO side effect in assertion? */
        assert!(live_state.nodes_undone.insert(self.node_id));

        if self.node_id == exec.workflow.start_node {
            /*
             * If we've undone the start node, the workflow is done.
             */
            live_state.mark_workflow_done();
            return;
        }

        /*
         * During unwinding, a node's becoming undone means it's time to check
         * ancestor nodes to see if they're now undoable.
         */
        for parent in graph.neighbors_directed(self.node_id, Incoming) {
            if neighbors_all(graph, &parent, Outgoing, |child| {
                live_state.nodes_undone.contains(child)
            }) {
                /*
                 * We're ready to undo "parent".  We don't know whether it's
                 * finished running, on the todo queue, or currenting
                 * outstanding.  (It should not be on the undo queue!)
                 * TODO-design Here's an awful approach just intended to let us
                 * flesh out more of the rest of this to better understand how
                 * to manage state.
                 */
                match live_state.node_exec_state(&parent) {
                    WfNodeExecState::Blocked | WfNodeExecState::Failed => {
                        /*
                         * If the node never started or if it failed, we can
                         * just mark it undone without doing anything else.
                         */
                        let new_node =
                            WfNode { node_id: parent, state: WfnsUndone };
                        new_node.propagate(exec, live_state);
                        continue;
                    }

                    WfNodeExecState::QueuedToRun
                    | WfNodeExecState::TaskInProgress => {
                        /*
                         * If we're running an action for this task, there's
                         * nothing we can do right now, but we'll handle it when
                         * it finishes.  We could do better with queued (and
                         * there's a TODO-design in kick_off_ready() to do so),
                         * but this isn't wrong as-is.
                         */
                        continue;
                    }

                    WfNodeExecState::Done => {
                        /*
                         * We have to actually run the undo action.
                         */
                        live_state.queue_undo.push(parent);
                    }

                    WfNodeExecState::QueuedToUndo | WfNodeExecState::Undone => {
                        panic!(
                            "already undoing or undone node \
                            whose child was just now undone"
                        );
                    }
                }
            }
        }
    }
}

/**
 * Execution state for the workflow overall
 */
#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
enum WfState {
    Running,
    Unwinding,
    Done,
}

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

    // TODO-cleanup should not need a copy here.
    creator: String,

    /** id of the graph node whose action we're running */
    node_id: NodeIndex,
    /** channel over which to send completion message */
    done_tx: mpsc::Sender<TaskCompletion>,
    /** Ancestor tree for this node.  See [`WfContext`]. */
    // TODO-cleanup there's no reason this should be an Arc.
    ancestor_tree: Arc<BTreeMap<String, Arc<JsonValue>>>,
    /** The action itself that we're executing. */
    action: Arc<dyn WfAction>,
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

    creator: String,

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
    pub fn new(w: Arc<Workflow>, creator: &str) -> WfExecutor {
        let workflow_id = Uuid::new_v4();
        let wflog = WfLog::new(creator, workflow_id);
        WfExecutor::new_recover(w, wflog, creator).unwrap()
    }

    /**
     * Create an executor to run the given workflow that may have already
     * started, using the given log events.
     */
    pub fn new_recover(
        workflow: Arc<Workflow>,
        wflog: WfLog,
        creator: &str,
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
            nodes_undone: BTreeSet::new(),
            child_workflows: BTreeMap::new(),
            wflog: wflog,
            injected_errors: BTreeSet::new(),
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
             * Validate this node's state against its parent nodes' states.  By
             * induction, this validates everything in the graph from the start
             * or end node to the current node.
             */
            for parent in workflow.graph.neighbors_directed(node_id, Incoming) {
                let parent_status = live_state
                    .wflog
                    .load_status_for_node(parent.index() as u64);
                if !recovery_validate_parent(parent_status, node_status) {
                    return Err(anyhow!(
                        "recovery for workflow {}: node {:?}: \
                        load status is \"{:?}\", which is illegal for \
                        parent load status \"{:?}\"",
                        workflow_id,
                        node_id,
                        node_status,
                        parent_status,
                    ));
                }
            }

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
                        live_state.nodes_undone.contains(c)
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
                             * undone and which has never started.  Just mark
                             * it undone.
                             * TODO-design Does this suggest a better way to do
                             * this might be to simply load all the state that
                             * we have into the WfExecLiveState and execute the
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
                            live_state.nodes_undone.insert(node_id);
                        }
                        _ => (),
                    }
                }
                WfNodeLoadStatus::Started => {
                    /*
                     * Whether we're unwinding or not, we have to finish
                     * execution of this action.
                     */
                    live_state.queue_todo.push(node_id);
                }
                WfNodeLoadStatus::Succeeded(output) => {
                    /*
                     * If the node has finished executing and not started
                     * undoing, and if we're unwinding and the children have
                     * all finished undoing, then it's time to undo this
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
                     * have all been undone, it's time to undo this one.
                     * But we just mark it undone -- we don't execute the
                     * undo action.
                     */
                    if let RecoveryDirection::Unwind(true) = direction {
                        live_state.nodes_undone.insert(node_id);
                    }
                }
                WfNodeLoadStatus::UndoStarted => {
                    /*
                     * We know we're unwinding. (Otherwise, we should have
                     * failed validation earlier.)  Execute the undo action.
                     */
                    assert!(!forward);
                    live_state.queue_undo.push(node_id);
                }
                WfNodeLoadStatus::UndoFinished => {
                    /*
                     * Again, we know we're unwinding.  We've also finished
                     * undoing this node.
                     */
                    assert!(!forward);
                    live_state.nodes_undone.insert(node_id);
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
            creator: creator.to_owned(),
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
        tree: &mut BTreeMap<String, Arc<JsonValue>>,
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
        tree: &mut BTreeMap<String, Arc<JsonValue>>,
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
         * on to one of the undoing states, then that implies we've already
         * finished undoing descendants, which would include the current node.
         */
        let name = self.workflow.node_names[&node].to_string();
        let output = live_state.node_output(node);
        tree.insert(name, output);
        self.make_ancestor_tree(tree, live_state, node, false);
    }

    pub async fn inject_error(&self, node_name: &str) {
        /* First, find the node with that name. */
        let mut maybe_node_id: Option<NodeIndex> = None;
        for (node, name) in &self.workflow.node_names {
            if name == node_name {
                maybe_node_id = Some(*node);
                break;
            }
        }

        if let Some(node_id) = maybe_node_id {
            let mut live_state = self.live_state.lock().await;
            live_state.injected_errors.insert(node_id);
        } else {
            panic!("no such node in workflow: \"{}\"", node_name);
        }
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

        let live_state = self.live_state.try_lock().unwrap();
        assert_eq!(live_state.exec_state, WfState::Done);
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
             * TODO-design It would be good to check whether the workflow is
             * unwinding, and if so, whether this action has ever started
             * running before.  If not, then we can send this straight to
             * undoing without doing any more work here.  What we're
             * doing here should also be safe, though.  We run the action
             * regardless, and when we complete it, we'll undo it.
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

            let wfaction = if live_state.injected_errors.contains(&node_id) {
                Arc::new(WfActionInjectError {}) as Arc<dyn WfAction>
            } else {
                Arc::clone(
                    live_state
                        .workflow
                        .launchers
                        .get(&node_id)
                        .expect("missing action for node"),
                )
            };

            let task_params = TaskParams {
                workflow: Arc::clone(&self.workflow),
                live_state: Arc::clone(&self.live_state),
                node_id,
                done_tx: tx.clone(),
                ancestor_tree: Arc::new(ancestor_tree),
                action: wfaction,
                creator: self.creator.clone(),
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
                creator: self.creator.clone(),
            };

            let task = tokio::spawn(WfExecutor::undo_node(task_params));
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
            let load_status =
                live_state.wflog.load_status_for_node(node_id.index() as u64);
            match load_status {
                WfNodeLoadStatus::NeverStarted => {
                    WfExecutor::record_now(
                        &mut live_state.wflog,
                        node_id,
                        WfNodeEventType::Started,
                    )
                    .await;
                }
                WfNodeLoadStatus::Started => (),
                WfNodeLoadStatus::Succeeded(_)
                | WfNodeLoadStatus::Failed
                | WfNodeLoadStatus::UndoStarted
                | WfNodeLoadStatus::UndoFinished => {
                    panic!("starting node in bad state")
                }
            }
        }

        let exec_future = task_params.action.do_it(WfContext {
            ancestor_tree: Arc::clone(&task_params.ancestor_tree),
            node_id,
            live_state: Arc::clone(&task_params.live_state),
            workflow: Arc::clone(&task_params.workflow),
            creator: task_params.creator.clone(),
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
     * TODO-cleanup This has a lot in common with exec_node(), but enough
     * different that it doesn't make sense to parametrize that one.  Still, it
     * sure would be nice to clean this up.
     */
    async fn undo_node(task_params: TaskParams) {
        let node_id = task_params.node_id;

        {
            let mut live_state = task_params.live_state.lock().await;
            let load_status =
                live_state.wflog.load_status_for_node(node_id.index() as u64);
            match load_status {
                WfNodeLoadStatus::Succeeded(_) => {
                    WfExecutor::record_now(
                        &mut live_state.wflog,
                        node_id,
                        WfNodeEventType::UndoStarted,
                    )
                    .await;
                }
                WfNodeLoadStatus::UndoStarted => (),
                WfNodeLoadStatus::NeverStarted
                | WfNodeLoadStatus::Started
                | WfNodeLoadStatus::Failed
                | WfNodeLoadStatus::UndoFinished => {
                    panic!("undoing node in bad state")
                }
            }
        }

        let exec_future = task_params.action.undo_it(WfContext {
            ancestor_tree: Arc::clone(&task_params.ancestor_tree),
            node_id,
            live_state: Arc::clone(&task_params.live_state),
            workflow: Arc::clone(&task_params.workflow),
            creator: task_params.creator.clone(),
        });
        /*
         * TODO-robustness We have to figure out what it means to fail here and
         * what we want to do about it.
         */
        exec_future.await.unwrap();
        let node = Box::new(WfNode { node_id, state: WfnsUndone });
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
                    let node_state = live_state.node_exec_state(&node);
                    write!(out, "{}: {}\n", node_state, node_name)?;
                } else {
                    write!(out, "+ (actions in parallel)\n")?;
                    for node in nodes {
                        let node_name = &self.workflow.node_names[&node];
                        let node_state = live_state.node_exec_state(&node);
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
    node_outputs: BTreeMap<NodeIndex, Arc<JsonValue>>,
    /** Set of undone nodes. */
    nodes_undone: BTreeSet<NodeIndex>,

    /** Child workflows created by a node (for status and control) */
    child_workflows: BTreeMap<NodeIndex, Vec<Arc<WfExecutor>>>,

    /** Persistent state */
    wflog: WfLog,

    /** Injected errors */
    injected_errors: BTreeSet<NodeIndex>,
}

#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
enum WfNodeExecState {
    Blocked,
    QueuedToRun,
    TaskInProgress,
    Done,
    Failed,
    QueuedToUndo,
    Undone,
}

impl fmt::Display for WfNodeExecState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            WfNodeExecState::Blocked => "blocked",
            WfNodeExecState::QueuedToRun => "queued-todo",
            WfNodeExecState::TaskInProgress => "working",
            WfNodeExecState::Done => "done",
            WfNodeExecState::Failed => "failed",
            WfNodeExecState::QueuedToUndo => "queued-undo",
            WfNodeExecState::Undone => "undone",
        })
    }
}

impl WfExecLiveState {
    /*
     * TODO-design The current implementation does not use explicit state.  In
     * most cases, this made things better than before because each hunk of code
     * was structured to accept only nodes in states that were valid.  But
     * there are a few cases where we need a bit more state than we're currently
     * keeping.  This function is used there.
     *
     * It's especially questionable to use load_status here -- or is that the
     * way we should go more generally?  See TODO-design in new_recover().
     */
    fn node_exec_state(&self, node_id: &NodeIndex) -> WfNodeExecState {
        /*
         * This seems like overkill but it seems helpful to validate state.
         */
        let mut set: BTreeSet<WfNodeExecState> = BTreeSet::new();
        let load_status =
            self.wflog.load_status_for_node(node_id.index() as u64);
        if self.nodes_undone.contains(node_id) {
            set.insert(WfNodeExecState::Undone);
        } else if self.node_outputs.contains_key(node_id) {
            set.insert(WfNodeExecState::Done);
        } else if let WfNodeLoadStatus::Failed = load_status {
            set.insert(WfNodeExecState::Failed);
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

    fn node_output(&self, node_id: NodeIndex) -> Arc<JsonValue> {
        let output =
            self.node_outputs.get(&node_id).expect("node has no output");
        Arc::clone(output)
    }
}

/**
 * Summarizes the final state of a workflow execution.
 */
pub struct WfExecResult {
    pub workflow_id: WfId,
    pub wflog: WfLog,
    pub node_results: BTreeMap<String, WfActionResult>,
    succeeded: bool,
}

impl WfExecResult {
    pub fn lookup_output<T: WfActionOutput + 'static>(
        &self,
        name: &str,
    ) -> Result<T, WfError> {
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
        // TODO-cleanup double-asterisk seems ridiculous
        let parsed: T = serde_json::from_value((**item).clone()).expect(&format!(
            "requested wrong type for output of node with name \"{}\"",
            name
        ));
        Ok(parsed)
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

/**
 * Returns true if the parent node's load status is valid for the given child
 * node's load status.
 */
fn recovery_validate_parent(
    parent_status: &WfNodeLoadStatus,
    child_status: &WfNodeLoadStatus,
) -> bool {
    match (child_status, parent_status) {
        /*
         * If the child node has started, finished successfully, finished with
         * an error, or even started undoing, the only allowed status for the
         * parent node is "done".  The states prior to "done" are ruled out
         * because we execute nodes in dependency order.  "failed" is ruled out
         * because we do not execute nodes whose parents failed.  The undoing
         * states are ruled out because we unwind in reverse-dependency order,
         * so we cannot have started undoing the parent if the child node has
         * not finished undoing.  (A subtle but important implementation
         * detail is that we do not undo a node that has not started
         * execution.  If we did, then the "undo started" load state could be
         * associated with a parent that failed.)
         */
        (
            WfNodeLoadStatus::Started
            | WfNodeLoadStatus::Succeeded(_)
            | WfNodeLoadStatus::Failed
            | WfNodeLoadStatus::UndoStarted,
            WfNodeLoadStatus::Succeeded(_),
        ) => true,

        /*
         * If we've finished undoing the child node, then the parent must be
         * either "done" or one of the undoing states.
         */
        (
            WfNodeLoadStatus::UndoFinished,
            WfNodeLoadStatus::Succeeded(_)
            | WfNodeLoadStatus::UndoStarted
            | WfNodeLoadStatus::UndoFinished,
        ) => true,

        /*
         * If a node has never started, the only illegal states for a parent are
         * those associated with undoing, since the child must be undone first.
         */
        (
            WfNodeLoadStatus::NeverStarted,
            WfNodeLoadStatus::NeverStarted
            | WfNodeLoadStatus::Started
            | WfNodeLoadStatus::Succeeded(_)
            | WfNodeLoadStatus::Failed,
        ) => true,
        _ => false,
    }
}

/**
 * Action's handle to the workflow subsystem
 *
 * Any APIs that are useful for actions should hang off this object.  It should
 * have enough state to know which node is invoking the API.
 */
pub struct WfContext {
    ancestor_tree: Arc<BTreeMap<String, Arc<JsonValue>>>,
    node_id: NodeIndex,
    workflow: Arc<Workflow>,
    live_state: Arc<Mutex<WfExecLiveState>>,
    /* TODO-cleanup should not need a copy here */
    creator: String,
}

impl WfContext {
    /**
     * Retrieves a piece of data stored by a previous (ancestor) node in the
     * current workflow.  The data is identified by `name`.
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
    // XXX why is this a Result?
    pub fn lookup<T: WfActionOutput + 'static>(
        &self,
        name: &str,
    ) -> Result<T, WfError> {
        let item = self
            .ancestor_tree
            .get(name)
            .expect(&format!("no ancestor called \"{}\"", name));
        // TODO-cleanup double-asterisk seems ridiculous
        let specific_item = serde_json::from_value((**item).clone())
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
        let e = Arc::new(WfExecutor::new(wf, &self.creator));
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
