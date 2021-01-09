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
use petgraph::graph::NodeIndex;
use petgraph::visit::Topo;
use petgraph::visit::Walker;
use petgraph::Graph;
use petgraph::Incoming;
use petgraph::Outgoing;
use std::collections::BTreeMap;
use std::fmt;
use std::io;
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
 * XXX This should probably be an enum with different values for whether this
 * was a forward execution or a cancellation.
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
    action: Arc<dyn WfAction>,
    /** Skip logging the "start" record (if this is a restart) */
    skip_log_start: bool,
    /** This node is being cancelled, not run normally */
    unwinding: bool,
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
        w: Arc<Workflow>,
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
        let topo_visitor = Topo::new(&w.graph);
        let mut recovery = WfRecovery::new();
        let forward = !wflog.unwinding;

        for node_id in topo_visitor.iter(&w.graph) {
            let node_status =
                wflog.load_status_for_node(node_id.index() as u64);

            /*
             * Validate this node's state against its parent nodes' states.  By
             * induction, this validates everything in the graph from the start
             * node to the current node.
             */
            for parent in w.graph.neighbors_directed(node_id, Incoming) {
                let parent_state = recovery.node_state(parent);
                if !recovery_validate_parent(parent_state, node_status) {
                    return Err(anyhow!(
                        "recovery for workflow {}: node {:?}: \
                        load state is \"{:?}\", which is illegal for parent \"
                        state \"{}\"",
                        workflow_id,
                        node_id,
                        node_status,
                        parent_state,
                    ));
                }
            }

            if forward {
                WfExecutor::recover_node_forward(
                    &w,
                    node_id,
                    &node_status,
                    &mut recovery,
                );
            } else {
                WfExecutor::recover_node_unwind(
                    &w,
                    node_id,
                    &node_status,
                    &mut recovery,
                );
            }
        }

        /* See new(). */
        // TODO do we need to send a message on "finish_tx" if the workflow is
        // already done?  Obviously that won't really work here.
        let (finish_tx, _) = broadcast::channel(1);
        let live_state = recovery.to_live_state(&w, wflog);

        Ok(WfExecutor {
            workflow: Arc::clone(&w),
            workflow_id,
            finish_tx,
            live_state: Arc::new(Mutex::new(live_state)),
        })
    }

    fn recover_node_forward(
        workflow: &Workflow,
        node_id: NodeIndex,
        node_status: &WfNodeLoadStatus,
        recovery: &mut WfRecovery,
    ) {
        match node_status {
            WfNodeLoadStatus::NeverStarted => {
                /*
                 * If the parent nodes are satisfied, then we must mark this
                 * node ready to run.
                 */
                let graph = &workflow.graph;
                if parents_satisfied(graph, &recovery.node_states, node_id) {
                    recovery.node_ready(node_id);
                }
            }
            WfNodeLoadStatus::Started => {
                recovery.node_restart(node_id);
            }
            WfNodeLoadStatus::Succeeded(o) => {
                recovery.node_succeeded(node_id, Arc::clone(o));
            }
            _ => {
                todo!("handle cancellation states");
            }
        }
    }

    fn recover_node_unwind(
        workflow: &Workflow,
        node_id: NodeIndex,
        node_status: &WfNodeLoadStatus,
        recovery: &mut WfRecovery,
    ) {
        // XXX XXX XXX working here
        // NOTE: it may make more sense in this case to walk through the graph
        // in reverse topological order, for the same reason that we want to go
        // in topological order when going forward: we want to know the state of
        // our *children* in the DAG to determine what to do with *us*.
        todo!();
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
            let (task, node_state) = {
                let mut live_state = self.live_state.lock().await;
                let node_state = *live_state.node_states.get(&node_id).unwrap();
                if node_state != WfNodeState::FinishingCancel {
                    live_state.node_make_done(node_id, &message.result);
                } else {
                    live_state.node_make_cancelled(node_id);
                }
                (
                    live_state.node_task_done(node_id),
                    *live_state.node_states.get(&node_id).unwrap(),
                )
            };

            /*
             * This should really not take long, as there's nothing else this
             * task does after sending the message that we just received.  It's
             * good to wait here to make sure things are cleaned up.
             * TODO-robustness can we enforce that this won't take long?
             */
            task.await.expect("node task failed unexpectedly");

            if node_state == WfNodeState::Failed {
                todo!(); // TODO trigger unwind!
            } else {
                assert!(
                    node_state == WfNodeState::Done
                        || node_state == WfNodeState::Cancelled
                );
                if self.unblock_dependents(node_id).await {
                    break;
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
            let node_state = live_state.node_states[&node_id];
            let unwinding = if live_state.exec_state == WfState::Running {
                /*
                 * Logically, during normal execution a node should be in the
                 * "ready" state when we get here.  This is a bit kludgy, but
                 * during recovery we use the state "Starting" here to indicate
                 * that the node has already been started.  See exec_node().
                 */
                assert!(
                    node_state == WfNodeState::Ready
                        || node_state == WfNodeState::Starting
                );
                false
            } else {
                /*
                 * If we're unwinding a workflow, when a node gets here then
                 * it's either (1) been previously started, in which case we
                 * need to finish executing the normal action before we cancel
                 * it (in which case it will be in state "starting" for the
                 * reason mentioned above) or (2) successfully completed in the
                 * past (in which case it's "done").  If it never started, we
                 * wouldn't bother running anything to cancel it.  If it failed,
                 * we don't cancel it.
                 *
                 * Relatedly, just because we're unwinding the workflow does
                 * not mean we're cancelling this node now.
                 * XXX We should recheck if CancelStarted belongs here for the
                 * same reason that Starting belongs above.
                 */
                assert_eq!(live_state.exec_state, WfState::Unwinding);
                // XXX recheck this condition and the value of this block
                assert!(
                    node_state == WfNodeState::Done
                        || node_state == WfNodeState::Starting
                );
                node_state == WfNodeState::Done
            };

            /*
             * TODO we could be much more efficient without copying this tree
             * each time.
             */
            let mut ancestor_tree = BTreeMap::new();
            self.make_ancestor_tree(
                &mut ancestor_tree,
                &live_state,
                node_id,
                unwinding,
            );

            /* XXX need a way to get the actual compensation action here! */
            let wfaction = live_state
                .launchers
                .remove(&node_id)
                .expect("missing action for node");

            let task_params = TaskParams {
                live_state: Arc::clone(&self.live_state),
                node_id,
                done_tx: tx.clone(),
                ancestor_tree,
                action: wfaction,
                skip_log_start: node_state == WfNodeState::Starting,
                unwinding,
            };

            if !unwinding {
                let task = tokio::spawn(WfExecutor::exec_node(task_params));
                live_state.node_make_starting(node_id, task);
            } else {
                let task = tokio::spawn(WfExecutor::cancel_node(task_params));
                live_state.node_make_cancelling(node_id, task);
            }
        }
    }

    /**
     * Body of a (tokio) task that executes an action.
     */
    async fn exec_node(mut task_params: TaskParams) {
        let node_id = task_params.node_id;
        assert!(!task_params.unwinding);

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
            live_state.node_make_state(node_id, WfNodeState::Running);
        }

        let exec_future = task_params.action.do_it(WfContext {
            ancestor_tree: task_params.ancestor_tree,
            node_id,
            live_state: Arc::clone(&task_params.live_state),
        });
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
            live_state.node_make_state(node_id, next_state);
            WfExecutor::record_now(&mut live_state.wflog, node_id, event_type)
                .await;
        }

        task_params
            .done_tx
            .try_send(TaskCompletion { node_id, result })
            .expect("unexpected channel failure");
    }

    /**
     * Body of a (tokio) task that executes a compensation action.
     */
    /*
     * XXX This has a lot in common with exec_node(), but enough different that
     * it doesn't make sense to parametrize that one.  Still, it sure would be
     * nice to clean this up.
     */
    async fn cancel_node(mut task_params: TaskParams) {
        let node_id = task_params.node_id;
        assert!(task_params.unwinding);

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
            live_state.node_make_state(node_id, WfNodeState::Cancelling);
        }

        let exec_future = task_params.action.do_it(WfContext {
            ancestor_tree: task_params.ancestor_tree,
            node_id,
            live_state: Arc::clone(&task_params.live_state),
        });
        exec_future.await;
        {
            let mut live_state = task_params.live_state.lock().await;
            live_state.node_make_state(node_id, WfNodeState::FinishingCancel);
            WfExecutor::record_now(
                &mut live_state.wflog,
                node_id,
                WfNodeEventType::CancelFinished,
            )
            .await;
        }

        task_params
            .done_tx
            .try_send(TaskCompletion { node_id, result: Ok(Arc::new(())) })
            .expect("unexpected channel failure");
    }

    async fn unblock_dependents(&self, node_id: NodeIndex) -> bool {
        let graph = &self.workflow.graph;
        let mut live_state = self.live_state.lock().await;
        let node_states = &live_state.node_states;

        // TODO this condition needs work.  It doesn't account for
        // failed nodes or unwinding or anything.
        if live_state.exec_state == WfState::Running {
            let end_node = &self.workflow.end_node;
            if let Some(end_node_state) = node_states.get(end_node) {
                if *end_node_state == WfNodeState::Done {
                    assert_eq!(
                        graph.node_count(),
                        live_state.node_outputs.len()
                    );
                    live_state.mark_workflow_done();
                    self.finish_tx
                        .send(())
                        .expect("failed to send finish message");
                    return true;
                }
            }
        } else {
            assert_eq!(live_state.exec_state, WfState::Unwinding);
            let start_node = &self.workflow.start_node;
            if let Some(start_node_state) = node_states.get(start_node) {
                if *start_node_state == WfNodeState::Cancelled {
                    live_state.mark_workflow_done();
                    self.finish_tx
                        .send(())
                        .expect("failed to send finish message");
                    return true;
                }
            }
        }

        let node_state = node_states.get(&node_id).unwrap();
        if live_state.exec_state == WfState::Running {
            assert_eq!(*node_state, WfNodeState::Done);
            for depnode in graph.neighbors_directed(node_id, Outgoing) {
                if parents_satisfied(&graph, &live_state.node_states, depnode) {
                    live_state.node_make_ready(depnode);
                }
            }
        } else {
            assert_eq!(live_state.exec_state, WfState::Unwinding);
            /*
             * We're unwinding.  If this node just finished its normal action,
             * immediately mark it ready again for cancellation.  And there's
             * nothing else to do because its parents are not yet ready for
             * cancellation.
             */
            if *node_state == WfNodeState::Done {
                live_state.node_make_ready_cancel(node_id);
            } else {
                assert_eq!(*node_state, WfNodeState::Cancelled);
                for depnode in graph.neighbors_directed(node_id, Incoming) {
                    if children_satisfied(
                        &graph,
                        &live_state.node_states,
                        depnode,
                    ) {
                        live_state.node_make_ready_cancel(depnode);
                    }
                }
            }
        }

        false
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
                    let node_state = live_state.node_state(node);
                    write!(out, "{}: {}\n", node_state, node_name)?;
                } else {
                    write!(out, "+ (actions in parallel)\n")?;
                    for node in nodes {
                        let node_name = &self.workflow.node_names[&node];
                        let node_state = live_state.node_state(node);
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
struct WfExecLiveState {
    /** See [`Workflow::launchers`] */
    launchers: BTreeMap<NodeIndex, Arc<dyn WfAction>>,

    /** Overall execution state */
    exec_state: WfState,
    /** Execution state for each node in the graph */
    node_states: BTreeMap<NodeIndex, WfNodeState>,
    /** Outstanding tasks for each node in the graph */
    node_tasks: BTreeMap<NodeIndex, JoinHandle<()>>,
    /** Nodes that have not started but whose dependencies are satisfied */
    ready: Vec<NodeIndex>,
    /** Outputs saved by completed actions. */
    // TODO may as well store errors too
    node_outputs: BTreeMap<NodeIndex, WfOutput>,

    /** Child workflows created by a node (for status and control) */
    child_workflows: BTreeMap<NodeIndex, Vec<Arc<WfExecutor>>>,

    /** Persistent state */
    wflog: WfLog,
}

impl WfExecLiveState {
    fn mark_workflow_done(&mut self) {
        assert!(self.ready.is_empty());
        assert!(
            self.exec_state == WfState::Running
                || self.exec_state == WfState::Unwinding
        );
        self.exec_state = WfState::Done;
    }

    pub fn node_state(&self, node_id: NodeIndex) -> WfNodeState {
        self.node_states.get(&node_id).copied().unwrap_or(WfNodeState::Blocked)
    }

    fn node_make_state(&mut self, node_id: NodeIndex, state: WfNodeState) {
        let allowed = match state {
            WfNodeState::Blocked => false,
            WfNodeState::Ready => false,
            WfNodeState::Starting => true,
            WfNodeState::Running => true,
            WfNodeState::Finishing => true,
            WfNodeState::Done => false,
            WfNodeState::Failing => true,
            WfNodeState::Failed => false,
            WfNodeState::StartingCancel => false,
            WfNodeState::Cancelling => false,
            WfNodeState::FinishingCancel => false,
            WfNodeState::Cancelled => false,
        };

        if !allowed {
            panic!("cannot use node_make_state() for state: {:?}", state);
        }

        self.node_states.insert(node_id, state).unwrap();
    }

    fn node_make_ready(&mut self, node_id: NodeIndex) {
        self.node_states
            .insert(node_id, WfNodeState::Ready)
            .expect_none("readying node that had a state");
        self.ready.push(node_id);
    }

    fn node_make_ready_cancel(&mut self, node_id: NodeIndex) {
        assert_eq!(self.node_state(node_id), WfNodeState::Done);
        self.ready.push(node_id);
    }

    fn node_make_done(&mut self, node_id: NodeIndex, output: &WfResult) {
        if let Ok(output) = output {
            self.node_outputs
                .insert(node_id, Arc::clone(output))
                .expect_none("node already emitted output");
            self.node_states.insert(node_id, WfNodeState::Done);
        } else {
            self.node_states.insert(node_id, WfNodeState::Failed);
        }
    }

    fn node_make_cancelled(&mut self, node_id: NodeIndex) {
        self.node_states.insert(node_id, WfNodeState::Cancelled);
    }

    fn node_make_starting(&mut self, node_id: NodeIndex, task: JoinHandle<()>) {
        self.node_states
            .insert(node_id, WfNodeState::Starting)
            .expect("started node that had no previous state");
        self.node_tasks.insert(node_id, task);
    }

    fn node_make_cancelling(
        &mut self,
        node_id: NodeIndex,
        task: JoinHandle<()>,
    ) {
        self.node_states
            .insert(node_id, WfNodeState::StartingCancel)
            .expect("started node that had no previous state");
        self.node_tasks.insert(node_id, task);
    }

    fn node_task_done(&mut self, node_id: NodeIndex) -> JoinHandle<()> {
        let state = *self
            .node_states
            .get(&node_id)
            .expect("processing task completion for task with no state");
        assert!(
            state == WfNodeState::Done
                || state == WfNodeState::Failed
                || state == WfNodeState::Cancelled
        );
        self.node_tasks
            .remove(&node_id)
            .expect("processing task completion with no task present")
    }

    fn node_output(&self, node_id: NodeIndex) -> WfOutput {
        let node_state =
            self.node_states.get(&node_id).expect("node has not finished");
        assert_eq!(*node_state, WfNodeState::Done);
        let output = self
            .node_outputs
            .get(&node_id)
            .expect("node in state \"done\" with no output");
        Arc::clone(&output)
    }
}

/**
 * Encapsulates live state associated with workflow recovery
 */
/* TODO This would be a good place to put a bunch of debug logging. */
struct WfRecovery {
    /** Execution state for each node in the graph */
    node_states: BTreeMap<NodeIndex, WfNodeState>,
    /** Nodes that have not started but whose dependencies are satisfied */
    ready: Vec<NodeIndex>,
    /** Outputs saved by completed actions. */
    // TODO may as well store errors too
    node_outputs: BTreeMap<NodeIndex, WfOutput>,
}

impl WfRecovery {
    fn new() -> WfRecovery {
        WfRecovery {
            node_states: BTreeMap::new(),
            ready: Vec::new(),
            node_outputs: BTreeMap::new(),
        }
    }

    pub fn node_state(&self, node_id: NodeIndex) -> WfNodeState {
        self.node_states.get(&node_id).copied().unwrap_or(WfNodeState::Blocked)
    }

    fn node_recover_state(
        &mut self,
        node_id: NodeIndex,
        new_state: WfNodeState,
    ) {
        let old_state = self.node_states.insert(node_id, new_state);
        if let Some(old_state) = old_state {
            panic!(
                "duplicate recovery for node {:?}: previously \
                \"{}\", now trying to mark \"{}\"",
                node_id, old_state, new_state,
            );
        }
    }

    pub fn node_ready(&mut self, node_id: NodeIndex) {
        self.node_recover_state(node_id, WfNodeState::Ready);
        self.ready.push(node_id);
    }

    pub fn node_restart(&mut self, node_id: NodeIndex) {
        self.node_recover_state(node_id, WfNodeState::Starting);
        self.ready.push(node_id);
    }

    pub fn node_succeeded(&mut self, node_id: NodeIndex, output: WfOutput) {
        self.node_recover_state(node_id, WfNodeState::Done);
        self.node_outputs.insert(node_id, output).expect_none(
            "recorded duplicate output for a node with no duplicate state",
        );
    }

    pub fn to_live_state(self, w: &Workflow, wflog: WfLog) -> WfExecLiveState {
        /*
         * TODO This would be a good time to run some consistency checks.  For
         * example, are there any nodes that have started whose ancestors have
         * not finished? etc.
         */
        /*
         * At this point, we've completed recovery for all the nodes in the log.
         * Now we have to determine if we're currently running the workflow
         * forward, running it backwards (unwinding), or finished one way or the
         * other.  WWe can determine this by looking at the start and end nodes
         * of the graph.
         */
        let start_state = self.node_state(w.start_node);
        assert_ne!(start_state, WfNodeState::Blocked);
        let end_state = self.node_state(w.end_node);
        /* TODO There are more cases than this. */
        let exec_state = if end_state == WfNodeState::Done {
            /*
             * It would be a violation of our own invariants if we haven't
             * accounted for the results for every node.  (An inconsistent log
             * should not allow us to get here because when we recovered the
             * "done" record for the end node, we would have found an ancestor
             * that was not also "done".)
             */
            assert_eq!(self.node_outputs.len(), w.graph.node_count());
            assert_eq!(self.node_states.len(), w.graph.node_count());
            assert!(self.ready.is_empty());
            WfState::Done
        } else {
            WfState::Running
        };

        WfExecLiveState {
            launchers: w.launchers.clone(),
            wflog,
            ready: self.ready,
            exec_state,
            node_states: self.node_states,
            node_outputs: self.node_outputs,
            node_tasks: BTreeMap::new(),
            child_workflows: BTreeMap::new(),
        }
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

/**
 * Checks whether all of this node's incoming edges are now satisfied.
 */
fn parents_satisfied(
    graph: &Graph<String, ()>,
    node_states: &BTreeMap<NodeIndex, WfNodeState>,
    node_id: NodeIndex,
) -> bool {
    for p in graph.neighbors_directed(node_id, Incoming) {
        let node_state = node_states.get(&p).unwrap_or(&WfNodeState::Blocked);
        if *node_state != WfNodeState::Done {
            return false;
        }
    }

    return true;
}

/**
 * Checks whether all of this node's outgoing edges are now satisfied for
 * cancellation.
 */
/* TODO commonize with parents_satisfied() */
fn children_satisfied(
    graph: &Graph<String, ()>,
    node_states: &BTreeMap<NodeIndex, WfNodeState>,
    node_id: NodeIndex,
) -> bool {
    for p in graph.neighbors_directed(node_id, Outgoing) {
        let node_state = node_states.get(&p).unwrap_or(&WfNodeState::Blocked);
        if *node_state != WfNodeState::Cancelled {
            return false;
        }
    }

    return true;
}

/**
 * Returns true if the parent node's state is valid for the given child node's
 * load status.
 */
fn recovery_validate_parent(
    parent_state: WfNodeState,
    child_load_status: &WfNodeLoadStatus,
) -> bool {
    match child_load_status {
        /*
         * If the child node has started, finished successfully, finished with
         * an error, or even started cancelling, the only allowed state for the
         * parent node is "done".  The states prior to "done" are ruled out
         * because we execute nodes in dependency order.  "failing" and "failed"
         * are ruled out because we do not execute nodes whose parents failed.
         * The cancelling states are ruled out because we cancel in
         * reverse-dependency order, so we cannot have started cancelling the
         * parent if the child node has not finished cancelling.  (A subtle but
         * important implementation detail is that we do not cancel a node that
         * has not started execution.  If we did, then the "cancel started" load
         * state could be associated with a parent that failed.)
         */
        WfNodeLoadStatus::Started => parent_state == WfNodeState::Done,
        WfNodeLoadStatus::Succeeded(_) => parent_state == WfNodeState::Done,
        WfNodeLoadStatus::Failed => parent_state == WfNodeState::Done,
        WfNodeLoadStatus::CancelStarted => parent_state == WfNodeState::Done,

        /*
         * If we've finished cancelling the child node, then the parent must be
         * either "done" or one of the cancelling states.
         */
        WfNodeLoadStatus::CancelFinished => match parent_state {
            WfNodeState::Done => true,
            WfNodeState::StartingCancel => true,
            WfNodeState::Cancelling => true,
            WfNodeState::FinishingCancel => true,
            WfNodeState::Cancelled => true,
            _ => false,
        },

        /*
         * If a node has never started, the only illegal states for a parent are
         * those associated with cancelling, since the child must be cancelled
         * first.
         */
        WfNodeLoadStatus::NeverStarted => match parent_state {
            WfNodeState::Blocked => true,
            WfNodeState::Ready => true,
            WfNodeState::Starting => true,
            WfNodeState::Running => true,
            WfNodeState::Finishing => true,
            WfNodeState::Done => true,
            WfNodeState::Failing => true,
            WfNodeState::Failed => true,
            _ => false,
        },
    }
}

/**
 * Action's handle to the workflow subsystem
 *
 * Any APIs that are useful for actions should hang off this object.  It should
 * have enough state to know which node is invoking the API.
 */
pub struct WfContext {
    ancestor_tree: BTreeMap<String, WfOutput>,
    node_id: NodeIndex,
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
}
