/*
 * This file contains an in-progress prototype interface for workflows.
 *
 * Fundamentally, our workflows are a directed acyclic graph of possibly
 * reversible actions, similar to distributed sagas.  There are two main
 * functional pieces: an interface for constructing the graph and an execution
 * engine for carrying out the actions described by the graph.  There are many
 * subparts to this problem.  See the adjacent notes.txt for a lot more
 * thoughts about this, some rather rambling.
 */

#![deny(elided_lifetimes_in_paths)]
#![feature(map_first_last)]
#![feature(option_expect_none)]
#![feature(option_unwrap_none)]
#![feature(or_patterns)]
#![feature(type_name_of_val)]

mod example_provision;
mod wf_exec;
mod wf_log;

use anyhow::anyhow;
use anyhow::Context;
use async_trait::async_trait;
use core::fmt;
use core::fmt::Debug;
use core::future::Future;
use core::marker::PhantomData;
use petgraph::graph::NodeIndex;
use petgraph::Graph;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::collections::BTreeMap;
use std::sync::Arc;
use uuid::Uuid;

pub use example_provision::make_provision_workflow;
pub use wf_exec::WfContext;
pub use wf_exec::WfExecutor;
pub use wf_log::WfLog;
pub use wf_log::WfLogResult;

/* Widely-used types (within workflows) */

/** Unique identifier for a Workflow */
/*
 * TODO-cleanup make this a "newtype".  We may want the display form to have a
 * "w-" prefix (or something like that).  (Does that mean the type needs to be
 * caller-provided?)
 */
pub type WfId = Uuid;
/** Error produced by a workflow action or a workflow itself */
pub type WfError = anyhow::Error;
/** Result of a function implementing a workflow action */
pub type WfFuncResult<T> = Result<T, WfError>;
/** Result of a function implementing a workflow action */
// TODO-cleanup can we drop this Arc?
pub type WfActionResult = Result<Arc<JsonValue>, WfError>;
/** Result of a workflow undo action. */
pub type WfUndoResult = Result<(), WfError>;

pub trait WfActionOutput:
    Debug + DeserializeOwned + Serialize + Send + Sync
{
}
impl<T: Debug + DeserializeOwned + Serialize + Send + Sync> WfActionOutput
    for T
{
}

/**
 * Building blocks of workflows
 *
 * Each action consumes a [`WfContext`] and asynchronously produces a
 * [`WfActionResult`].  A workflow is essentially a directed acyclic graph of
 * actions with dependencies between them.
 */
#[async_trait]
pub trait WfAction: Debug + Send + Sync {
    /**
     * Executes the action for this workflow node, whatever that is.  Actions
     * function like requests in distributed sagas: critically, they must be
     * idempotent.  They should be very careful in using interfaces outside of
     * [`WfContext`] -- we want them to be as self-contained as possible to
     * ensure idempotence and to minimize versioning issues.
     *
     * On success, this function produces a `WfActionOutput`.  This output will
     * be stored persistently, keyed by the _name_ of the current workflow node.
     * Subsequent stages can access this data with [`WfContext::lookup`].  This
     * is the _only_ supported means of sharing state across actions within a
     * workflow.
     */
    async fn do_it(&self, wfctx: WfContext) -> WfActionResult;

    /**
     * Executes the compensation action for this workflow node, whatever that
     * is.
     */
    async fn undo_it(&self, wfctx: WfContext) -> WfUndoResult;
}

/**
 * [`WfAction`] implementation for functions
 */
pub struct WfActionFunc<
    ActionFutType,
    ActionFuncType,
    ActionFuncOutput,
    UndoFutType,
    UndoFuncType,
> where
    ActionFuncType: Fn(WfContext) -> ActionFutType + Send + Sync + 'static,
    ActionFutType:
        Future<Output = WfFuncResult<ActionFuncOutput>> + Send + Sync + 'static,
    ActionFuncOutput: WfActionOutput + 'static,
    UndoFuncType: Fn(WfContext) -> UndoFutType + Send + Sync + 'static,
    UndoFutType: Future<Output = WfUndoResult> + Send + Sync + 'static,
{
    action_func: ActionFuncType,
    undo_func: UndoFuncType,
    phantom: PhantomData<(ActionFutType, UndoFutType)>,
}

impl<
        ActionFutType,
        ActionFuncType,
        ActionFuncOutput,
        UndoFutType,
        UndoFuncType,
    >
    WfActionFunc<
        ActionFutType,
        ActionFuncType,
        ActionFuncOutput,
        UndoFutType,
        UndoFuncType,
    >
where
    ActionFuncType: Fn(WfContext) -> ActionFutType + Send + Sync + 'static,
    ActionFutType:
        Future<Output = WfFuncResult<ActionFuncOutput>> + Send + Sync + 'static,
    ActionFuncOutput: WfActionOutput + 'static,
    UndoFuncType: Fn(WfContext) -> UndoFutType + Send + Sync + 'static,
    UndoFutType: Future<Output = WfUndoResult> + Send + Sync + 'static,
{
    /**
     * Wrap a function in a `WfActionFunc`
     *
     * We return the result as a `Arc<dyn WfAction>` so that it can be used
     * directly where `WfAction`s are expected.  The struct `WfActionFunc` has
     * no interfaces of its own so there's generally no need to have the
     * specific type.
     */
    pub fn new_action(
        action_func: ActionFuncType,
        undo_func: UndoFuncType,
    ) -> Arc<dyn WfAction> {
        Arc::new(WfActionFunc { action_func, undo_func, phantom: PhantomData })
    }
}

/*
 * TODO-cleanup why can't new_action_noop_undo be in the WfAction namespace?
 */

async fn undo_noop(wfctx: WfContext) -> WfUndoResult {
    eprintln!("<noop undo for node: \"{}\">", wfctx.node_label());
    Ok(())
}

/**
 * Wrap an action function whose "undo" is a noop.
 */
pub fn new_action_noop_undo<ActionFutType, ActionFuncType, ActionFuncOutput>(
    f: ActionFuncType,
) -> Arc<dyn WfAction>
where
    ActionFuncType: Fn(WfContext) -> ActionFutType + Send + Sync + 'static,
    ActionFutType:
        Future<Output = WfFuncResult<ActionFuncOutput>> + Send + Sync + 'static,
    ActionFuncOutput: WfActionOutput + 'static,
{
    WfActionFunc::new_action(f, undo_noop)
}

#[async_trait]
impl<
        ActionFutType,
        ActionFuncType,
        ActionFuncOutput,
        UndoFutType,
        UndoFuncType,
    > WfAction
    for WfActionFunc<
        ActionFutType,
        ActionFuncType,
        ActionFuncOutput,
        UndoFutType,
        UndoFuncType,
    >
where
    ActionFuncType: Fn(WfContext) -> ActionFutType + Send + Sync + 'static,
    ActionFutType:
        Future<Output = WfFuncResult<ActionFuncOutput>> + Send + Sync + 'static,
    ActionFuncOutput: WfActionOutput + 'static,
    UndoFuncType: Fn(WfContext) -> UndoFutType + Send + Sync + 'static,
    UndoFutType: Future<Output = WfUndoResult> + Send + Sync + 'static,
{
    async fn do_it(&self, wfctx: WfContext) -> WfActionResult {
        let label = wfctx.node_label().to_owned();
        let fut = { (self.action_func)(wfctx) };
        fut.await
            .with_context(|| format!("executing node \"{}\"", label))
            .and_then(|func_output| {
                serde_json::to_value(func_output).with_context(|| {
                    format!("serializing output from node \"{}\"", label)
                })
            })
            .map(Arc::new)
    }

    async fn undo_it(&self, wfctx: WfContext) -> WfUndoResult {
        let fut = { (self.undo_func)(wfctx) };
        fut.await
    }
}

impl<
        ActionFutType,
        ActionFuncType,
        ActionFuncOutput,
        UndoFutType,
        UndoFuncType,
    > Debug
    for WfActionFunc<
        ActionFutType,
        ActionFuncType,
        ActionFuncOutput,
        UndoFutType,
        UndoFuncType,
    >
where
    ActionFuncType: Fn(WfContext) -> ActionFutType + Send + Sync + 'static,
    ActionFutType:
        Future<Output = WfFuncResult<ActionFuncOutput>> + Send + Sync + 'static,
    ActionFuncOutput: WfActionOutput + 'static,
    UndoFuncType: Fn(WfContext) -> UndoFutType + Send + Sync + 'static,
    UndoFutType: Future<Output = WfUndoResult> + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&std::any::type_name_of_val(&self.action_func))
    }
}

/** Placeholder type for the start node in a graph. */
#[derive(Debug)]
struct WfActionUniversalStart {}

#[async_trait]
impl WfAction for WfActionUniversalStart {
    async fn do_it(&self, _: WfContext) -> WfActionResult {
        eprintln!("<action for \"start\" node>");
        Ok(Arc::new(JsonValue::Null))
    }

    async fn undo_it(&self, _: WfContext) -> WfUndoResult {
        eprintln!(
            "<undo for \"start\" node (workflow is nearly done unwinding)>"
        );
        Ok(())
    }
}

/** Placeholder type for the end node in a graph. */
#[derive(Debug)]
struct WfActionUniversalEnd {}

#[async_trait]
impl WfAction for WfActionUniversalEnd {
    async fn do_it(&self, _: WfContext) -> WfActionResult {
        eprintln!("<action for \"end\" node: workflow is nearly done>");
        Ok(Arc::new(JsonValue::Null))
    }

    async fn undo_it(&self, _: WfContext) -> WfUndoResult {
        /*
         * We should not run compensation actions for nodes that have not
         * started.  We should never start this node unless all other actions
         * have completed.  We should never unwind a workflow unless some action
         * failed.  Thus, we should never undo the "end" node in a workflow.
         */
        panic!("attempted to undo end node in workflow");
    }
}

/** Action implementation that simulates an error at a given stage. */
#[derive(Debug)]
struct WfActionInjectError {}

#[async_trait]
impl WfAction for WfActionInjectError {
    async fn do_it(&self, wfctx: WfContext) -> WfActionResult {
        let message = format!(
            "<boom! error injected instead of action for \
            node \"{}\">",
            wfctx.node_label()
        );
        eprintln!("{}", message);
        Err(anyhow!("{}", message))
    }

    async fn undo_it(&self, _: WfContext) -> WfUndoResult {
        unimplemented!();
    }
}

/**
 * Describes a directed acyclic graph of actions
 *
 * See [`WfBuilder`] to construct a Workflow.  See [`WfExecutor`] to execute
 * one.
 */
pub struct Workflow {
    graph: Graph<String, ()>,
    launchers: BTreeMap<NodeIndex, Arc<dyn WfAction>>,
    node_names: BTreeMap<NodeIndex, String>,
    start_node: NodeIndex,
    end_node: NodeIndex,
}

impl Debug for Workflow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let dot = petgraph::dot::Dot::new(&self.graph);
        write!(f, "workflow graph: {:?}", dot)
    }
}

impl Workflow {
    pub fn node_for_name(
        &self,
        target_name: &str,
    ) -> Result<NodeIndex, anyhow::Error> {
        for (node, name) in &self.node_names {
            if name == target_name {
                return Ok(*node);
            }
        }

        /* TODO-debug workflows should have names, too */
        Err(anyhow!("workflow has no node named \"{}\"", target_name))
    }
}

/**
 * Construct a workflow graph
 *
 * The interface here only supports linear construction using an "append"
 * operation.  See [`WfBuilder::append`] and [`WfBuilder::append_parallel`].
 */
#[derive(Debug)]
pub struct WfBuilder {
    /** DAG of workflow nodes.  Weights for nodes are debug labels. */
    graph: Graph<String, ()>,
    /** For each node, the [`WfAction`] executed at that node. */
    launchers: BTreeMap<NodeIndex, Arc<dyn WfAction>>,
    /**
     * For each node, the name of the node.  This is used for data stored by
     * that node.
     */
    node_names: BTreeMap<NodeIndex, String>,
    /** Root node of the graph */
    root: NodeIndex,
    /** Last set of nodes added.  This is used when appending to the graph. */
    last_added: Vec<NodeIndex>,
}

impl WfBuilder {
    pub fn new() -> WfBuilder {
        let mut graph = Graph::new();
        let mut launchers = BTreeMap::new();
        let node_names = BTreeMap::new();
        let first: Arc<dyn WfAction + 'static> =
            Arc::new(WfActionUniversalStart {});
        let label = format!("{:?}", first);
        let root = graph.add_node(label);
        launchers.insert(root, first).expect_none("empty map had an element");

        WfBuilder { graph, launchers, root, node_names, last_added: vec![root] }
    }

    /**
     * Adds a new node to the graph
     *
     * The new node will depend on completion of all actions that were added in
     * the last call to `append` or `append_parallel`.  (The idea is to `append`
     * a sequence of steps that run one after another.)
     *
     * `action` will be used when this node is being executed.
     *
     * The node is called `name`.  This name is used for storing the output of
     * the action so that descendant nodes can access it using
     * [`WfContext::lookup`].
     */
    pub fn append(&mut self, name: &str, action: Arc<dyn WfAction>) {
        let label = format!("{:?}", action);
        let newnode = self.graph.add_node(label);
        self.launchers
            .insert(newnode, action)
            .expect_none("action already present for newly created node");
        self.node_names
            .insert(newnode, name.to_string())
            .expect_none("name already used in this workflow");
        for node in &self.last_added {
            self.graph.add_edge(*node, newnode, ());
        }

        self.last_added = vec![newnode];
    }

    /**
     * Adds a set of nodes to the graph that can be executed concurrently
     *
     * The new nodes will individually depend on completion of all actions that
     * were added in the last call to `append` or `append_parallel`.  `actions`
     * is a vector of `(name, action)` tuples analogous to the arguments to
     * [`WfBuilder::append`].
     */
    pub fn append_parallel(&mut self, actions: Vec<(&str, Arc<dyn WfAction>)>) {
        let newnodes: Vec<NodeIndex> = actions
            .into_iter()
            .map(|(n, a)| {
                let label = format!("{:?}", a);
                let node = self.graph.add_node(label);
                self.launchers.insert(node, a).expect_none(
                    "action already present for newly created node",
                );
                self.node_names
                    .insert(node, n.to_string())
                    .expect_none("name already used in this workflow");
                node
            })
            .collect();

        /*
         * For this exploration, we assume that any nodes appended after a
         * parallel set are intended to depend on _all_ nodes in the parallel
         * set.  This doesn't have to be the case in general, but if you wanted
         * to do something else, you probably would need pretty fine-grained
         * control over the construction of the graph.  This is mostly a
         * question of how to express the construction of the graph, not the
         * graph itself nor how it gets processed, so let's defer for now.
         *
         * Even given all that, it might make more sense to implement this by
         * creating an intermediate node that all the parallel nodes have edges
         * to, and then edges from this intermediate node to the next set of
         * parallel nodes.
         */
        for node in &self.last_added {
            for newnode in &newnodes {
                self.graph.add_edge(*node, *newnode, ());
            }
        }

        self.last_added = newnodes;
    }

    /** Finishes building the Workflow */
    pub fn build(mut self) -> Workflow {
        /*
         * Append an "end" node so that we can easily tell when the workflow has
         * completed.
         */
        let last: Arc<dyn WfAction + 'static> =
            Arc::new(WfActionUniversalEnd {});
        let label = format!("{:?}", last);
        let newnode = self.graph.add_node(label);
        self.launchers.insert(newnode, last).unwrap_none();

        for node in &self.last_added {
            self.graph.add_edge(*node, newnode, ());
        }

        Workflow {
            graph: self.graph,
            launchers: self.launchers,
            node_names: self.node_names,
            start_node: self.root,
            end_node: newnode,
        }
    }
}
