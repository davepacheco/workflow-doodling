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
mod wf_action;
mod wf_exec;
mod wf_log;

use anyhow::anyhow;
use core::fmt;
use core::fmt::Debug;
use petgraph::graph::NodeIndex;
use petgraph::Graph;
use std::collections::BTreeMap;
use std::sync::Arc;
use uuid::Uuid;

pub use example_provision::make_provision_workflow;
pub use wf_action::new_action_noop_undo;
pub use wf_action::WfActionFunc;
pub use wf_action::WfActionResult;
pub use wf_action::WfError;
pub use wf_action::WfFuncResult;
pub use wf_action::WfUndoResult;
pub use wf_exec::WfContext;
pub use wf_exec::WfExecutor;
pub use wf_log::WfLog;

use wf_action::WfAction;
use wf_action::WfActionUniversalEnd;
use wf_action::WfActionUniversalStart;

/* Widely-used types (within workflows) */

/** Unique identifier for a Workflow */
/*
 * TODO-cleanup make this a "newtype".  We may want the display form to have a
 * "w-" prefix (or something like that).  (Does that mean the type needs to be
 * caller-provided?)
 */
pub type WfId = Uuid;

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
