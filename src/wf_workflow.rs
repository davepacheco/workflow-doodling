//! Facilities for constructing workflow graphs

use crate::wf_action::WfAction;
use crate::wf_action::WfActionEndNode;
use crate::wf_action::WfActionStartNode;
use anyhow::anyhow;
use petgraph::dot;
use petgraph::graph::NodeIndex;
use petgraph::Graph;
use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;
use uuid::Uuid;

/** Unique identifier for a Workflow */
/*
 * TODO-cleanup make this a "newtype".  We may want the display form to have a
 * "w-" prefix (or something like that).  (Does that mean the type needs to be
 * caller-provided?)
 */
pub type WfId = Uuid;

/**
 * Workflows help organize execution of a set of asynchronous tasks that can
 * fail
 *
 * Each workflow is a directed acyclic graph (DAG) where each node implements
 * [`WfAction`].  With each node, there's typically an execution action and an
 * undo action.  Execution guarantees that eventually all workflow nodes will
 * complete successfully or else that any nodes whose actions may have run have
 * also had their undo action run.  This abstraction is based on the distributed
 * saga pattern.
 *
 * You define a workflow using [`WfBuilder`].  You can execute a workflow as
 * many times as you want using [`WfExecutor`].
 */
#[derive(Debug)]
pub struct Workflow {
    /** describes the nodes in the graph and their dependencies */
    pub(crate) graph: Graph<String, ()>,
    /** action associated with each node in the graph */
    pub(crate) launchers: BTreeMap<NodeIndex, Arc<dyn WfAction>>,
    /** name associated with each node in the graph */
    pub(crate) node_names: BTreeMap<NodeIndex, String>,
    /** human-readable labels associated with each node in the graph */
    pub(crate) node_labels: BTreeMap<NodeIndex, String>,
    /** start node */
    pub(crate) start_node: NodeIndex,
    /** end node */
    pub(crate) end_node: NodeIndex,
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

    /*
     * TODO-cleanup It would be more idiomatic to return a Dot struct that impls
     * Display to do this.
     */
    pub fn print_dot(&self, out: &mut dyn io::Write) -> io::Result<()> {
        let dot =
            dot::Dot::with_config(&self.graph, &[dot::Config::EdgeNoLabel]);
        write!(out, "{:?}", dot)
    }
}

/**
 * Builder for constructing a Workflow
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
    /** For each node, a human-readable label for the node. */
    node_labels: BTreeMap<NodeIndex, String>,
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
        let node_labels = BTreeMap::new();
        let first: Arc<dyn WfAction + 'static> = Arc::new(WfActionStartNode {});
        let label = format!("{:?}", first);
        let root = graph.add_node(label);
        launchers.insert(root, first).expect_none("empty map had an element");

        WfBuilder {
            graph,
            launchers,
            root,
            node_names,
            node_labels,
            last_added: vec![root],
        }
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
    pub fn append(
        &mut self,
        name: &str,
        label: &str,
        action: Arc<dyn WfAction>,
    ) {
        let newnode = self.graph.add_node(label.to_string());
        self.launchers
            .insert(newnode, action)
            .expect_none("action already present for newly created node");
        /* TODO-correctness this doesn't check name uniqueness! */
        self.node_names
            .insert(newnode, name.to_string())
            .expect_none("name already used in this workflow");
        self.node_labels
            .insert(newnode, label.to_string())
            .expect_none("labels already used in this workflow");
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
    pub fn append_parallel(
        &mut self,
        actions: Vec<(&str, &str, Arc<dyn WfAction>)>,
    ) {
        let newnodes: Vec<NodeIndex> = actions
            .into_iter()
            .map(|(n, l, a)| {
                let node = self.graph.add_node(l.to_string());
                self.launchers.insert(node, a).expect_none(
                    "action already present for newly created node",
                );
                /* TODO-correctness does not validate the name! */
                self.node_names
                    .insert(node, n.to_string())
                    .expect_none("name already used in this workflow");
                self.node_labels
                    .insert(node, l.to_string())
                    .expect_none("node already has a label");
                node
            })
            .collect();

        /*
         * TODO-design For this exploration, we assume that any nodes appended
         * after a parallel set are intended to depend on _all_ nodes in the
         * parallel set.  This doesn't have to be the case in general, but if
         * you wanted to do something else, you probably would need pretty
         * fine-grained control over the construction of the graph.  This is
         * mostly a question of how to express the construction of the graph,
         * not the graph itself nor how it gets processed, so let's defer for
         * now.
         *
         * Even given that, it might make more sense to implement this by
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
        let last: Arc<dyn WfAction + 'static> = Arc::new(WfActionEndNode {});
        let label = format!("{:?}", last);
        let newnode = self.graph.add_node(label);
        /*
         * It seems sketchy to have assertions with side effects.  We'd prefer
         * `unwrap_none()`, but that's still experimental.
         */
        assert!(self.launchers.insert(newnode, last).is_none());

        for node in &self.last_added {
            self.graph.add_edge(*node, newnode, ());
        }

        Workflow {
            graph: self.graph,
            launchers: self.launchers,
            node_names: self.node_names,
            node_labels: self.node_labels,
            start_node: self.root,
            end_node: newnode,
        }
    }
}
