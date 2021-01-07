/*
 * This file contains an in-progress prototype interface for workflows.
 *
 * Fundamentally, workflows are a directed acyclic graph of possibly reversible
 * actions, similar to distributed sagas.  There are two main functional pieces:
 * an interface for constructing the graph and an execution engine for carrying
 * out the actions described by the graph.  There are many subparts to this
 * problem, including:
 *
 * - What's the syntax for constructing the graph?  Can graphs be modified while
 *   they're being executed?  (If not, the syntax for constructing them can
 *   probably be made a lot cleaner.  But some workflows may require this, as
 *   when step N is used to stamp out a bunch of different instances of step N +
 *   1.)
 *
 *   To keep things simple for now, we're going to make the construction totally
 *   explicit.
 *
 * - How is data shared between different actions in the graph?  Ideally, this
 *   would be statically type-checked, so that you could not add a node B to the
 *   graph that uses data that wasn't provided by some ancestor of B in the
 *   graph.  It's not clear how to express this in Rust without a lot of
 *   boilerplate or macros.
 *
 * - How do execution parameters like canarying, limited concurrency, and
 *   limiting blast radius fit in?
 *
 * - Persistence: what state do we persist and how?  More on this below.
 *
 * The current status is that we have:
 *
 * - basic types: WfError, WfResult
 * - WfAction, a trait representing the actions taken for nodes in the graph
 * - WfActionFunc, which makes it easy to create WfActions from functions
 * - WfBuilder, an interface for constructing a workflow graph pretty much by
 *   hand
 * - basic execution via WfExecutor: an executor that walks the graph and
 *   executes exactly the steps that it's allowed to execute with maximum
 *   parallelism
 * - shared data between actions
 * - composeability: inserting an entire workflow into a workflow node.  A big
 *   challenge here is that the state objects will differ.  Callers have to deal
 *   with this in the node that's part of the parent graph.  (That's kind of
 *   what you want anyway: callers will already be taking parameters out of the
 *   parent state and making them part of the child state, most likely.)
 * - summary of current state a la Manta resharder
 *
 * Remaining things to de-risk:
 *
 * - persistence!  Can we get away with persisting only what nodes have started
 *   and finished?  Derisking this will involve not just persisting the state
 *   but making sure we can reconstitute the state from the persisted form and
 *   correctly execute the workflow.
 * - Reversibility: how do we allow consumers to express that some or all of
 *   these actions are reversible and then implement that such that if a
 *   Workflow step fails in a way that demands reversal of the workflow, then we
 *   carry that out?  And how do we determine whether errors within subworkflows
 *   demand that the parent workflow also be reversed?
 * - bells and whistles:
 *   - pausing
 *   - concurrency limit
 *   - canarying
 *   - blast radius?
 *   - policy around what to do on failure (stop, rewind)
 * - revisit static typing in the construction and execution of the graph?
 *   - It's not even super clear what this will look like until we implement
 *     persistence with the shared state objects we have.  Right now, this
 *     static type safety would mean making sure all the nodes in a graph have
 *     the same state object.  I'd love to also allow subsequent stages to be
 *     sure that previous stages have completed.
 *   - One piece of this might be to use macros on the action functions that
 *     also generate input and output types specific to that function?
 *     - They'd also need to generate glue code from previous and subsequent
 *       nodes, it seems.
 *   - I think we want the Graph data structure to erase the specific
 *     input/output types as we do today.  But maybe when it's still in the
 *     builder stage, we keep track of these types so that we can fail at
 *     compile time when constructing an invalid graph.
 *
 * Persistence is a big question.  Can we get away with persisting _only_ what
 * distributed sagas does: namely, which steps we've started and finished?  As a
 * simple example of additional state we might like to store, consider the IP
 * address of an Instance on its VPC.  Formally, for distributed sagas, we'd
 * want to say that `vpc_allocate_ip` is idempotent and we would simply have the
 * executor run this node again in the event of a crash.  For that to work, we'd
 * want it to find the previously-allocated IP.  If all we persisted was which
 * steps we'd run already, then in order to idempotently implement this, we'd
 * presumably need `vpc_allocate_ip` to record not just that the IP was
 * allocated, but which workflow allocated it, and then the idempotent
 * implementation would look for IPs allocated by this workflow.  That's fine, I
 * guess, but it seems weird for the VIP database records to include information
 * about the workflow system that they otherwise shouldn't really know much
 * about.  On the other hand, maybe the VIP is linked to the _instance_.  In
 * that case, we need the Instance id available to the workflow (so that we can
 * search for existing VIPs allocated for this Instance), which brings us to the
 * same problem: do Instances know what Workflow ID created them (shudder), or
 * does the Workflow have a way to persist the Instance id once it's been
 * assigned?  On the other hand, if Instances _don't_ know what workflow created
 * them, then how could one possibly implement the instance_create() workflow
 * step idempotently?  Maybe we split this into two steps:
 * instance_id_allocate() followed by instance_create()?  This way if we crash
 * we'll only ever try to recreate an Instance with the same ID (or,
 * equivalently, we can look it up first).  But this still assumes that the
 * instance_id_allocate() step is able to save its output somewhere.
 * (Alternatively, in this specific case, we could say that the instance id is
 * part of the immutable parameters to the Workflow that's persisted when we
 * create the workflow.  Does that always work?)
 *
 * What it boils down to is: it _feels_ a lot easier to allow stages to persist
 * whatever state they want; however, this also feels very easy to implement
 * incorrectly.  Imagine you haven't considered the above problem and you do the
 * obvious thing of having instance_create() allocate an id, construct a new
 * record, save it to the database, and then record persistent workflow state
 * consisting solely of the instance id.  If you fail after the database
 * operation, you'll run this again, create a new id and Instance, and leak the
 * Instance row in the database.
 *
 * Maybe another idea is that you can save persistent state only in synchronous
 * actions.  This would prevent you from implementing the above -- you'd _have_
 * to separate it into a synchronous step (allocating the id) that saves
 * persistent state and an asynchronous state (that uses the id to create the
 * instance).
 *
 * Another thing to consider: do we expect an Instance to be operated on
 * multiple workflows in its lifetime?  (Probably -- create, destroy, maybe even
 * boot and halt.)  What about concurrently?  (It seems likely.)  Do we want
 * every row in the Instance table to have a column for every possible workflow?
 * That kind of sucks.  (On the other hand, it might help us avoid issuing
 * multiple concurrent reboot workflows or something like that.)
 *
 * Playground links:
 * Simplified version of problem with WfAction::do_it() taking a reference:
 * - https://play.rust-lang.org/?version=stable&mode=debug&edition=2018&gist=09c93f6d2978eb09bed336e9f380d543
 *
 * Note on distributed saga implementation (see
 * https://www.youtube.com/watch?v=0UTOLRTwOX0, around T=23m):
 * - distributed saga log -- only thing that's persisted
 *   - "start saga" message includes initial parameter
 *   - "start saga" node marked complete
 *   - "start <node>" written to log and acked before executing request
 *   - "end <node>" written to log _with result of request_ logged and acked
 *   - write "end saga" message
 * - failure @ ~27m: this is clear failure of a request, which triggers a
 *   rollback.  write "abort <node>" message, etc.
 *   - when rolling back node: check log for entries for a node.  If none,
 *     do nothing and move on.  If there's an abort message, do nothing and move
 *     on.  If completed successfully, log "start cancel <node>", issue
 *     compensating request, log "end cancel <node>".  If there's a "start" and
 *     that's all, then _send it again_, get a response (and log it), _then_
 *     compensate it (logging that).  (I'm not sure why you can't _just_ cancel
 *     it.  Asked at 35m45s or so.) OOHH! This guarantees that there's always
 *     _something_ to cancel -- you never have to cancel something you've never
 *     heard of.  (That means commutativity isn't quite the right name for the
 *     property these compensating requests have to have.  What you need is that
 *     if you've issued the compensating one and subsequently the original
 *     request is replayed, the result should be that the compensating action
 *     has happened.)
 *
 * Open items from distributed saga log:
 * - how do you ensure that two executors aren't working concurrently on the
 *   same saga? (as described, there's a race in checking log and taking action,
 *   I think).  Maybe we can deal with this by always updating a workflow
 *   "generation" record every time we add an entry to the log?
 * - At 30m30s or so, she sort of talks about SEC failure or distribution.
 *   Sounds like she thinks it all "just works"?
 * - how do you identify when the coordinator fails and resume elsewhere?
 *   (discussed around 40m and several more questions about it, but basically
 *   punted)
 * - how do any nodes in the graph access output from previous nodes?  from log
 *   messages?  does that mean we reconstruct this shared state from the log?
 *
 * NOTE: this has implication for the shared in-memory state between nodes.  It
 * needs to be reconstituted from the log.
 *
 * [2020-12-15]
 * Regarding persistence: the closest analog to what's described in the
 * distributed sagas talk is that each workflow action produces a structure that
 * can be persisted in the saga log.  That's easy enough (if a bit of a pain):
 * WfResult's output type can be a "Box<dyn Serialize>" or the like.  This
 * approach has the problem mentioned above, which is that it's very easy to
 * write an action that first creates a random uuid and then creates it in the
 * database.  This is not idempotent because if we crash after that but before
 * we persist the result, then we'll end up running the action again and
 * creating a second record (and likely leaking the first).  However, this
 * approach can at least work and let us make forward progress on other parts of
 * the system.
 *
 * There's a second problem here, which is: how is that state made available to
 * the compensation action (which seems relatively easy) or other actions in the
 * workflow DAG (which seems tricky)?  If it were all dynamically-typed, we
 * could use key-value pairs with `Arc<dyn Any>` for the value...but how awful
 * is that.  Can we really not do better?
 *
 * The current prototype uses a single shared state struct for the whole
 * workflow.  This is easy enough to express, but then persistence becomes
 * a little tricky.  I guess we could save a new copy to the log any time an
 * action completes.  Of course, the act of saving it probably requires taking a
 * Mutex in practice, and how do we do that in the context of serialize?  I
 * suppose we could require that the struct provide a serialize-like entry point
 * that takes the lock and serializes itself.  It's starting to feel nasty.
 *
 * If we instead say that workflow actions can only persist data by returning
 * it, and that they can only access previously-persisted data using a special
 * interface, then we can take care of concurrency.  The request to fetch data
 * would be asynchronous and infallible by default (panicking if the data is
 * missing or has the wrong type, possibly with a try_() version if we want
 * something to be optional).  By being async, that gives the implementation
 * flexibility to use a lock or a channel or something else.
 *
 * But how does the caller specify what they're asking for?  This is the most
 * obvious:
 *
 *     let ip: IpAddr = wfctx.lookup("instance_ip").await;
 *
 * but then how do we know what data that corresponds to?  One option is the
 * previous stage had to do:
 *
 *     wfctx.store("instance_ip", ip_addr);
 *
 * and that would be infallible.
 *
 * This isn't as ergonomic as simply being able to return the item from the
 * Workflow action.  And it's easy to get this wrong (e.g., typo in the property
 * name).  That could be an enum...but the enum's type would be different for
 * different workflows, which means you need adapters of some kind for composing
 * workflows.  We already have that, so maybe that's not so bad.
 *
 * One nice thing about this is that you can define crisp interfaces between
 * actions.  You can say:
 *
 *     struct VipAllocation {
 *         vpc_id: Uuid,
 *         ip: IpAddr,
 *         subnet: Subnet,
 *     }
 *
 *     ...
 *
 *     wfctx.store("instance_vip", vip_allocation);
 *
 *     ...
 *
 *     let ip: IpAddr = wfctx.lookup::<VipAllocation>("instance_vip").await.ip;
 *
 * This way, the interface between actions can be a stable interface, not just
 * whatever happened to be convenient.
 *
 * If we're going this far, what if we get rid of the key altogether and make
 * you use an interface like this to access the shared state?  So it looks like
 * this:
 *
 *     //
 *     // I'm making this fallible now so that we can abort the workflow at this
 *     // point if we want to.  We could still panic on type errors.
 *     //
 *     let my_state: ProvisionVmWorkflowState = wfctx.state().await?;
 *     // Manipulate my_state as desired.  One implementation is that you've got
 *     // a lock held at this point.  (What happens if you drop the lock before
 *     // completing the workflow action?  We don't want to persist that
 *     // intermediate state -- then we don't have any of the guarantees of
 *     // distributed sagas.  But if you've changed it, and other stages can see
 *     // that, that's bad!  Speaking of this: we have an opportunity here to
 *     // absolutely prevent workflow actions from seeing data they shouldn't,
 *     // even if it's technically available, because we have the ability to
 *     // know which DAG node produced it and so whether it should be visible to
 *     // another node.  This might be too ambitious, but it seems like a really
 *     // valuable way to catch an important class of programmer error!
 *
 * This last point sends me back towards having each action return just a small
 * chunk of state, not make arbitrary modifications to the whole workflow state.
 * But the question remains: how is that state made available to subsequent
 * actions?  At the end of the day, the subsequent action has got to refer to
 * that by some sort of name.  That can either be statically resolved (e.g., a
 * structure field) or dynamically (e.g., lookup in a tree).  The static option
 * seems hard, at least right now -- it means every action needs its own input
 * type that presumably we'd want to autogenerate based on the fields it uses
 * or macro arguments or something.  If we go the dynamic route, how do we name
 * the "key"?  One idea is the one mentioned above -- the generating action
 * stores it with an explicit key name.  Another idea is that it's a property of
 * the action itself -- i.e., when you create the node (or add it to the graph),
 * you give it a name, and its return value becomes the value that a subsequent
 * node can lookup based on the node's name.  One downside to this is that the
 * structure of actions in the graph determines how this data is accessed --
 * i.e., if you split one action into two, then consumers need to be updated to
 * reflect that (unless you add a new node that combines the data from the other
 * two nodes).
 *
 * Maybe this is the way to go for now.
 *
 * Persistent state plan:
 *
 * - the only access to previous actions' state is through an explicit function
 *   call that uses the name of the previous state as the key
 * - the only way to store state is by the return value of the action, which
 *   will be dynamically typed (Arc<dyn Any>).  This will be stored keyed by the
 *   name of the node.  (So nodes have names now.)
 *
 * TODO lots of ergonomic improvements could be made in the updated API here:
 * - should be easier to define functions that _don't_ record any persistent
 *   state (to avoid having to assign a name and to avoid the boilerplate of
 *   Ok(Arc::new(()))
 * - starting to see how a macro could allow you to say field X has type Y and
 *   insert into the beginning of your function the appropriate calls to lookup(),
 *   though that still wouldn't be statically type-checked.
 * - probably want to allow workflow execution to consume parameters
 *
 * Next steps here:
 *
 * - Add WfExecutor::recover(), which is somehow given a list of WfEvents and
 *   uses recover_workflow_log() to compute the load state of each node, then
 *   uses this to restore the in-memory execution state.
 *   - XXX working here: I've written some recovery code but it's a little hard
 *     to test.  I did some refactoring, lots of cleanup, and other foundation
 *     for testing this: like print_status().
 *     The next step is to figure out how to actually test recovery.  One idea
 *     is to add pause_at() so that I can pause execution at some specific
 *     point, create and use snapshot() to get the log at that point, then test
 *     recovery from that log.
 *     This isn't bad, but it will never exercise a case where the log contains
 *     a start record for an action but no "done" record.
 *     We might want another mode where we can just extract partial leading
 *     subsequences of a WfLog.
 * - There's also a ton of cleanup to do here.
 * - Add a few demos to convince myself this all works reasonably correctly.
 * - Implement unwinding.
 */

#![feature(type_name_of_val)]
#![feature(option_expect_none)]
#![feature(option_unwrap_none)]
#![deny(elided_lifetimes_in_paths)]

mod wf_exec;
mod wf_log;

use async_trait::async_trait;
use core::any::Any;
use core::fmt;
use core::fmt::Debug;
use core::future::Future;
use core::marker::PhantomData;
use petgraph::graph::NodeIndex;
use petgraph::Graph;
use std::collections::BTreeMap;
use std::sync::Arc;
use uuid::Uuid;

pub use wf_exec::WfContext;
pub use wf_exec::WfExecutor;
// TODO Probably don't need to be public, actually
pub use wf_log::recover_workflow_log;
pub use wf_log::WfLog;
pub use wf_log::WfLogResult;

/* Widely-used types (within workflows) */

/** Unique identifier for a Workflow */
// TODO Should this have "w-" prefix like other clouds use?
pub type WfId = Uuid;
/** Error produced by a workflow action or a workflow itself */
pub type WfError = anyhow::Error;
/** Output produced on success by a workflow action or the workflow itself */
pub type WfOutput = Arc<dyn Any + Send + Sync + 'static>;
/** Result of a workflow action or the workflow itself */
// pub type WfResult = Arc<Result<WfOutput, WfError>>;
pub type WfResult = Result<WfOutput, WfError>;
/** Result of a function implementing a workflow action */
pub type WfFuncResult = Result<WfOutput, WfError>;
/** Result of a workflow cancel action. */
pub type WfCancelResult = Result<(), WfError>;

/**
 * Building blocks of workflows
 *
 * Each action consumes a [`WfContext`] and asynchronously produces a
 * [`WfResult`].  A workflow is essentially a directed acyclic graph of actions
 * with dependencies between them.
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
     * On success, this function produces a `WfOutput`, which is just `Arc<dyn
     * Any + ...>`.  This output will be stored persistently, keyed by the
     * _name_ of the current workflow node.  Subsequent stages can access this
     * data with [`WfContext::lookup`].  This is the _only_ supported means of
     * sharing state across actions within a workflow.
     */
    async fn do_it(&self, wfctx: WfContext) -> WfResult;
}

/**
 * [`WfAction`] implementation for functions
 */
pub struct WfActionFunc<FutType, FuncType>
where
    FuncType: Fn(WfContext) -> FutType + Send + Sync + 'static,
    FutType: Future<Output = WfFuncResult> + Send + Sync + 'static,
{
    func: FuncType,
    phantom: PhantomData<FutType>,
}

impl<FutType, FuncType> WfActionFunc<FutType, FuncType>
where
    FuncType: Fn(WfContext) -> FutType + Send + Sync + 'static,
    FutType: Future<Output = WfFuncResult> + Send + Sync + 'static,
{
    /**
     * Wrap a function in a `WfActionFunc`
     *
     * We return the result as a `Arc<dyn WfAction>` so that it can be used
     * directly where `WfAction`s are expected.  The struct `WfActionFunc` has
     * no interfaces of its own so there's generally no need to have the
     * specific type.
     */
    pub fn new_action(f: FuncType) -> Arc<dyn WfAction> {
        Arc::new(WfActionFunc { func: f, phantom: PhantomData })
    }
}

#[async_trait]
impl<FutType, FuncType> WfAction for WfActionFunc<FutType, FuncType>
where
    FuncType: Fn(WfContext) -> FutType + Send + Sync + 'static,
    FutType: Future<Output = WfFuncResult> + Send + Sync + 'static,
{
    async fn do_it(&self, wfctx: WfContext) -> WfResult {
        let fut = { (self.func)(wfctx) };
        fut.await
    }
}

impl<FutType, FuncType> Debug for WfActionFunc<FutType, FuncType>
where
    FuncType: Fn(WfContext) -> FutType + Send + Sync + 'static,
    FutType: Future<Output = WfFuncResult> + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&std::any::type_name_of_val(&self.func))
    }
}

/** Placeholder type for the start node in a graph. */
#[derive(Debug)]
struct WfActionUniversalStart {}

#[async_trait]
impl WfAction for WfActionUniversalStart {
    async fn do_it(&self, _: WfContext) -> WfResult {
        eprintln!("universal start action");
        Ok(Arc::new(()))
    }
}

/** Placeholder type for the end node in a graph. */
#[derive(Debug)]
struct WfActionUniversalEnd {}

#[async_trait]
impl WfAction for WfActionUniversalEnd {
    async fn do_it(&self, _: WfContext) -> WfResult {
        eprintln!("universal end action");
        Ok(Arc::new(()))
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
    // TODO Maybe the WfExec should just have a reference to the workflow rather
    // than needing Arc<dyn WfAction> and copying these other fields.
    launchers: BTreeMap<NodeIndex, Arc<dyn WfAction>>,
    node_names: BTreeMap<NodeIndex, String>,
    start: NodeIndex,
    end: NodeIndex,
}

impl Debug for Workflow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let dot = petgraph::dot::Dot::new(&self.graph);
        f.debug_struct("Workflow")
            .field("graph", &self.graph)
            .field("dot", &dot)
            .finish()
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
    /*
     * TODO it's not clear if it's important that this step exist.  Maybe some
     * workflows could append to themselves while they're running, and that
     * might be okay?
     */
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
            start: self.root,
            end: newnode,
        }
    }
}
