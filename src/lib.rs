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
 *   We're deferring this for now.  _Right_ now, there's no built-in way to
 *   share any data at all.  The next step will be to add some generic state
 *   parameter provided to every function.
 *
 *   In terms of modifying this state, we'll need to consider whether we want to
 *   use Mutexes to protect them or provide a single execution thread/task with
 *   messages sent on a channel to update state.
 *
 * - How do execution parameters like canarying, limited concurrency, and
 *   limiting blast radius fit in?
 *
 * - Persistence: what state do we persist and how?  More on this below
 *
 * The current status is that we have:
 *
 * - basic types: WfError, WfResult
 * - WfAction, which has a blanket impl for functions to minimize boilerplate
 * - WfBuilder, an interface for constructing a workflow graph pretty much by
 *   hand
 * - basic execution: build an executor that walks the graph and executes
 *   exactly the steps that it's allowed to execute with maximum parallelism
 * - shared data: flesh out the demo implementation by having the functions
 *   actually share data
 * - composeability: inserting an entire workflow into a workflow node.  A big
 *   challenge here is that the state objects will differ.  Callers have to deal
 *   with this in the node that's part of the parent graph.  (That's kind of
 *   what you want anyway: callers will already be taking parameters out of the
 *   parent state and making them part of the child state, most likely.)
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
 *   - concurrency limit
 *   - canarying
 *   - blast radius?
 *   - pausing
 *   - summary of current state a la Manta resharder
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
 * - failure @ ~27m: this is clear failure of a request that triggers a
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
 * But next steps: incorporate a more real notion of persistence
 */

#![feature(type_name_of_val)]
#![feature(option_expect_none)]
#![deny(elided_lifetimes_in_paths)]

use anyhow::anyhow;
use core::any::Any;
use core::fmt;
use core::fmt::Debug;
use core::future::Future;
use core::marker::PhantomData;
use core::pin::Pin;
use core::task::Context;
use core::task::Poll;
use futures::future::BoxFuture;
use futures::FutureExt;
use petgraph::graph::NodeIndex;
use petgraph::Graph;
use petgraph::Incoming;
use petgraph::Outgoing;
use std::collections::BTreeMap;
use std::sync::Arc;

#[macro_use]
extern crate async_trait;

/* Widely-used types (within workflows) */
pub type WfError = anyhow::Error;
pub type WfOutput = Arc<dyn Any + Send + Sync + 'static>;
pub type WfResult = Result<WfOutput, WfError>;

pub struct WfContext {
    node: NodeIndex,
    ancestor_tree: BTreeMap<String, WfOutput>,
}

impl WfContext {
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

    pub async fn child_workflow(&self, wf: Workflow) -> WfResult {
        /*
         * TODO Really we want this to reach into the parent WfExecutor and make
         * a record about this new execution.  This is mostly for observability
         * and control: if someone asks for the status of the parent workflow,
         * we'd like to give details on this child workflow.  Similarly if they
         * pause the parent, that should pause the child one.  How can we do
         * this?
         */
        let e = WfExecutor::new(wf);
        e.await
    }
}

#[async_trait]
pub trait WfAction: Debug + Send {
    async fn do_it(self: Box<Self>, wfctx: Arc<WfContext>) -> WfResult;
}

pub struct WfActionFunc<FutType, FuncType>
where
    FuncType: Fn(Arc<WfContext>) -> FutType + Send + 'static,
    FutType: Future<Output = WfResult> + Send + 'static,
{
    func: FuncType,
    phantom: PhantomData<FutType>,
}

impl<FutType, FuncType> WfActionFunc<FutType, FuncType>
where
    FuncType: Fn(Arc<WfContext>) -> FutType + Send + 'static,
    FutType: Future<Output = WfResult> + Send + 'static,
{
    pub fn new_action(f: FuncType) -> Box<dyn WfAction> {
        Box::new(WfActionFunc {
            func: f,
            phantom: PhantomData,
        })
    }
}

impl<FutType, FuncType> Debug for WfActionFunc<FutType, FuncType>
where
    FuncType: Fn(Arc<WfContext>) -> FutType + Send + 'static,
    FutType: Future<Output = WfResult> + Send + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WfActionFunc")
            .field("func", &std::any::type_name_of_val(&self.func))
            .finish()
    }
}

#[async_trait]
impl<FutType, FuncType> WfAction for WfActionFunc<FutType, FuncType>
where
    FuncType: Fn(Arc<WfContext>) -> FutType + Send + 'static,
    FutType: Future<Output = WfResult> + Send + 'static,
{
    async fn do_it(self: Box<Self>, wfctx: Arc<WfContext>) -> WfResult {
        let fut = { (self.func)(wfctx) };
        fut.await
    }
}

/*
 * WfBuilder is an interface for constructing a workflow graph.  See `append()`
 * and `append_parallel()` for more.
 */
#[derive(Debug)]
pub struct WfBuilder {
    /* DAG of workflow nodes. */
    graph: Graph<String, ()>,
    launchers: BTreeMap<NodeIndex, Box<dyn WfAction>>,
    node_names: BTreeMap<NodeIndex, String>,
    root: NodeIndex,
    last_added: Vec<NodeIndex>,
}

#[derive(Debug)]
struct WfActionUniversalFirst {}

#[async_trait]
impl WfAction for WfActionUniversalFirst {
    async fn do_it(self: Box<Self>, _: Arc<WfContext>) -> WfResult {
        eprintln!("universal first action");
        Ok(Arc::new(()))
    }
}

impl WfBuilder {
    pub fn new() -> WfBuilder {
        let mut graph = Graph::new();
        let mut launchers = BTreeMap::new();
        let node_names = BTreeMap::new();
        let first: Box<dyn WfAction + 'static> =
            Box::new(WfActionUniversalFirst {});
        let label = format!("{:?}", first);
        let root = graph.add_node(label);
        launchers.insert(root, first).expect_none("empty map had an element");

        WfBuilder {
            graph,
            launchers,
            root,
            node_names,
            last_added: vec![root],
        }
    }

    /*
     * Creates a new "phase" of the workflow consisting of a single action.
     * This action will depend on completion of all actions in the previous
     * phase.
     */
    pub fn append(&mut self, name: &str, action: Box<dyn WfAction>) {
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

    /*
     * Creates a new "phase" of the workflow consisting of a set of actions that
     * may run concurrently.  These actions will individually depend on all
     * actions in the previous phase having been completed.
     */
    pub fn append_parallel(&mut self, actions: Vec<(&str, Box<dyn WfAction>)>) {
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

    pub fn build(self) -> Workflow {
        Workflow {
            graph: self.graph,
            launchers: self.launchers,
            root: self.root,
            node_names: self.node_names,
        }
    }
}

pub struct Workflow {
    graph: Graph<String, ()>,
    launchers: BTreeMap<NodeIndex, Box<dyn WfAction>>,
    node_names: BTreeMap<NodeIndex, String>,
    root: NodeIndex,
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

/*
 * Executes a workflow.
 */
pub struct WfExecutor {
    graph: Graph<String, ()>,
    launchers: BTreeMap<NodeIndex, Box<dyn WfAction>>,
    node_names: BTreeMap<NodeIndex, String>,
    root: NodeIndex,
    running: BTreeMap<NodeIndex, BoxFuture<'static, WfResult>>,
    finished: BTreeMap<NodeIndex, WfOutput>,
    ready: Vec<NodeIndex>,
    //
    // TODO This is really janky.  It's here just to have a way to get the
    // output of the workflow as a whole based on the output of the last node
    // completed.
    //
    last_finished: NodeIndex,

    // TODO probably better as a state enum
    error: Option<WfError>,
}

impl WfExecutor {
    pub fn new(w: Workflow) -> WfExecutor {
        WfExecutor {
            graph: w.graph,
            launchers: w.launchers,
            node_names: w.node_names,
            root: w.root,
            last_finished: w.root,
            running: BTreeMap::new(),
            finished: BTreeMap::new(),
            ready: vec![w.root],
            error: None,
        }
    }

    /*
     * The "ancestor tree" for a node is a Map whose keys are strings that
     * identify ancestor nodes in the graph and whose values represent the
     * outputs from those nodes.  See where we call this function for more
     * details.
     */
    fn make_ancestor_tree(
        &self,
        tree: &mut BTreeMap<String, WfOutput>,
        node: NodeIndex,
    ) {
        let ancestors = self.graph.neighbors_directed(node, Incoming);
        for ancestor in ancestors {
            self.make_ancestor_tree_node(tree, ancestor);
        }
    }

    fn make_ancestor_tree_node(
        &self,
        tree: &mut BTreeMap<String, WfOutput>,
        node: NodeIndex,
    ) {
        if node == self.root {
            return;
        }

        let name = self.node_names[&node].to_string();
        let output = self
            .finished
            .get(&node)
            .expect("cannot make ancestor tree on unfinished node");
        tree.insert(name, Arc::clone(output));
        self.make_ancestor_tree(tree, node);
    }
}

impl Future for WfExecutor {
    type Output = WfResult;

    fn poll<'a>(
        mut self: Pin<&'a mut WfExecutor>,
        cx: &'a mut Context<'_>,
    ) -> Poll<WfResult> {
        let mut recheck = false;

        assert!(self.finished.len() <= self.graph.node_count());

        if let Some(_) = &self.error {
            // TODO We'd like to emit the error that we saved here but we still
            // hold a reference to it.  Maybe use take()?  But that leaves the
            // internal state rather confused.  Maybe there should be a separate
            // boolean for whether an error has been recorded.
            return Poll::Ready(Err(anyhow!("workflow failed")));
        }

        if self.finished.len() == self.graph.node_count() {
            // TODO Besides being janky, this will probably blow up for the case
            // of an empty workflow.
            return Poll::Ready(Ok(Arc::clone(
                &self.finished[&self.last_finished],
            )));
        }

        /*
         * If there's nothing running and nothing ready to run and we're still
         * not finished and haven't encountered an error, something has gone
         * seriously wrong.
         */
        if self.running.is_empty() && self.ready.is_empty() {
            panic!("workflow came to rest without having finished");
        }

        /*
         * If any of the tasks we currently think are running have now finished,
         * walk their dependents and potentially mark them ready to run.
         * TODO Is polling on _everything_ again really the right way to do
         * this?  I'm basically following what futures::join! does.
         */
        let newly_finished = {
            let mut newly_finished = Vec::new();

            for (node, fut) in &mut self.running {
                if let Poll::Ready(result) = fut.poll_unpin(cx) {
                    recheck = true;
                    newly_finished.push((*node, result));
                }
            }

            newly_finished
        };

        for (node, result) in newly_finished {
            self.running.remove(&node);

            if let Err(error) = result {
                /*
                 * We currently assume errors are fatal.  That's not
                 * necessarily right.
                 * TODO how do we end right now?
                 * TODO we'll need to clean up too!
                 * TODO want to record which node generated this error.
                 */
                self.error = Some(error)
            } else {
                let output = result.unwrap();
                self.finished
                    .insert(node, output)
                    .expect_none("node finished twice");
                let mut newly_ready = Vec::new();

                for depnode in self.graph.neighbors_directed(node, Outgoing) {
                    /*
                     * Check whether all of this node's incoming edges are
                     * now satisfied.
                     */
                    let mut okay = true;
                    for upstream in
                        self.graph.neighbors_directed(depnode, Incoming)
                    {
                        if !self.finished.contains_key(&upstream) {
                            okay = false;
                            break;
                        }
                    }

                    if okay {
                        newly_ready.push(depnode);
                    }
                }

                // TODO It'd be nice to do this inline above, but we cannot
                // borrow self.graph in order to iterate over
                // neighbors_directed() while also borrowing self.ready mutably.
                for depnode in newly_ready {
                    self.ready.push(depnode);
                }

                self.last_finished = node;
            }
        }

        if self.error.is_none() {
            let to_schedule = self.ready.drain(..).collect::<Vec<NodeIndex>>();
            for node in to_schedule {
                let wfaction = self
                    .launchers
                    .remove(&node)
                    .expect("missing action for node");
                // TODO we could be much more efficient without copying this
                // tree each time.
                let mut ancestor_tree = BTreeMap::new();
                self.make_ancestor_tree(&mut ancestor_tree, node);

                // TODO can probably drop the outer Arc
                let fut = wfaction.do_it(Arc::new(WfContext {
                    node,
                    ancestor_tree,
                }));
                let boxed = fut.boxed();
                self.running.insert(node, boxed);
                recheck = true;
            }
        }

        /*
         * If we completed any outstanding work, we need to re-check the end
         * conditions.  If we dispatched any new work, we need to poll on those
         * futures.  We could do either of those right here, but we'd have to
         * duplicate code above.  It's easier to just invoke ourselves again
         * with a tail call.  Of course, we don't want to do this if nothing
         * changed, or we'll recurse indefinitely!
         * TODO This isn't ideal, since we'll wind up polling again on anything
         * that was already running.
         */
        if recheck {
            self.poll(cx)
        } else {
            Poll::Pending
        }
    }
}
