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
 * - revisit static typing in the construction and execution of the graph?  We
 *   could say that each node has its own input type and maybe use a macro to
 *   make that ergonomic (so that people don't have to define a new type for
 *   every stage).  But what about the way they save state?  Or maybe
 *   equivalently: how do we transform the output type from one phase into the
 *   input type for the next?
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
 */

#![feature(type_name_of_val)]
#![feature(option_expect_none)]
#![deny(elided_lifetimes_in_paths)]

use core::any::Any;
use core::fmt;
use core::fmt::Debug;
use core::future::Future;
use core::marker::PhantomData;
use core::pin::Pin;
use core::task::Context;
use core::task::Poll;
use futures::future::BoxFuture;
use futures::lock::Mutex;
use futures::FutureExt;
use petgraph::graph::NodeIndex;
use petgraph::Graph;
use petgraph::Incoming;
use petgraph::Outgoing;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::sync::Arc;

use anyhow::anyhow;

#[macro_use]
extern crate async_trait;

/* Widely-used types (within workflows) */
pub type WfError = anyhow::Error;
pub type WfResult = Result<(), WfError>;

pub struct WfContext {
    internal_state: Arc<dyn Any + Send + Sync + 'static>,
    node: NodeIndex,
}

impl WfContext {
    async fn child_workflow<T>(&self, wf: Workflow, st: Arc<T>) -> WfResult
    where
        T: Send + Sync + 'static,
    {
        /*
         * TODO Really we want this to reach into the parent WfExecutor and make
         * a record about this new execution.  This is mostly for observability
         * and control: if someone asks for the status of the parent workflow,
         * we'd like to give details on this child workflow.  Similarly if they
         * pause the parent, that should pause the child one.  How can we do
         * this?
         */
        let generic_state = st as Arc<dyn Any + Send + Sync>;
        let e = WfExecutor::new(wf, generic_state);
        e.await
    }
}

#[async_trait]
trait WfAction: Debug + Send {
    async fn do_it(self: Box<Self>, wfctx: Arc<WfContext>) -> WfResult;
}

struct WfActionFunc<StateType, FutType, FuncType>
where
    StateType: Send + Sync + 'static,
    FuncType: Fn(Arc<WfContext>, Arc<StateType>) -> FutType + Send + 'static,
    FutType: Future<Output = WfResult> + Send + 'static,
{
    func: FuncType,
    phantom: PhantomData<(StateType, FutType)>,
}

impl<StateType, FutType, FuncType> WfActionFunc<StateType, FutType, FuncType>
where
    StateType: Send + Sync + 'static,
    FuncType: Fn(Arc<WfContext>, Arc<StateType>) -> FutType + Send + 'static,
    FutType: Future<Output = WfResult> + Send + 'static,
{
    fn new_action(f: FuncType) -> Box<dyn WfAction> {
        Box::new(WfActionFunc {
            func: f,
            phantom: PhantomData,
        })
    }
}

impl<StateType, FutType, FuncType> Debug
    for WfActionFunc<StateType, FutType, FuncType>
where
    StateType: Send + Sync + 'static,
    FuncType: Fn(Arc<WfContext>, Arc<StateType>) -> FutType + Send + 'static,
    FutType: Future<Output = WfResult> + Send + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WfActionFunc")
            .field("func", &std::any::type_name_of_val(&self.func))
            .finish()
    }
}

#[async_trait]
impl<StateType, FutType, FuncType> WfAction
    for WfActionFunc<StateType, FutType, FuncType>
where
    StateType: Send + Sync + 'static,
    FuncType: Fn(Arc<WfContext>, Arc<StateType>) -> FutType + Send + 'static,
    FutType: Future<Output = WfResult> + Send + 'static,
{
    async fn do_it(self: Box<Self>, wfctx: Arc<WfContext>) -> WfResult {
        //
        // TODO what compile-time check prevents the following runtime check
        // from being violated?  (Currently: none!  This should probably be part
        // of the builder interface.)
        //
        let generic_state = Arc::clone(&wfctx.internal_state);
        let specific_state = generic_state
            .downcast::<StateType>()
            .expect("wrong state type for WfActionFunc!");
        let fut = { (self.func)(wfctx, specific_state) };
        fut.await
    }
}

/*
 * Demo provision workflow:
 *
 *          create instance (database)
 *              |  |  |
 *       +------+  +  +-------------+
 *       |         |                |
 *       v         v                v
 *    alloc IP   create volume    pick server
 *       |         |                |
 *       +------+--+                v
 *              |             allocate server resources
 *              |                   |
 *              +-------------------+
 *              |
 *              v
 *          configure instance (server)
 *              |
 *              v
 *          attach volume
 *              |
 *              v
 *          boot instance
 */

#[derive(Debug)]
pub struct DemoProv {
    instance_id: Option<u64>,
    volume_id: Option<u64>,
    ip: Option<String>,
    server_id: Option<u64>,
}

type DemoProvWfState = Arc<Mutex<DemoProv>>;

async fn demo_prov_instance_create(
    _wfctx: Arc<WfContext>,
    wfstate: DemoProvWfState,
) -> WfResult {
    eprintln!("create instance");
    let instance_id = 1211;
    let mut st = wfstate.lock().await;
    st.instance_id.replace(instance_id).expect_none("duplicate instance id");
    Ok(())
}
async fn demo_prov_vpc_alloc_ip(
    _wfctx: Arc<WfContext>,
    wfstate: DemoProvWfState,
) -> WfResult {
    eprintln!("allocate IP");
    let ip = String::from("10.120.121.122");
    let mut st = wfstate.lock().await;
    assert_eq!(st.instance_id.unwrap(), 1211);
    st.ip.replace(ip).expect_none("duplicate vpc ip");
    Ok(())
}

/*
 * The next two steps are in a subworkflow!
 */
async fn demo_prov_server_alloc(
    wfctx: Arc<WfContext>,
    wfstate: DemoProvWfState,
) -> WfResult {
    eprintln!("allocate server (subworkflow)");

    let mut w = WfBuilder::new();
    w.append(WfActionFunc::new_action(demo_prov_server_pick));
    w.append(WfActionFunc::new_action(demo_prov_server_reserve));
    let wf = w.build();
    let init_state = DemoProvPickState {
        server_id: None,
        allocated: false,
    };

    let child_state = Arc::new(Mutex::new(init_state));
    wfctx.child_workflow(wf, Arc::clone(&child_state)).await?;

    /*
     * lock order: parent workflow state -> child workflow state
     * at least for now.  In reality, there should be no other references to the
     * child state so deadlock should not be possible in either order.
     */
    let mut parent_state = wfstate.lock().await;
    let child_state = child_state.lock().await;
    parent_state
        .server_id
        .replace(child_state.server_id.expect(
            "child workflow completed without error but did not set server_id",
        ))
        .expect_none("server_id was already set");
    Ok(())
}

struct DemoProvPickState {
    server_id: Option<u64>,
    allocated: bool,
}
async fn demo_prov_server_pick(
    _wfctx: Arc<WfContext>,
    wfstate: Arc<Mutex<DemoProvPickState>>,
) -> WfResult {
    eprintln!("    pick server");
    let server_id = 1212;
    let mut st = wfstate.lock().await;
    st.server_id.replace(server_id).expect_none("duplicate server id");
    Ok(())
}
async fn demo_prov_server_reserve(
    _wfctx: Arc<WfContext>,
    wfstate: Arc<Mutex<DemoProvPickState>>,
) -> WfResult {
    eprintln!("    reserve server");
    let mut st = wfstate.lock().await;
    assert_eq!(st.server_id.unwrap(), 1212);
    assert!(!st.allocated);
    st.allocated = true;
    Ok(())
}

async fn demo_prov_volume_create(
    _wfctx: Arc<WfContext>,
    wfstate: DemoProvWfState,
) -> WfResult {
    eprintln!("create volume");
    let volume_id = 1213;
    let mut st = wfstate.lock().await;
    assert_eq!(st.instance_id.unwrap(), 1211);
    st.volume_id.replace(volume_id).expect_none("duplicate volume id");
    Ok(())
}
async fn demo_prov_instance_configure(
    _wfctx: Arc<WfContext>,
    wfstate: DemoProvWfState,
) -> WfResult {
    eprintln!("configure instance");
    let st = wfstate.lock().await;
    assert_eq!(st.instance_id.unwrap(), 1211);
    assert_eq!(st.server_id.unwrap(), 1212);
    assert_eq!(st.volume_id.unwrap(), 1213);
    Ok(())
}
async fn demo_prov_volume_attach(
    _wfctx: Arc<WfContext>,
    wfstate: DemoProvWfState,
) -> WfResult {
    eprintln!("attach volume");
    let st = wfstate.lock().await;
    assert_eq!(st.instance_id.unwrap(), 1211);
    assert_eq!(st.server_id.unwrap(), 1212);
    assert_eq!(st.volume_id.unwrap(), 1213);
    Ok(())
}
async fn demo_prov_instance_boot(
    _wfctx: Arc<WfContext>,
    wfstate: DemoProvWfState,
) -> WfResult {
    eprintln!("boot instance");
    let st = wfstate.lock().await;
    assert_eq!(st.instance_id.unwrap(), 1211);
    assert_eq!(st.server_id.unwrap(), 1212);
    assert_eq!(st.volume_id.unwrap(), 1213);
    Ok(())
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
    root: NodeIndex,
    last: Vec<NodeIndex>,
}

#[derive(Debug)]
struct WfActionUniversalFirst {}

#[async_trait]
impl WfAction for WfActionUniversalFirst {
    async fn do_it(self: Box<Self>, _: Arc<WfContext>) -> WfResult {
        eprintln!("universal first action");
        Ok(())
    }
}

impl WfBuilder {
    fn new() -> WfBuilder {
        let mut graph = Graph::new();
        let mut launchers = BTreeMap::new();
        let first: Box<dyn WfAction + 'static> =
            Box::new(WfActionUniversalFirst {});
        let label = format!("{:?}", first);
        let root = graph.add_node(label);
        launchers.insert(root, first).expect_none("empty map had an element");

        WfBuilder {
            graph,
            launchers,
            root,
            last: vec![root],
        }
    }

    /*
     * Creates a new "phase" of the workflow consisting of a single action.
     * This action will depend on completion of all actions in the previous
     * phase.
     */
    fn append(&mut self, action: Box<dyn WfAction>) {
        let label = format!("{:?}", action);
        let newnode = self.graph.add_node(label);
        self.launchers
            .insert(newnode, action)
            .expect_none("action already present for newly created node");
        for node in &self.last {
            self.graph.add_edge(*node, newnode, ());
        }

        self.last = vec![newnode];
    }

    /*
     * Creates a new "phase" of the workflow consisting of a set of actions that
     * may run concurrently.  These actions will individually depend on all
     * actions in the previous phase having been completed.
     */
    fn append_parallel(&mut self, actions: Vec<Box<dyn WfAction>>) {
        let newnodes: Vec<NodeIndex> = actions
            .into_iter()
            .map(|a| {
                let label = format!("{:?}", a);
                let node = self.graph.add_node(label);
                self.launchers.insert(node, a).expect_none(
                    "action already present for newly created node",
                );
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
        for node in &self.last {
            for newnode in &newnodes {
                self.graph.add_edge(*node, *newnode, ());
            }
        }

        self.last = newnodes;
    }

    fn build(self) -> Workflow {
        Workflow {
            graph: self.graph,
            launchers: self.launchers,
            root: self.root,
        }
    }
}

pub struct Workflow {
    graph: Graph<String, ()>,
    launchers: BTreeMap<NodeIndex, Box<dyn WfAction>>,
    root: NodeIndex,
}

impl Debug for Workflow {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Workflow").field("graph", &self.graph).finish()
    }
}

/*
 * Construct a demo "provision" workflow matching the description above.
 * TODO maybe the Workflow should include the initial state object instead of
 * returning a tuple here?
 * Should Workflow be Clone and require that the initial state here be Clone?
 * I'm not sure it's possible to say only the initial state be Clone and not all
 * of the intermediate states.
 */
pub fn make_provision_workflow() -> (Workflow, Arc<Mutex<DemoProv>>) {
    let mut w = WfBuilder::new();

    w.append(WfActionFunc::new_action(demo_prov_instance_create));
    w.append_parallel(vec![
        WfActionFunc::new_action(demo_prov_vpc_alloc_ip),
        WfActionFunc::new_action(demo_prov_volume_create),
        WfActionFunc::new_action(demo_prov_server_alloc),
    ]);
    w.append(WfActionFunc::new_action(demo_prov_instance_configure));
    w.append(WfActionFunc::new_action(demo_prov_volume_attach));
    w.append(WfActionFunc::new_action(demo_prov_instance_boot));

    let wf = w.build();
    let init_state = DemoProv {
        instance_id: None,
        volume_id: None,
        ip: None,
        server_id: None,
    };

    (wf, Arc::new(Mutex::new(init_state)))
}

/*
 * Executes a workflow.
 */
pub struct WfExecutor {
    graph: Graph<String, ()>,
    launchers: BTreeMap<NodeIndex, Box<dyn WfAction>>,
    running: BTreeMap<NodeIndex, BoxFuture<'static, WfResult>>,
    finished: BTreeSet<NodeIndex>,
    ready: Vec<NodeIndex>,
    internal_state: Arc<dyn Any + Send + Sync + 'static>,

    // TODO probably better as a state enum
    error: Option<WfError>,
}

impl WfExecutor {
    pub fn new(
        w: Workflow,
        wfstate: Arc<dyn Any + Send + Sync + 'static>,
    ) -> WfExecutor {
        WfExecutor {
            graph: w.graph,
            launchers: w.launchers,
            running: BTreeMap::new(),
            finished: BTreeSet::new(),
            ready: vec![w.root],
            internal_state: wfstate,
            error: None,
        }
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
            return Poll::Ready(Ok(()));
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
            // TODO Is it reasonable to mutate inside assert?
            assert!(self.finished.insert(node));

            if let Err(error) = result {
                /*
                 * We currently assume errors are fatal.  That's not
                 * necessarily right.
                 * TODO how do we end right now?
                 * TODO we'll need to clean up too!
                 */
                self.error = Some(error)
            } else {
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
                        if !self.finished.contains(&upstream) {
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
            }
        }

        if self.error.is_none() {
            let to_schedule = self.ready.drain(..).collect::<Vec<NodeIndex>>();
            for node in to_schedule {
                let wfaction = self
                    .launchers
                    .remove(&node)
                    .expect("missing action for node");
                // TODO can probably drop this Arc
                let fut = wfaction.do_it(Arc::new(WfContext {
                    internal_state: Arc::clone(&self.internal_state),
                    node: node,
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

#[cfg(test)]
mod test {
    use super::make_provision_workflow;
    use super::WfExecutor;
    use core::any::Any;
    use std::sync::Arc;

    /*
     * Exercises much of the code here by constructing the demo provision
     * workflow.  We print the "Dot"-format graph to stderr so that we can
     * visually inspect the result.
     */
    #[tokio::test]
    async fn test_make_provision() {
        let (w, init_state) = make_provision_workflow();
        eprintln!("{:?}", w);
        {
            let st = init_state.lock().await;
            eprintln!("{:?}", *st);
        }
        eprintln!("{:?}", petgraph::dot::Dot::new(&w.graph));

        let generic_state =
            Arc::clone(&init_state) as Arc<dyn Any + Send + Sync>;
        let e = WfExecutor::new(w, generic_state);
        e.await.unwrap();
        let st = init_state.lock().await;
        eprintln!("final state: {:?}", *st);
    }
}
