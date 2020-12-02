#![feature(type_name_of_val)]

use core::fmt;
use core::fmt::Debug;
use core::future::Future;
use petgraph::graph::Graph;
use petgraph::graph::NodeIndex;

#[macro_use]
extern crate async_trait;

type WfError = anyhow::Error;
type WfResult = Result<(), WfError>;

#[async_trait]
trait WfAction {
    async fn do_it(&self) -> WfResult;

    /*
     * Currently, all WfAction objects are really functions.  If Fn impl'd
     * Debug, then we wouldn't need this, and we wouldn't need to impl Debug for
     * WfAction either.
     */
    fn debug_label(&self) -> &str;
}

/*
 * See above.  This also sucks because the specific WfAction impl might have its
 * own more specific Debug impl.  If we wanted to keep this workaround while
 * still supporting that, then instead of implementing WfAction for functions,
 * we could create a struct WfActionFunc that wraps a function and implements
 * Debug itself (the way we do below).  Then we would simply require that
 * WfAction types impl Debug instead of doing this here.
 */
impl Debug for dyn WfAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WfAction").field("impl", &self.debug_label()).finish()
    }
}

#[async_trait]
impl<Fut, Func> WfAction for Func
where
    Func: Fn() -> Fut + Send + Sync + 'static,
    Fut: Future<Output = WfResult> + Send + Sync + 'static,
{
    async fn do_it(&self) -> WfResult {
        (self)().await
    }

    fn debug_label(&self) -> &str {
        std::any::type_name_of_val(self)
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

async fn demo_prov_instance_create() -> WfResult {
    eprintln!("create instance");
    Ok(())
}
async fn demo_prov_vpc_alloc_ip() -> WfResult {
    eprintln!("allocate IP");
    Ok(())
}
async fn demo_prov_server_pick() -> WfResult {
    eprintln!("pick server");
    Ok(())
}
async fn demo_prov_server_reserve() -> WfResult {
    eprintln!("reserve server");
    Ok(())
}
async fn demo_prov_volume_create() -> WfResult {
    eprintln!("create volume");
    Ok(())
}
async fn demo_prov_instance_configure() -> WfResult {
    eprintln!("configure instance");
    Ok(())
}
async fn demo_prov_volume_attach() -> WfResult {
    eprintln!("attach volume");
    Ok(())
}
async fn demo_prov_instance_boot() -> WfResult {
    eprintln!("boot instance");
    Ok(())
}

#[derive(Debug)]
struct WorkflowBuilder {
    graph: Graph<Box<dyn WfAction>, ()>,
    root: NodeIndex,
    last: Vec<NodeIndex>,
}

async fn wf_action_first() -> WfResult {
    eprintln!("universal first action");
    Ok(())
}

impl WorkflowBuilder {
    fn new() -> WorkflowBuilder {
        let mut graph = Graph::<Box<dyn WfAction>, ()>::new();
        let root = graph.add_node(Box::new(wf_action_first));

        WorkflowBuilder {
            graph,
            root,
            last: vec![root],
        }
    }

    fn append(&mut self, action: Box<dyn WfAction>) {
        let newnode = self.graph.add_node(action);
        for node in &self.last {
            self.graph.add_edge(*node, newnode, ());
        }

        self.last = vec![newnode];
    }

    fn append_parallel(&mut self, actions: Vec<Box<dyn WfAction>>) {
        let newnodes: Vec<NodeIndex> =
            actions.into_iter().map(|a| self.graph.add_node(a)).collect();

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
}

fn make_provision_workflow() -> WorkflowBuilder {
    let mut w = WorkflowBuilder::new();

    w.append(Box::new(demo_prov_instance_create));
    w.append_parallel(vec![
        Box::new(demo_prov_vpc_alloc_ip),
        Box::new(demo_prov_volume_create),
    ]);
    w.append(Box::new(demo_prov_instance_configure));

    w
}

#[cfg(test)]
mod test {
    use super::make_provision_workflow;
    use super::WfAction;
    use super::WfResult;
    use super::WorkflowBuilder;

    #[test]
    fn test_make_provision() {
        let w = make_provision_workflow();
        eprintln!("{:?}", w);
        eprintln!("{:?}", petgraph::dot::Dot::new(&w.graph))
    }
}
