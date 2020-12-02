use core::future::Future;

type WfError = anyhow::Error;
type WfResult<T> = Result<T, WfError>;

trait WfAction<I, O, Fut>
where
    Fut: Future<Output=WfResult<O>>
{
    fn do_it(&self, input: I) -> Fut;
}

impl<I, O, F, Fut> WfAction<I, O, Fut> for F
where
    F: Fn(I) -> Fut,
    Fut: Future<Output=WfResult<O>>
{
    fn do_it(&self, input: I) -> Fut {
        (self)(input)
    }
}

#[cfg(test)]
mod test {
    use petgraph::graph::Graph;
    use petgraph::visit::Topo;
    use super::WfAction;
    use super::WfResult;

    async fn do_thing_1(label: &str) -> WfResult<usize> {
        Ok(label.len())
    }

    #[tokio::test]
    async fn demo_wf_action() {
        let w: &dyn WfAction<&str, usize, _> = &do_thing_1;
        eprintln!("dap: {}", w.do_it("hello").await.unwrap());
    }

    #[test]
    fn demo() {
        /*
         * Create a 5-node graph that looks like this:
         *
         *     A
         *   / | \
         *  B  C  D
         *   \ | /
         *     E
         *
         * Then walk through the graph in dependency order.
         */
        let mut g = Graph::<&str, ()>::new();
        let ne = g.add_node("E");
        let nd = g.add_node("D");
        let na = g.add_node("A");
        let nb = g.add_node("B");
        let nc = g.add_node("C");

        g.add_edge(nb, ne, ());
        g.add_edge(nc, ne, ());
        g.add_edge(nd, ne, ());
        g.add_edge(na, nb, ());
        g.add_edge(na, nc, ());
        g.add_edge(na, nd, ());

        let mut topo = Topo::new(&g);
        while let Some(n) = topo.next(&g) {
            eprintln!("visit: {}", g[n]);
        }
    }
}
