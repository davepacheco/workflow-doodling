type WfError = anyhow::Error;
type WfResult<T> = Result<T, WfError>;

trait WfAction<I, O> {
    fn do_it(&self, input: I) -> WfResult<O>;
}

impl<I, O, F> WfAction<I, O> for F
where
    F: Fn(I) -> WfResult<O>
{
    fn do_it(&self, input: I) -> WfResult<O> {
        (self)(input)
    }
}

#[cfg(test)]
mod test {
    use petgraph::graph::Graph;
    use petgraph::visit::Topo;

    #[test]
    pub fn demo() {
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
