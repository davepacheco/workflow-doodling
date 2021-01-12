/*!
 * Very basic example that just prints the demo workflow
 */
use std::io;
use std::sync::Arc;
use workflow_doodling::make_provision_workflow;
use workflow_doodling::WfExecutor;

#[tokio::main]
async fn main() {
    let mut stderr = io::stderr();
    let w = make_provision_workflow();
    eprintln!("*** workflow definition ***");
    eprintln!("{:?}", w);

    eprintln!("*** initial state ***");
    let e = WfExecutor::new(Arc::clone(&w), "provision-info");
    e.print_status(&mut stderr, 0).await.unwrap();
}
