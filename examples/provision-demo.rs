/*!
 * Very basic example of running the provision workflow
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
    let e = WfExecutor::new(Arc::clone(&w), "provision-demo");
    e.print_status(&mut stderr, 0).await.unwrap();

    eprintln!("\n*** running workflow ***");
    e.run().await;
    eprintln!("*** finished workflow ***");

    eprintln!("\n*** final state ***");
    e.print_status(&mut stderr, 0).await.unwrap();

    eprintln!("\n*** final log ***");
    let result = e.result();
    eprintln!("{:?}", result.wflog);
}
