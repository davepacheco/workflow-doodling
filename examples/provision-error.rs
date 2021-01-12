/*!
 * Example of running the provision workflow with an injected error
 */
use std::io;
use std::sync::Arc;
use workflow_doodling::make_provision_workflow;
use workflow_doodling::WfExecutor;

#[tokio::main]
async fn main() {
    let mut stderr = io::stderr();
    let w = make_provision_workflow();
    let e = WfExecutor::new(Arc::clone(&w), "provision-demo");
    eprintln!("\n*** running workflow ***");
    e.inject_error("volume_attach").await;
    e.run().await;
    eprintln!("*** finished workflow ***");

    eprintln!("\n*** final state ***");
    e.print_status(&mut stderr, 0).await.unwrap();

    eprintln!("\n*** final log ***");
    let result = e.result();
    eprintln!("{:?}", result.wflog);
}
