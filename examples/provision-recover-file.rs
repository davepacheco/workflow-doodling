/*!
 * Exercise recovery of a successful workflow.
 */

use std::io;
use std::fs;
use std::sync::Arc;
use workflow_doodling::make_provision_workflow;
use workflow_doodling::WfExecutor;
use workflow_doodling::WfLog;

#[tokio::main]
async fn main() {
    let mut stderr = io::stderr();
    let w = make_provision_workflow();

    let file = fs::File::open("./demo_log").unwrap();
    let wflog = WfLog::recover(file).unwrap();

    eprintln!("*** recovering from the following log: ***");
    eprintln!("{:?}", wflog);
    let e = WfExecutor::new_recover(Arc::clone(&w), wflog, "provision-demo")
        .unwrap();
    eprintln!("*** initial state (recovered workflow): ***");
    e.print_status(&mut stderr, 0).await.unwrap();
    e.run().await;
    eprintln!("*** final state (recovered workflow): ***");
    e.print_status(&mut stderr, 0).await.unwrap();
}

