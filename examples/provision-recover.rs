/*!
 * Exercise recovery of a successful workflow.
 */

use std::io;
use std::sync::Arc;
use workflow_doodling::make_provision_workflow;
use workflow_doodling::recover_workflow_log;
use workflow_doodling::WfExecutor;
use workflow_doodling::WfLog;

#[tokio::main]
async fn main() {
    let mut stderr = io::stderr();
    let w = make_provision_workflow();
    eprintln!("*** workflow definition ***");
    eprintln!("{:?}", w);

    /* Run the workflow once. */
    eprintln!("*** running workflow to generate a complete log ***");
    let e = WfExecutor::new(Arc::clone(&w), "provision-demo");
    e.run().await;

    /* Extract the log, trim it, and recover from there. */
    let result = e.result();
    let mut events = result.wflog.events().clone();
    events.truncate(9);
    let mut wflog = WfLog::new("example", result.wflog.workflow_id);
    recover_workflow_log(&mut wflog, events).unwrap();
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
