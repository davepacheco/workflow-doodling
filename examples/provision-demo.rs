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

    eprintln!("*** initial state ***");
    let e = WfExecutor::new(Arc::clone(&w));
    e.print_status(&mut stderr, 0).await.unwrap();

    eprintln!("*** running workflow ***");
    e.run().await;
    eprintln!("*** finished workflow ***");

    eprintln!("*** final state ***");
    e.print_status(&mut stderr, 0).await.unwrap();

    eprintln!("*** final log ***");
    let result = e.result();
    eprintln!("{:?}", result.wflog);

    /* Exercise recovery. */
    let mut events = result.wflog.events().clone();
    events.truncate(9);
    let mut wflog = WfLog::new("example", result.wflog.workflow_id);
    recover_workflow_log(&mut wflog, events).unwrap();
    eprintln!("*** recovery using these events: ***");
    eprintln!("{:?}", wflog);
    let e = WfExecutor::new_recover(Arc::clone(&w), wflog).unwrap();
    eprintln!("*** initial state (recovered workflow): ***");
    e.print_status(&mut stderr, 0).await.unwrap();
    e.run().await;
    eprintln!("*** final state (recovered workflow): ***");
    e.print_status(&mut stderr, 0).await.unwrap();
}
