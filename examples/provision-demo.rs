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

use std::io;
use std::sync::Arc;
use workflow_doodling::WfActionFunc;
use workflow_doodling::WfBuilder;
use workflow_doodling::WfContext;
use workflow_doodling::WfExecutor;
use workflow_doodling::WfFuncResult;
use workflow_doodling::Workflow;

/*
 * Construct a demo "provision" workflow matching the description above.
 */
pub fn make_provision_workflow() -> Workflow {
    let mut w = WfBuilder::new();

    w.append(
        "instance_id",
        WfActionFunc::new_action(demo_prov_instance_create),
    );
    w.append_parallel(vec![
        ("instance_ip", WfActionFunc::new_action(demo_prov_vpc_alloc_ip)),
        ("volume_id", WfActionFunc::new_action(demo_prov_volume_create)),
        ("server_id", WfActionFunc::new_action(demo_prov_server_alloc)),
    ]);
    w.append(
        "instance_configure",
        WfActionFunc::new_action(demo_prov_instance_configure),
    );
    w.append(
        "volume_attach",
        WfActionFunc::new_action(demo_prov_volume_attach),
    );
    w.append(
        "instance_boot",
        WfActionFunc::new_action(demo_prov_instance_boot),
    );
    w.append("print", WfActionFunc::new_action(demo_prov_print));
    w.build()
}

async fn demo_prov_instance_create(_wfctx: WfContext) -> WfFuncResult {
    eprintln!("create instance");
    let instance_id = 1211u64;
    Ok(Arc::new(instance_id))
}

async fn demo_prov_vpc_alloc_ip(wfctx: WfContext) -> WfFuncResult {
    eprintln!("allocate IP");
    let ip = String::from("10.120.121.122");
    let instance_id = wfctx.lookup::<u64>("instance_id")?;
    assert_eq!(*instance_id, 1211);
    Ok(Arc::new(ip))
}

/*
 * The next two steps are in a subworkflow!
 */
async fn demo_prov_server_alloc(wfctx: WfContext) -> WfFuncResult {
    eprintln!("allocate server (subworkflow)");

    let mut w = WfBuilder::new();
    w.append("server_id", WfActionFunc::new_action(demo_prov_server_pick));
    w.append(
        "server_reserve",
        WfActionFunc::new_action(demo_prov_server_reserve),
    );
    let wf = w.build();

    let e = wfctx.child_workflow(wf).await;
    e.run().await;
    let result = e.result();
    let server_allocated: Arc<ServerAllocResult> =
        result.lookup_output("server_reserve")?;
    Ok(Arc::new(server_allocated.server_id))
}

struct ServerAllocResult {
    server_id: u64,
}

async fn demo_prov_server_pick(_wfctx: WfContext) -> WfFuncResult {
    eprintln!("    pick server");
    let server_id = 1212u64;
    Ok(Arc::new(server_id))
}

async fn demo_prov_server_reserve(wfctx: WfContext) -> WfFuncResult {
    eprintln!("    reserve server");
    let server_id = *wfctx.lookup::<u64>("server_id")?;
    assert_eq!(server_id, 1212);
    Ok(Arc::new(ServerAllocResult { server_id }))
}

async fn demo_prov_volume_create(wfctx: WfContext) -> WfFuncResult {
    eprintln!("create volume");
    let volume_id = 1213u64;
    assert_eq!(*wfctx.lookup::<u64>("instance_id")?, 1211);
    Ok(Arc::new(volume_id))
}
async fn demo_prov_instance_configure(wfctx: WfContext) -> WfFuncResult {
    eprintln!("configure instance");
    assert_eq!(*wfctx.lookup::<u64>("instance_id")?, 1211);
    assert_eq!(*wfctx.lookup::<u64>("server_id")?, 1212);
    assert_eq!(*wfctx.lookup::<u64>("volume_id")?, 1213);
    Ok(Arc::new(()))
}
async fn demo_prov_volume_attach(wfctx: WfContext) -> WfFuncResult {
    eprintln!("attach volume");
    assert_eq!(*wfctx.lookup::<u64>("instance_id")?, 1211);
    assert_eq!(*wfctx.lookup::<u64>("server_id")?, 1212);
    assert_eq!(*wfctx.lookup::<u64>("volume_id")?, 1213);
    Ok(Arc::new(()))
}
async fn demo_prov_instance_boot(wfctx: WfContext) -> WfFuncResult {
    eprintln!("boot instance");
    assert_eq!(*wfctx.lookup::<u64>("instance_id")?, 1211);
    assert_eq!(*wfctx.lookup::<u64>("server_id")?, 1212);
    assert_eq!(*wfctx.lookup::<u64>("volume_id")?, 1213);
    Ok(Arc::new(()))
}

async fn demo_prov_print(wfctx: WfContext) -> WfFuncResult {
    eprintln!("printing final state:");
    let instance_id = wfctx.lookup::<u64>("instance_id")?;
    eprintln!("  instance id: {}", *instance_id);
    let ip = wfctx.lookup::<String>("instance_ip")?;
    eprintln!("  IP address: {}", *ip);
    let volume_id = wfctx.lookup::<u64>("volume_id")?;
    eprintln!("  volume id: {}", *volume_id);
    let server_id = wfctx.lookup::<u64>("server_id")?;
    eprintln!("  server id: {}", *server_id);
    Ok(Arc::new(()))
}

#[tokio::main]
async fn main() {
    let mut stderr = io::stderr();
    let w = make_provision_workflow();
    eprintln!("*** workflow definition ***");
    eprintln!("{:?}", w);

    eprintln!("*** initial state ***");
    let e = WfExecutor::new(w);
    e.print_status(&mut stderr, 0).await.unwrap();

    eprintln!("*** running workflow ***");
    e.run().await;
    eprintln!("*** finished workflow ***");

    eprintln!("*** final state ***");
    e.print_status(&mut stderr, 0).await.unwrap();

    eprintln!("*** final log ***");
    let result = e.result();
    eprintln!("{:?}", result.wflog);
}
