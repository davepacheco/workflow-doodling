/*!
 * Common code shared by examples
 */

use crate::new_action_noop_undo;
use crate::WfBuilder;
use crate::WfContext;
use crate::WfFuncResult;
use crate::Workflow;
use std::sync::Arc;

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

/*
 * Construct a demo "provision" workflow matching the description above.
 */
pub fn make_provision_workflow() -> Arc<Workflow> {
    let mut w = WfBuilder::new();

    w.append("instance_id", new_action_noop_undo(demo_prov_instance_create));
    w.append_parallel(vec![
        ("instance_ip", new_action_noop_undo(demo_prov_vpc_alloc_ip)),
        ("volume_id", new_action_noop_undo(demo_prov_volume_create)),
        ("server_id", new_action_noop_undo(demo_prov_server_alloc)),
    ]);
    w.append(
        "instance_configure",
        new_action_noop_undo(demo_prov_instance_configure),
    );
    w.append("volume_attach", new_action_noop_undo(demo_prov_volume_attach));
    w.append("instance_boot", new_action_noop_undo(demo_prov_instance_boot));
    w.append("print", new_action_noop_undo(demo_prov_print));
    Arc::new(w.build())
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
    w.append("server_id", new_action_noop_undo(demo_prov_server_pick));
    w.append("server_reserve", new_action_noop_undo(demo_prov_server_reserve));
    let wf = Arc::new(w.build());

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
    // Ok(Arc::new(()))
    Err(anyhow::anyhow!("injected error"))
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
