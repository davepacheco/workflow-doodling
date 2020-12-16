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

use std::sync::Arc;
use workflow_doodling::WfActionFunc;
use workflow_doodling::WfBuilder;
use workflow_doodling::WfContext;
use workflow_doodling::WfExecutor;
use workflow_doodling::WfResult;
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

async fn demo_prov_instance_create(_wfctx: Arc<WfContext>) -> WfResult {
    eprintln!("create instance");
    let instance_id = 1211u64;
    Ok(Arc::new(instance_id))
}

async fn demo_prov_vpc_alloc_ip(wfctx: Arc<WfContext>) -> WfResult {
    eprintln!("allocate IP");
    let ip = String::from("10.120.121.122");
    let instance_id = wfctx.lookup::<u64>("instance_id")?;
    assert_eq!(*instance_id, 1211);
    Ok(Arc::new(ip))
}

/*
 * The next two steps are in a subworkflow!
 */
async fn demo_prov_server_alloc(wfctx: Arc<WfContext>) -> WfResult {
    eprintln!("allocate server (subworkflow)");

    let mut w = WfBuilder::new();
    w.append("server_id", WfActionFunc::new_action(demo_prov_server_pick));
    w.append(
        "server_reserve",
        WfActionFunc::new_action(demo_prov_server_reserve),
    );
    let wf = w.build();

    let result = wfctx.child_workflow(wf).await?;
    Ok(Arc::new(result.downcast::<ServerAllocResult>().unwrap().server_id))
}

struct ServerAllocResult {
    server_id: u64,
}

async fn demo_prov_server_pick(_wfctx: Arc<WfContext>) -> WfResult {
    eprintln!("    pick server");
    let server_id = 1212u64;
    Ok(Arc::new(server_id))
}
async fn demo_prov_server_reserve(wfctx: Arc<WfContext>) -> WfResult {
    eprintln!("    reserve server");
    let server_id = *wfctx.lookup::<u64>("server_id")?;
    assert_eq!(server_id, 1212);
    // XXX This is a janky way to provide output from the workflow itself
    Ok(Arc::new(ServerAllocResult {
        server_id,
    }))
}

async fn demo_prov_volume_create(wfctx: Arc<WfContext>) -> WfResult {
    eprintln!("create volume");
    let volume_id = 1213u64;
    assert_eq!(*wfctx.lookup::<u64>("instance_id")?, 1211);
    Ok(Arc::new(volume_id))
}
async fn demo_prov_instance_configure(wfctx: Arc<WfContext>) -> WfResult {
    eprintln!("configure instance");
    assert_eq!(*wfctx.lookup::<u64>("instance_id")?, 1211);
    assert_eq!(*wfctx.lookup::<u64>("server_id")?, 1212);
    assert_eq!(*wfctx.lookup::<u64>("volume_id")?, 1213);
    Ok(Arc::new(()))
}
async fn demo_prov_volume_attach(wfctx: Arc<WfContext>) -> WfResult {
    eprintln!("attach volume");
    assert_eq!(*wfctx.lookup::<u64>("instance_id")?, 1211);
    assert_eq!(*wfctx.lookup::<u64>("server_id")?, 1212);
    assert_eq!(*wfctx.lookup::<u64>("volume_id")?, 1213);
    Ok(Arc::new(()))
}
async fn demo_prov_instance_boot(wfctx: Arc<WfContext>) -> WfResult {
    eprintln!("boot instance");
    assert_eq!(*wfctx.lookup::<u64>("instance_id")?, 1211);
    assert_eq!(*wfctx.lookup::<u64>("server_id")?, 1212);
    assert_eq!(*wfctx.lookup::<u64>("volume_id")?, 1213);
    Ok(Arc::new(()))
}

async fn demo_prov_print(wfctx: Arc<WfContext>) -> WfResult {
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
    let w = make_provision_workflow();
    eprintln!("{:?}", w);
    let e = WfExecutor::new(w);
    e.await.unwrap();
}
