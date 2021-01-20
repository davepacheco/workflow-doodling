/*!
 * Common code shared by examples
 */

use crate::new_action_noop_undo;
use crate::SagaContext;
use crate::SagaTemplate;
use crate::SagaTemplateBuilder;
use crate::WfFuncResult;
use serde::Deserialize;
use serde::Serialize;
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
pub fn make_provision_saga() -> Arc<SagaTemplate> {
    let mut w = SagaTemplateBuilder::new();

    w.append(
        "instance_id",
        "InstanceCreate",
        new_action_noop_undo(demo_prov_instance_create),
    );
    w.append_parallel(vec![
        (
            "instance_ip",
            "VpcAllocIp",
            new_action_noop_undo(demo_prov_vpc_alloc_ip),
        ),
        (
            "volume_id",
            "VolumeCreate",
            new_action_noop_undo(demo_prov_volume_create),
        ),
        (
            "server_id",
            "ServerAlloc (subworkflow)",
            new_action_noop_undo(demo_prov_server_alloc),
        ),
    ]);
    w.append(
        "instance_configure",
        "InstanceConfigure",
        new_action_noop_undo(demo_prov_instance_configure),
    );
    w.append(
        "volume_attach",
        "VolumeAttach",
        new_action_noop_undo(demo_prov_volume_attach),
    );
    w.append(
        "instance_boot",
        "InstanceBoot",
        new_action_noop_undo(demo_prov_instance_boot),
    );
    w.append("print", "Print", new_action_noop_undo(demo_prov_print));
    Arc::new(w.build())
}

async fn demo_prov_instance_create(wfctx: SagaContext) -> WfFuncResult<u64> {
    eprintln!("running action: {}", wfctx.node_label());
    /* make up an instance ID */
    let instance_id = 1211u64;
    Ok(instance_id)
}

async fn demo_prov_vpc_alloc_ip(wfctx: SagaContext) -> WfFuncResult<String> {
    eprintln!("running action: {}", wfctx.node_label());
    /* exercise using some data from a previous node */
    let instance_id = wfctx.lookup::<u64>("instance_id");
    assert_eq!(instance_id, 1211);
    /* make up an IP (simulate allocation) */
    let ip = String::from("10.120.121.122");
    Ok(ip)
}

/*
 * The next two steps are in a subworkflow!
 */
async fn demo_prov_server_alloc(wfctx: SagaContext) -> WfFuncResult<u64> {
    eprintln!("running action: {}", wfctx.node_label());

    let mut w = SagaTemplateBuilder::new();
    w.append(
        "server_id",
        "ServerPick",
        new_action_noop_undo(demo_prov_server_pick),
    );
    w.append(
        "server_reserve",
        "ServerReserve",
        new_action_noop_undo(demo_prov_server_reserve),
    );
    let wf = Arc::new(w.build());

    let e = wfctx.child_workflow(wf).await;
    e.run().await;
    let result = e.result();
    let server_allocated: Arc<ServerAllocResult> =
        result.lookup_output("server_reserve")?;
    Ok(server_allocated.server_id)
}

#[derive(Debug, Deserialize, Serialize)]
struct ServerAllocResult {
    server_id: u64,
}

async fn demo_prov_server_pick(wfctx: SagaContext) -> WfFuncResult<u64> {
    eprintln!("running action: {}", wfctx.node_label());
    /* make up ("allocate") a new server id */
    let server_id = 1212u64;
    Ok(server_id)
}

async fn demo_prov_server_reserve(
    wfctx: SagaContext,
) -> WfFuncResult<ServerAllocResult> {
    eprintln!("running action: {}", wfctx.node_label());
    /* exercise using data from previous nodes */
    let server_id = wfctx.lookup::<u64>("server_id");
    assert_eq!(server_id, 1212);
    /* package this up for downstream consumers */
    Ok(ServerAllocResult { server_id })
}

async fn demo_prov_volume_create(wfctx: SagaContext) -> WfFuncResult<u64> {
    eprintln!("running action: {}", wfctx.node_label());
    /* exercise using data from previous nodes */
    assert_eq!(wfctx.lookup::<u64>("instance_id"), 1211);
    /* make up ("allocate") a volume id */
    let volume_id = 1213u64;
    Ok(volume_id)
}
async fn demo_prov_instance_configure(wfctx: SagaContext) -> WfFuncResult<()> {
    eprintln!("running action: {}", wfctx.node_label());
    /* exercise using data from previous nodes */
    assert_eq!(wfctx.lookup::<u64>("instance_id"), 1211);
    assert_eq!(wfctx.lookup::<u64>("server_id"), 1212);
    assert_eq!(wfctx.lookup::<u64>("volume_id"), 1213);
    Ok(())
}
async fn demo_prov_volume_attach(wfctx: SagaContext) -> WfFuncResult<()> {
    eprintln!("running action: {}", wfctx.node_label());
    /* exercise using data from previous nodes */
    assert_eq!(wfctx.lookup::<u64>("instance_id"), 1211);
    assert_eq!(wfctx.lookup::<u64>("server_id"), 1212);
    assert_eq!(wfctx.lookup::<u64>("volume_id"), 1213);
    Ok(())
}
async fn demo_prov_instance_boot(wfctx: SagaContext) -> WfFuncResult<()> {
    eprintln!("running action: {}", wfctx.node_label());
    /* exercise using data from previous nodes */
    assert_eq!(wfctx.lookup::<u64>("instance_id"), 1211);
    assert_eq!(wfctx.lookup::<u64>("server_id"), 1212);
    assert_eq!(wfctx.lookup::<u64>("volume_id"), 1213);
    Ok(())
}

async fn demo_prov_print(wfctx: SagaContext) -> WfFuncResult<()> {
    eprintln!("running action: {}", wfctx.node_label());
    eprintln!("printing final state:");
    let instance_id = wfctx.lookup::<u64>("instance_id");
    eprintln!("  instance id: {}", instance_id);
    let ip = wfctx.lookup::<String>("instance_ip");
    eprintln!("  IP address: {}", ip);
    let volume_id = wfctx.lookup::<u64>("volume_id");
    eprintln!("  volume id: {}", volume_id);
    let server_id = wfctx.lookup::<u64>("server_id");
    eprintln!("  server id: {}", server_id);
    Ok(())
}
