/*!
 * Command-line tool for demo'ing workflow interfaces
 */

use anyhow::Context;
use std::fs;
use std::io;
use std::path::PathBuf;
use std::sync::Arc;
use structopt::StructOpt;
use workflow_doodling::make_provision_workflow;
use workflow_doodling::WfExecutor;
use workflow_doodling::WfLog;

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    let subcmd = Demo::from_args();
    match subcmd {
        Demo::Dot => cmd_dot().await,
        Demo::Info => cmd_info().await,
        Demo::PrintLog { ref print_log_args } => {
            cmd_print_log(print_log_args).await
        }
        Demo::Run { ref run_args } => cmd_run(run_args).await,
    }
}

/// Demo workflow implementation
#[derive(Debug, StructOpt)]
enum Demo {
    /// Dump a dot (graphviz) representation of the workflow graph
    Dot,

    /// Dump information about the workflow graph (not an execution)
    Info,

    /// Pretty-print the log from a previous execution
    PrintLog {
        #[structopt(flatten)]
        print_log_args: PrintLogArgs,
    },

    /// Execute the workflow
    Run {
        #[structopt(flatten)]
        run_args: RunArgs,
    },
}

/*
 * "dot" subcommand
 */

async fn cmd_dot() -> Result<(), anyhow::Error> {
    let mut stdout = io::stdout();
    let workflow = make_provision_workflow();
    workflow.print_dot(&mut stdout).unwrap();
    Ok(())
}

/*
 * "info" subcommand
 */

async fn cmd_info() -> Result<(), anyhow::Error> {
    let mut stderr = io::stderr();
    let workflow = make_provision_workflow();
    eprintln!("*** workflow definition ***");
    eprintln!("workflow graph: ");
    workflow.print_dot(&mut stderr).unwrap();

    eprintln!("*** initial state ***");
    let exec = WfExecutor::new(workflow, "provision-info");
    exec.print_status(&mut stderr, 0).await.unwrap();
    Ok(())
}

/*
 * "print-log" subcommand
 */

#[derive(Debug, StructOpt)]
struct PrintLogArgs {
    /// path to the workflow log to pretty-print
    input_log_path: PathBuf,
}

async fn cmd_print_log(args: &PrintLogArgs) -> Result<(), anyhow::Error> {
    let input_log_path = &args.input_log_path;
    let file = fs::File::open(input_log_path).with_context(|| {
        format!("open recovery log \"{}\"", input_log_path.display())
    })?;
    let wflog = WfLog::load("unused", file).with_context(|| {
        format!("load log \"{}\"", input_log_path.display())
    })?;
    eprintln!("{:?}", wflog);
    Ok(())
}

/*
 * "run" subcommand
 */

#[derive(Debug, StructOpt)]
struct RunArgs {
    /// simulate an error at the named workflow node
    #[structopt(long)]
    inject_error: Vec<String>,

    /// upon completion, dump the workflog log to the named file
    #[structopt(long)]
    dump_to: Option<PathBuf>,

    /// recover the workflow log from the named file and resume execution
    #[structopt(long)]
    recover_from: Option<PathBuf>,

    /// "creator" attribute used when creating new log entries
    #[structopt(long, default_value = "demo-provision")]
    creator: String,
}

async fn cmd_run(args: &RunArgs) -> Result<(), anyhow::Error> {
    let mut stderr = io::stderr();
    let workflow = make_provision_workflow();
    let exec = if let Some(input_log_path) = &args.recover_from {
        eprintln!("recovering from log: {}", input_log_path.display());

        let file = fs::File::open(&input_log_path).with_context(|| {
            format!("open recovery log \"{}\"", input_log_path.display())
        })?;
        let wflog = WfLog::load(&args.creator, file).with_context(|| {
            format!("load log \"{}\"", input_log_path.display())
        })?;
        let exec = WfExecutor::new_recover(
            Arc::clone(&workflow),
            wflog,
            &args.creator,
        )
        .with_context(|| {
            format!("recover log \"{}\"", input_log_path.display())
        })?;

        eprintln!("recovered state:");
        exec.print_status(&mut stderr, 0).await.unwrap();
        exec
    } else {
        WfExecutor::new(Arc::clone(&workflow), &args.creator)
    };

    for node_name in &args.inject_error {
        let node_id =
            workflow.node_for_name(&node_name).with_context(|| {
                format!("bad argument for --inject-error: {:?}", node_name)
            })?;
        exec.inject_error(node_id).await;
        eprintln!("injecting error at node \"{}\"", node_name);
    }

    eprintln!("\n*** running workflow ***");
    exec.run().await;
    eprintln!("*** finished workflow ***");

    eprintln!("\n*** final state ***");
    exec.print_status(&mut stderr, 0).await.unwrap();

    if let Some(output_log_path) = &args.dump_to {
        let result = exec.result();
        let log = result.wflog;
        let out = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(output_log_path)
            .with_context(|| {
                format!("open output log \"{}\"", output_log_path.display())
            })?;
        log.dump(out).with_context(|| {
            format!("save output log \"{}\"", output_log_path.display())
        })?;
        eprintln!("dumped log to \"{}\"", output_log_path.display());
    }

    Ok(())
}
