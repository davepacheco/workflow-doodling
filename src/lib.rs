//! In-progress prototype implementation of workflows based on distributed sagas

#![deny(elided_lifetimes_in_paths)]
#![feature(option_expect_none)]
#![feature(or_patterns)]

mod example_provision;
mod wf_action;
mod wf_exec;
mod wf_log;
mod wf_workflow;

pub use example_provision::make_provision_workflow;
pub use wf_action::new_action_noop_undo;
pub use wf_action::WfActionFunc;
pub use wf_action::WfActionResult;
pub use wf_action::WfError;
pub use wf_action::WfFuncResult;
pub use wf_action::WfUndoResult;
pub use wf_exec::WfContext;
pub use wf_exec::WfExecutor;
pub use wf_log::WfLog;
pub use wf_workflow::WfBuilder;
pub use wf_workflow::WfId;
pub use wf_workflow::Workflow;
