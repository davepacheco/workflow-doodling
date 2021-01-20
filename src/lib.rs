//! In-progress prototype implementation based on distributed sagas

#![deny(elided_lifetimes_in_paths)]
#![feature(option_expect_none)]
#![feature(or_patterns)]

mod example_provision;
mod wf_action;
mod wf_exec;
mod wf_log;
mod wf_workflow;

pub use example_provision::make_provision_saga;
pub use wf_action::new_action_noop_undo;
pub use wf_action::WfActionFunc;
pub use wf_action::WfActionResult;
pub use wf_action::WfError;
pub use wf_action::WfFuncResult;
pub use wf_action::WfUndoResult;
pub use wf_exec::SagaContext;
pub use wf_exec::SagaExecutor;
pub use wf_log::SagaLog;
pub use wf_workflow::SagaTemplate;
pub use wf_workflow::SagaTemplateBuilder;
