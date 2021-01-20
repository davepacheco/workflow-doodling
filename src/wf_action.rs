//! Definition of Action trait, core implementations, and related facilities

use crate::wf_exec::WfContext;
use anyhow::anyhow;
use anyhow::Context;
use async_trait::async_trait;
use core::any::type_name;
use core::fmt;
use core::fmt::Debug;
use core::future::Future;
use core::marker::PhantomData;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value as JsonValue;
use std::sync::Arc;

/*
 * Result, output, and error types used for actions
 */

/** Error produced by a workflow action or a workflow itself */
pub type WfError = anyhow::Error;

/** Result of a workflow action */
// TODO-cleanup can we drop this Arc?
pub type WfActionResult = Result<Arc<JsonValue>, WfError>;
/** Result of a workflow undo action. */
pub type WfUndoResult = Result<(), WfError>;

/**
 * Result of a function that implements a workflow action
 *
 * This differs from [`WfActionResult`] because [`WfActionResult`] returns a
 * pretty generic type.  The function-oriented interface allows you to return
 * more specific types as long as they implement the [`WfActionOutput`] trait.
 */
/*
 * TODO-design There's no reason that WfActionResult couldn't also look like
 * this.  We have this mechanism to allow `WfActionFunc` functions to return
 * specific types while storing the generic thing inside the framework.  We do
 * this translation in the impl of `WfActionFunc`.  Instead, we could create
 * another layer above `WfAction` that does this.  This gets complicated and
 * doesn't seem especially useful yet.
 */
pub type WfFuncResult<T> = Result<T, WfError>;

/**
 * Success return type for functions that are used as workflow actions
 *
 * This trait exists as a name for `Debug + DeserializeOwned + Serialize + Send
 * + Sync`.  Consumers are not expected to impl this directly.  
 */
pub trait WfActionOutput:
    Debug + DeserializeOwned + Serialize + Send + Sync
{
}
impl<T: Debug + DeserializeOwned + Serialize + Send + Sync> WfActionOutput
    for T
{
}

/*
 * Generic Action interface
 */

/**
 * Building blocks of workflows
 *
 * Each node in a workflow graph is represented with some kind of `WfAction`,
 * which provides entry points to asynchronously execute an action and its
 * corresponding undo action.  A workflow is essentially a directed acyclic
 * graph of these actions with dependencies between them.  Each action consumes
 * a [`WfContext`] and asynchronously produces a [`WfActionResult`].  The
 * primary implementor for most consumers is [`WfActionFunc`].
 */
/*
 * We currently don't expose the `WfAction` trait directly to users, but we
 * easily could if that proved useful.  We may want to think more carefully
 * about the `WfActionResult` type if we do that.
 */
#[async_trait]
pub trait WfAction: Debug + Send + Sync {
    /**
     * Executes the action for this workflow node, whatever that is.  Actions
     * function like requests in distributed sagas: critically, they must be
     * idempotent.  They should be very careful in using interfaces outside of
     * [`WfContext`] -- we want them to be as self-contained as possible to
     * ensure idempotence and to minimize versioning issues.
     *
     * On success, this function produces a `WfActionOutput`.  This output will
     * be stored persistently, keyed by the _name_ of the current workflow node.
     * Subsequent stages can access this data with [`WfContext::lookup`].  This
     * is the _only_ supported means of sharing state across actions within a
     * workflow.
     */
    async fn do_it(&self, wfctx: WfContext) -> WfActionResult;

    /**
     * Executes the compensation action for this workflow node, whatever that
     * is.
     */
    async fn undo_it(&self, wfctx: WfContext) -> WfUndoResult;
}

/*
 * WfAction implementations
 */

/** Represents the start node in a graph */
#[derive(Debug)]
pub struct WfActionStartNode {}

#[async_trait]
impl WfAction for WfActionStartNode {
    async fn do_it(&self, _: WfContext) -> WfActionResult {
        eprintln!("<action for \"start\" node>");
        Ok(Arc::new(JsonValue::Null))
    }

    async fn undo_it(&self, _: WfContext) -> WfUndoResult {
        eprintln!(
            "<undo for \"start\" node (workflow is nearly done unwinding)>"
        );
        Ok(())
    }
}

/** Represents the end node in a graph */
#[derive(Debug)]
pub struct WfActionEndNode {}

#[async_trait]
impl WfAction for WfActionEndNode {
    async fn do_it(&self, _: WfContext) -> WfActionResult {
        eprintln!("<action for \"end\" node: workflow is nearly done>");
        Ok(Arc::new(JsonValue::Null))
    }

    async fn undo_it(&self, _: WfContext) -> WfUndoResult {
        /*
         * We should not run compensation actions for nodes that have not
         * started.  We should never start this node unless all other actions
         * have completed.  We should never unwind a workflow unless some action
         * failed.  Thus, we should never undo the "end" node in a workflow.
         */
        panic!("attempted to undo end node in workflow");
    }
}

/** Simulates an error at a given spot in the graph */
#[derive(Debug)]
pub struct WfActionInjectError {}

#[async_trait]
impl WfAction for WfActionInjectError {
    async fn do_it(&self, wfctx: WfContext) -> WfActionResult {
        let message = format!(
            "<boom! error injected instead of action for \
            node \"{}\">",
            wfctx.node_label()
        );
        eprintln!("{}", message);
        Err(anyhow!("{}", message))
    }

    async fn undo_it(&self, _: WfContext) -> WfUndoResult {
        /* We should never undo an action that failed. */
        unimplemented!();
    }
}

/**
 * Implementation for [`WfAction`] using simple functions for the action and
 * undo action
 */
/*
 * The type parameters here look pretty complicated, but it's simpler than it
 * looks.  `WfActionFunc` wraps two asynchronous functions.  Both consume a
 * `WfContext`.  On success, the action function produces a type that impls
 * `WfActionOutput` and the undo function produces nothing.  Because they're
 * asynchronous and because the first function can produce any type that impls
 * `WfActionOutput`, we get this explosion of type parameters and trait bounds.
 */
pub struct WfActionFunc<
    ActionFutType,
    ActionFuncType,
    ActionFuncOutput,
    UndoFutType,
    UndoFuncType,
> where
    ActionFuncType: Fn(WfContext) -> ActionFutType + Send + Sync + 'static,
    ActionFutType:
        Future<Output = WfFuncResult<ActionFuncOutput>> + Send + Sync + 'static,
    ActionFuncOutput: WfActionOutput + 'static,
    UndoFuncType: Fn(WfContext) -> UndoFutType + Send + Sync + 'static,
    UndoFutType: Future<Output = WfUndoResult> + Send + Sync + 'static,
{
    action_func: ActionFuncType,
    undo_func: UndoFuncType,
    phantom: PhantomData<(ActionFutType, UndoFutType)>,
}

impl<
        ActionFutType,
        ActionFuncType,
        ActionFuncOutput,
        UndoFutType,
        UndoFuncType,
    >
    WfActionFunc<
        ActionFutType,
        ActionFuncType,
        ActionFuncOutput,
        UndoFutType,
        UndoFuncType,
    >
where
    ActionFuncType: Fn(WfContext) -> ActionFutType + Send + Sync + 'static,
    ActionFutType:
        Future<Output = WfFuncResult<ActionFuncOutput>> + Send + Sync + 'static,
    ActionFuncOutput: WfActionOutput + 'static,
    UndoFuncType: Fn(WfContext) -> UndoFutType + Send + Sync + 'static,
    UndoFutType: Future<Output = WfUndoResult> + Send + Sync + 'static,
{
    /**
     * Construct a `WfAction` from a pair of functions, using `action_func`
     * for the action and `undo_func` for the undo action
     *
     * We return the result as a `Arc<dyn WfAction>` so that it can be used
     * directly where `WfAction`s are expected.  (The struct `WfActionFunc` has
     * no interfaces of its own so there's generally no need to have the
     * specific type.)
     */
    pub fn new_action(
        action_func: ActionFuncType,
        undo_func: UndoFuncType,
    ) -> Arc<dyn WfAction> {
        Arc::new(WfActionFunc { action_func, undo_func, phantom: PhantomData })
    }
}

/*
 * TODO-cleanup why can't new_action_noop_undo live in the WfAction namespace?
 */

async fn undo_noop(wfctx: WfContext) -> WfUndoResult {
    eprintln!("<noop undo for node: \"{}\">", wfctx.node_label());
    Ok(())
}

/**
 * Given a function `f`, return a `WfActionFunc` that uses `f` as the action and
 * provides a no-op undo function (which does nothing and always succeeds).
 */
pub fn new_action_noop_undo<ActionFutType, ActionFuncType, ActionFuncOutput>(
    f: ActionFuncType,
) -> Arc<dyn WfAction>
where
    ActionFuncType: Fn(WfContext) -> ActionFutType + Send + Sync + 'static,
    ActionFutType:
        Future<Output = WfFuncResult<ActionFuncOutput>> + Send + Sync + 'static,
    ActionFuncOutput: WfActionOutput + 'static,
{
    WfActionFunc::new_action(f, undo_noop)
}

#[async_trait]
impl<
        ActionFutType,
        ActionFuncType,
        ActionFuncOutput,
        UndoFutType,
        UndoFuncType,
    > WfAction
    for WfActionFunc<
        ActionFutType,
        ActionFuncType,
        ActionFuncOutput,
        UndoFutType,
        UndoFuncType,
    >
where
    ActionFuncType: Fn(WfContext) -> ActionFutType + Send + Sync + 'static,
    ActionFutType:
        Future<Output = WfFuncResult<ActionFuncOutput>> + Send + Sync + 'static,
    ActionFuncOutput: WfActionOutput + 'static,
    UndoFuncType: Fn(WfContext) -> UndoFutType + Send + Sync + 'static,
    UndoFutType: Future<Output = WfUndoResult> + Send + Sync + 'static,
{
    async fn do_it(&self, wfctx: WfContext) -> WfActionResult {
        let label = wfctx.node_label().to_owned();
        let fut = { (self.action_func)(wfctx) };
        /*
         * Execute the caller's function and translate its type into the generic
         * JsonValue that the framework uses to store action outputs.
         */
        fut.await
            .with_context(|| format!("executing node \"{}\"", label))
            .and_then(|func_output| {
                serde_json::to_value(func_output).with_context(|| {
                    format!("serializing output from node \"{}\"", label)
                })
            })
            .map(Arc::new)
    }

    async fn undo_it(&self, wfctx: WfContext) -> WfUndoResult {
        let fut = { (self.undo_func)(wfctx) };
        fut.await
    }
}

impl<
        ActionFutType,
        ActionFuncType,
        ActionFuncOutput,
        UndoFutType,
        UndoFuncType,
    > Debug
    for WfActionFunc<
        ActionFutType,
        ActionFuncType,
        ActionFuncOutput,
        UndoFutType,
        UndoFuncType,
    >
where
    ActionFuncType: Fn(WfContext) -> ActionFutType + Send + Sync + 'static,
    ActionFutType:
        Future<Output = WfFuncResult<ActionFuncOutput>> + Send + Sync + 'static,
    ActionFuncOutput: WfActionOutput + 'static,
    UndoFuncType: Fn(WfContext) -> UndoFutType + Send + Sync + 'static,
    UndoFutType: Future<Output = WfUndoResult> + Send + Sync + 'static,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        /*
         * The type name for a function includes its name, so it's a handy
         * summary for debugging.
         */
        f.write_str(&type_name::<ActionFuncType>())
    }
}
