//! Persistent state for workflows
//! This implementation is heavily inspired by McCaffrey's work on Distributed
//! Sagas.  See README for details.  TODO write that README.

use crate::WfError;
use crate::WfId;
use crate::WfOutput;
use anyhow::anyhow;
use anyhow::Context;
use chrono::DateTime;
use chrono::Utc;
use std::any::type_name_of_val;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

pub type WfNodeId = u64;
pub type WfLogResult = Result<(), WfError>;

/**
 * Event types that may be found in the log for a particular action
 *
 * (This is not a general-purpose debug log, but more like an intent log for
 * recovering the action's state in the event of an executor crash.)
 * TODO We might still want to put more information here, like the failure
 * error details and other debugging state.
 */
#[derive(Clone, Debug)]
pub enum WfNodeEventType {
    /** The action has started running */
    Started,
    /** The action completed successfully (with output data) */
    Succeeded(WfOutput),
    /** The action failed */
    Failed,
    /** The cancel action has started running */
    CancelStarted,
    /** The cancel action has finished */
    CancelFinished,
}

impl fmt::Display for WfNodeEventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WfNodeEventType::Started => f.write_str("started"),
            WfNodeEventType::Succeeded(o) => {
                write!(f, "succeeded (output type: {})", type_name_of_val(o))
            }
            WfNodeEventType::Failed => f.write_str("failed"),
            WfNodeEventType::CancelStarted => f.write_str("cancel started"),
            WfNodeEventType::CancelFinished => f.write_str("cancel finished"),
        }
    }
}

/**
 * Persistent status for a workflow node
 *
 * The events present in the log determine the _persistent status_ of the node.
 * You can think of this like a single summary of the state of this action,
 * based solely on the persistent state.  When recovering from a crash, the
 * workflow executor uses this status to determine what to do next.  We also
 * maintain this for each WfLog to identify illegal transitions at runtime.
 *
 * A node's status is very nearly identified by the type of the last event seen.
 * It's cleaner to have a first-class summary here.
 * TODO refer to the functions that ingest a list of events.
 */
#[derive(Debug, Clone)]
pub enum WfNodeLoadStatus {
    /** The action never started running */
    NeverStarted,
    /** The action has started running */
    Started,
    /** The action completed successfully (with output data) */
    Succeeded(WfOutput),
    /** The action failed */
    Failed,
    /** The cancel action has started running */
    CancelStarted,
    /** The cancel action has finished */
    CancelFinished,
}

impl WfNodeLoadStatus {
    /** Returns the new status for a node after recording the given event. */
    fn next_status(
        &self,
        event_type: &WfNodeEventType,
    ) -> Result<WfNodeLoadStatus, WfError> {
        match (self, event_type) {
            (WfNodeLoadStatus::NeverStarted, WfNodeEventType::Started) => {
                Ok(WfNodeLoadStatus::Started)
            }
            (WfNodeLoadStatus::Started, WfNodeEventType::Succeeded(out)) => {
                Ok(WfNodeLoadStatus::Succeeded(Arc::clone(&out)))
            }
            (WfNodeLoadStatus::Started, WfNodeEventType::Failed) => {
                Ok(WfNodeLoadStatus::Failed)
            }
            (
                WfNodeLoadStatus::Succeeded(_),
                WfNodeEventType::CancelStarted,
            ) => Ok(WfNodeLoadStatus::CancelStarted),
            (WfNodeLoadStatus::Failed, WfNodeEventType::CancelStarted) => {
                Ok(WfNodeLoadStatus::CancelStarted)
            }
            (
                WfNodeLoadStatus::CancelStarted,
                WfNodeEventType::CancelFinished,
            ) => Ok(WfNodeLoadStatus::CancelFinished),
            _ => Err(anyhow!(
                "workflow node with status \"{}\": event \"{}\" is illegal"
            )),
        }
    }
}

/**
 * An entry in the workflow log
 */
#[derive(Clone)]
pub struct WfNodeEvent {
    /** id of the workflow */
    workflow_id: WfId,
    /** id of the workflow node */
    node_id: WfNodeId,
    /** what's indicated by this event */
    event_type: WfNodeEventType,

    /* The following debugging fields are not used in the code. */
    /** when this event was recorded (for debugging) */
    #[allow(dead_code)]
    event_time: DateTime<Utc>,
    /** creator of this event (e.g., a hostname, for debugging) */
    #[allow(dead_code)]
    creator: String,
}

impl fmt::Debug for WfNodeEvent {
    /*
     * TODO We'd save a lot of horizontal space (9 columns) by dropping
     * precision after milliseconds and the time zone.
     */
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{} {} N{:0>3} {}",
            self.event_time.to_rfc3339(),
            self.creator,
            self.node_id,
            self.event_type
        )
    }
}

/**
 * Write to a workflow's log
 */
// TODO This structure is used both for writing to the log and recovering the
// log.  There are some similarities.  However, it might be useful to enforce
// that you're only doing one of these at a time by having these by separate
// types, with the recovery one converting into WfLog when you're done with
// recovery.
#[derive(Clone)]
pub struct WfLog {
    // TODO include version here
    pub workflow_id: WfId,
    creator: String,
    events: Vec<WfNodeEvent>,
    node_status: BTreeMap<WfNodeId, WfNodeLoadStatus>,
}

impl WfLog {
    pub fn new(creator: &str, workflow_id: WfId) -> WfLog {
        WfLog {
            workflow_id,
            creator: creator.to_string(),
            events: Vec::new(),
            node_status: BTreeMap::new(),
        }
    }

    pub fn record_now(
        &mut self,
        node_id: WfNodeId,
        event_type: WfNodeEventType,
    ) -> impl core::future::Future<Output = WfLogResult> {
        let event = WfNodeEvent {
            workflow_id: self.workflow_id,
            node_id,
            event_time: Utc::now(),
            event_type,
            creator: self.creator.clone(),
        };

        let result = self.record(event).expect("illegal event");

        /*
         * Although this implementation is synchronous, we want callers to
         * behave as though it were async.
         */
        async move { Ok(result) }
    }

    fn record(&mut self, event: WfNodeEvent) -> Result<(), WfError> {
        let current_status = self.load_status_for_node(event.node_id);
        let next_status = current_status.next_status(&event.event_type)?;

        self.node_status.insert(event.node_id, next_status);
        self.events.push(event);
        Ok(())
    }

    pub fn load_status_for_node(&self, node_id: WfNodeId) -> &WfNodeLoadStatus {
        self.node_status
            .get(&node_id)
            .unwrap_or(&WfNodeLoadStatus::NeverStarted)
    }
}

impl fmt::Debug for WfLog {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "WORKFLOW LOG:\n")?;
        write!(f, "workflow execution id: {}\n", self.workflow_id)?;
        write!(f, "creator:               {}\n", self.creator)?;
        write!(f, "events ({} total):\n", self.events.len())?;
        write!(f, "\n")?;

        for (i, event) in self.events.iter().enumerate() {
            write!(f, "  #{:0>3} {:?}\n", i + 1, event)?;
        }

        Ok(())
    }
}

/**
 * Reconstruct a workflow's persistent log state
 * TODO Something about this being a standalone function feels odd.
 */
pub fn recover_workflow_log(
    wflog: &mut WfLog,
    mut events: Vec<WfNodeEvent>,
) -> Result<(), WfError> {
    /*
     * This is our runtime way of ensuring you don't go from write-mode to
     * recovery mode.  See the TODO on WfLog above -- we could enforce this at
     * compile time instead.
     */
    assert!(wflog.events.is_empty());

    /*
     * Sort the events by the event type.  This ensures that if there's at least
     * one valid sequence of events, then we'll replay the events in a valid
     * sequence.  Thus, if we fail to replay below, then the log is corrupted
     * somehow.  (Remember, the wall timestamp is never used for correctness.)
     * For debugging purposes, this is a little disappointing: most likely, the
     * events are already in a valid order that reflects when they actually
     * happened.  However, there's nothing to guarantee that unless we make it
     * so, and our simple approach for doing so here destroys the sequential
     * order.  This should only really matter for a person looking at the
     * sequence of entries (as they appear in memory) for debugging.
     */
    events.sort_by_key(|f| match f.event_type {
        /*
         * TODO Is there a better way to do this?  We want to sort by the event
         * type, where event types are compared by the order they're defined in
         * WfEventType.  We could almost use derived PartialOrd and PartialEq
         * implementations for WfEventType, except that one variant has a
         * payload that does _not_ necessarily implement PartialEq or
         * PartialOrd.  It seems like that means we have to implement this by
         * hand.
         */
        WfNodeEventType::Started => 1,
        WfNodeEventType::Succeeded(_) => 2,
        WfNodeEventType::Failed => 3,
        WfNodeEventType::CancelStarted => 4,
        WfNodeEventType::CancelFinished => 5,
    });

    /*
     * Replay the events for this workflow.
     */
    for event in events {
        /*
         * The caller is responsible for ensuring that all of our events are for
         * the correct workflow.
         */
        assert_eq!(wflog.workflow_id, event.workflow_id);
        wflog.record(event).with_context(|| "recovering workflow log")?;
    }

    Ok(())
}

//
// TODO lots of automated tests are possible here, but let's see if the
// abstraction makes any sense first.
//
