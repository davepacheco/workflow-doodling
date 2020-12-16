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
use std::sync::Arc;

pub type WfNodeId = u64;

/**
 * Event types that may be found in the log for a particular action
 *
 * (This is not a general-purpose debug log, but more like an intent log for
 * recovering the action's state in the event of an executor crash.)
 * TODO We might still want to put more information here, like the failure
 * error details and other debugging state.
 */
#[derive(Debug)]
pub enum WfEventType {
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
        event_type: &WfEventType,
    ) -> Result<WfNodeLoadStatus, WfError> {
        match (self, event_type) {
            (WfNodeLoadStatus::NeverStarted, WfEventType::Started) => {
                Ok(WfNodeLoadStatus::Started)
            }
            (WfNodeLoadStatus::Started, WfEventType::Succeeded(out)) => {
                Ok(WfNodeLoadStatus::Succeeded(Arc::clone(&out)))
            }
            (WfNodeLoadStatus::Started, WfEventType::Failed) => {
                Ok(WfNodeLoadStatus::Failed)
            }
            (WfNodeLoadStatus::Succeeded(_), WfEventType::CancelStarted) => {
                Ok(WfNodeLoadStatus::CancelStarted)
            }
            (WfNodeLoadStatus::Failed, WfEventType::CancelStarted) => {
                Ok(WfNodeLoadStatus::CancelStarted)
            }
            (WfNodeLoadStatus::CancelStarted, WfEventType::CancelFinished) => {
                Ok(WfNodeLoadStatus::CancelFinished)
            }
            _ => Err(anyhow!(
                "workflow node with status \"{}\": event \"{}\" is illegal"
            )),
        }
    }
}

/**
 * An entry in the workflow log
 */
pub struct WfEvent {
    /** id of the workflow */
    workflow_id: WfId,
    /** id of the workflow node */
    node_id: WfNodeId,
    /** when this event was recorded (for debugging) */
    event_time: DateTime<Utc>,
    /** what's indicated by this event */
    event_type: WfEventType,
    /** creator of this event (e.g., a hostname, for debugging) */
    creator: String,
}

/**
 * Write to a workflow's log
 */
pub struct WfLog {
    // TODO include version here
    workflow_id: WfId,
    creator: String,
    events: Vec<WfEvent>,
    status: WfNodeLoadStatus,
}

impl WfLog {
    pub fn new(creator: String, workflow_id: WfId) -> WfLog {
        WfLog {
            workflow_id,
            creator,
            events: Vec::new(),
            status: WfNodeLoadStatus::NeverStarted,
        }
    }

    pub async fn record_now(
        &mut self,
        node_id: WfNodeId,
        event_type: WfEventType,
    ) -> Result<(), WfError> {
        let event = WfEvent {
            workflow_id: self.workflow_id,
            node_id,
            event_time: Utc::now(),
            event_type,
            creator: self.creator.clone(),
        };

        Ok(self.record(event).expect("illegal event"))
    }

    fn record(&mut self, event: WfEvent) -> Result<(), WfError> {
        let event_type = &event.event_type;
        let next_status = self.status.next_status(event_type)?;

        self.events.push(event);
        self.status = next_status;
        Ok(())
    }
}

/**
 * Reconstruct a workflow's persistent log state
 * TODO Something about this being a standalone function feels odd.
 */
pub fn recover_workflow_log(
    wflog: &mut WfLog,
    mut events: Vec<WfEvent>,
) -> Result<WfNodeLoadStatus, WfError> {
    assert!(wflog.events.is_empty());

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
        WfEventType::Started => 1,
        WfEventType::Succeeded(_) => 2,
        WfEventType::Failed => 3,
        WfEventType::CancelStarted => 4,
        WfEventType::CancelFinished => 5,
    });

    for event in events {
        wflog.record(event).with_context(|| "recovering workflow log")?;
    }

    Ok(wflog.status.clone())
}

//
// TODO lots of automated tests are possible here, but let's see if the
// abstraction makes any sense first.
//
