//! Session identity helpers for lifecycle envelopes (DECISIONS D-004 / D-044).

use monoloop_contracts::{ChannelId, SessionId, SessionKey, TransactionId};

/// Transaction-scoped SessionId for sessionless DirectLlm tool/event envelopes.
///
/// Not an external resume identity (not for `session/load`).
pub(crate) fn transaction_scoped_session_id(transaction_id: TransactionId) -> SessionId {
    SessionId::try_new(format!("tx-{transaction_id}"))
        .or_else(|_| SessionId::try_new("direct"))
        .expect("session id")
}

/// True when `id` is exactly the synthetic id for this `transaction_id` (or legacy `direct`).
///
/// Exact match only — never treat arbitrary `tx-…` external ids as synthetic (LAW 7).
pub(crate) fn is_transaction_scoped_session(id: &SessionId, transaction_id: TransactionId) -> bool {
    let s = id.as_str();
    if s == "direct" {
        return true;
    }
    s == transaction_scoped_session_id(transaction_id).as_str()
}

/// SessionKey for tool results: admitted session when present, else transaction-scoped.
pub(crate) fn session_key_for(
    channel_id: ChannelId,
    session_id: Option<SessionId>,
    transaction_id: TransactionId,
) -> SessionKey {
    let sid = session_id.unwrap_or_else(|| transaction_scoped_session_id(transaction_id));
    SessionKey::new(channel_id, sid)
}

/// Resolve publisher session for one event.
///
/// Prefer an authoritative `preferred` id over a prior **exact** synthetic key for
/// this transaction; never replace an already-authoritative id (LAW 7).
pub(crate) fn ensure_session(
    session: &mut Option<SessionId>,
    preferred: Option<SessionId>,
    transaction_id: TransactionId,
) -> SessionId {
    if let Some(s) = preferred {
        let upgrade = match session.as_ref() {
            None => true,
            Some(cur) => is_transaction_scoped_session(cur, transaction_id),
        };
        if upgrade {
            *session = Some(s.clone());
            return s;
        }
        return session.clone().expect("authoritative session present");
    }
    if let Some(s) = session.clone() {
        return s;
    }
    let s = transaction_scoped_session_id(transaction_id);
    *session = Some(s.clone());
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn synthetic_and_authoritative_upgrade() {
        let tx = TransactionId::generate();
        let mut session = None;
        let syn = ensure_session(&mut session, None, tx);
        assert!(is_transaction_scoped_session(&syn, tx));
        let auth = SessionId::try_new("grok-real").unwrap();
        let got = ensure_session(&mut session, Some(auth.clone()), tx);
        assert_eq!(got, auth);
        // Second preferred must not flip an authoritative id.
        let other = SessionId::try_new("grok-other").unwrap();
        let kept = ensure_session(&mut session, Some(other), tx);
        assert_eq!(kept, auth);
    }

    #[test]
    fn tx_prefix_external_id_is_not_synthetic() {
        let tx = TransactionId::generate();
        // An external id that merely starts with "tx-" must not be treated as ours.
        let external = SessionId::try_new("tx-external-not-ours").unwrap();
        assert!(!is_transaction_scoped_session(&external, tx));
        let mut session = Some(external.clone());
        let other = SessionId::try_new("grok-claim").unwrap();
        // Must not upgrade away from a non-synthetic id.
        let kept = ensure_session(&mut session, Some(other), tx);
        assert_eq!(kept, external);
    }

    #[test]
    fn session_key_matches_scoped_id() {
        let tx = TransactionId::generate();
        let ch = ChannelId::try_new("llm").unwrap();
        let key = session_key_for(ch.clone(), None, tx);
        assert_eq!(key.session_id, transaction_scoped_session_id(tx));
        assert_eq!(key.channel_id, ch);
    }
}
