use super::{decode, state_for_identifier};
use serde_json::Value;

#[test]
fn all_external_observations() {
    let fixture: Value = serde_json::from_str(include_str!(
        "../../../../tests/fixtures/subscription/status-observations.json"
    ))
    .unwrap();
    let mut seen = [false; 256];
    for group in fixture["groups"].as_array().unwrap() {
        for code in group["codes"].as_array().unwrap() {
            let code = code.as_u64().unwrap() as u16;
            assert!(!seen[usize::from(code)]);
            seen[usize::from(code)] = true;
            let entry = decode(code, Some(257));
            assert_eq!(entry.code, code);
            assert_eq!(entry.status, group["api"]["status"].as_str(), "{code:#06x}");
            if entry.status.is_some() {
                assert_eq!(entry.observed_summary, group["api"]["summary"].as_str());
                assert_eq!(state_for_identifier(entry.status.unwrap()), entry.state);
            } else {
                assert_eq!(entry.state, "unknown");
            }
        }
    }
    assert!(seen.into_iter().all(|value| value));
    for record in fixture["context_records"].as_array().unwrap() {
        let entry = decode(
            record["wire"]["subscription_status_code"].as_u64().unwrap() as u16,
            Some(record["wire"]["rx_status_code"].as_u64().unwrap() as u16),
        );
        assert_eq!(entry.status, record["api"]["status"].as_str());
        assert_eq!(entry.observed_summary, record["api"]["summary"].as_str());
    }
}

#[test]
fn untested_high_values_never_alias_a_tested_value() {
    for code in 256..=u16::MAX {
        let entry = decode(code, Some(257));
        assert_eq!(entry.code, code);
        assert_eq!(entry.receiver_status_code, Some(257));
        assert_eq!(entry.status, None);
        assert_eq!(entry.state, "unknown");
    }
}

#[test]
fn code_one_requires_an_observed_receiver_context() {
    assert_eq!(decode(1, Some(257)).status, Some("DYNAMIC"));
    assert_eq!(decode(1, Some(0)).status, Some("UNRESOLVED"));
    for receiver in [None, Some(1), Some(256), Some(258), Some(u16::MAX)] {
        let entry = decode(1, receiver);
        assert_eq!(entry.receiver_status_code, receiver);
        assert_eq!(entry.status, None);
        assert_eq!(entry.interpretation, "receiver_context_required");
    }
    assert_eq!(
        decode(9, Some(0)).interpretation,
        "receiver_context_unverified"
    );
    assert_eq!(decode(9, Some(0)).observed_summary, None);
}

#[test]
fn managed_identifiers_do_not_invent_numeric_values() {
    assert_eq!(state_for_identifier("DYNAMIC"), "connected");
    assert_eq!(state_for_identifier("UNRESOLVED"), "unresolved");
    assert_eq!(state_for_identifier("NEW_UNKNOWN_STATUS"), "unknown");
}
