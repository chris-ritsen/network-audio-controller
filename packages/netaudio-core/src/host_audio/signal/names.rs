use super::{Details, Graph, NetworkDirection, PulseKind};
use std::cmp::Ordering;
use std::collections::BTreeSet;

const FILLER: &[&str] = &["my", "our", "your", "the", "a", "an", "this", "that"];
const GENERIC: &[&str] = &[
    "avio", "usb", "usbc", "aes", "bt", "rx", "tx", "lx", "dante", "ch", "in", "out", "left",
    "right", "local",
];

pub fn fold(value: &str) -> String {
    caseless::default_case_fold_str(&value.split_whitespace().collect::<Vec<_>>().join(" "))
}

pub fn plain_label(value: &str) -> &str {
    if let Some(before) = value.strip_suffix(')') {
        if let Some((label, kind)) = before.rsplit_once('(') {
            if !kind.contains(')') {
                return label.trim_end();
            }
        }
    }
    value
}

pub fn natural(left: &str, right: &str) -> Ordering {
    let mut first = left.chars().peekable();
    let mut second = right.chars().peekable();
    loop {
        match (first.peek(), second.peek()) {
            (Some(left), Some(right)) if left.is_ascii_digit() && right.is_ascii_digit() => {
                let mut left = String::new();
                let mut right = String::new();
                while first.peek().is_some_and(char::is_ascii_digit) {
                    left.push(first.next().expect("peeked character"));
                }
                while second.peek().is_some_and(char::is_ascii_digit) {
                    right.push(second.next().expect("peeked character"));
                }
                let left_number = left.trim_start_matches('0');
                let right_number = right.trim_start_matches('0');
                let order = left_number
                    .len()
                    .cmp(&right_number.len())
                    .then_with(|| left_number.cmp(right_number));
                if order != Ordering::Equal {
                    return order;
                }
            }
            _ => {
                let left = first.next();
                let right = second.next();
                let order = left.cmp(&right);
                if order != Ordering::Equal || left.is_none() {
                    return order;
                }
            }
        }
    }
}

fn words(value: &str) -> Vec<String> {
    fold(value)
        .split(|character: char| !character.is_ascii_alphanumeric())
        .filter(|word| !word.is_empty())
        .map(str::to_owned)
        .collect()
}

fn bigrams(value: &str) -> BTreeSet<(char, char)> {
    value.chars().zip(value.chars().skip(1)).collect()
}

impl Graph {
    pub fn resolve(&self, point: &str) -> Vec<String> {
        let Some(entries) = self.names.get(&fold(point)) else {
            return Vec::new();
        };
        let Some(priority) = entries.iter().map(|(priority, _)| priority).min() else {
            return Vec::new();
        };
        let mut identities = entries
            .iter()
            .filter(|(current, _)| current == priority)
            .map(|(_, identity)| identity.clone())
            .collect::<Vec<_>>();
        identities.sort_by(|left, right| natural(left, right).then_with(|| left.cmp(right)));
        identities.dedup();
        identities
    }

    pub fn suggestions(&self, point: &str) -> Vec<String> {
        let point = fold(point);
        let wanted = bigrams(&point);
        if wanted.is_empty() {
            return Vec::new();
        }
        let mut candidates = self
            .names
            .keys()
            .filter_map(|name| {
                let available = bigrams(name);
                let score = (2 * wanted.intersection(&available).count()) as f64
                    / (wanted.len() + available.len()) as f64;
                (score >= 0.6).then_some((score, name))
            })
            .collect::<Vec<_>>();
        candidates.sort_by(|(first_score, first), (second_score, second)| {
            second_score
                .total_cmp(first_score)
                .then_with(|| natural(first, second))
        });
        candidates
            .into_iter()
            .take(5)
            .map(|(_, name)| name.clone())
            .collect()
    }

    pub fn resolve_device(&self, point: &str) -> Vec<String> {
        let point = fold(point);
        let mut found = self
            .nodes
            .iter()
            .filter_map(|(identity, node)| {
                let Details::Dante(node) = &node.details else {
                    return None;
                };
                let edges = match node.direction {
                    NetworkDirection::Receiver => &self.upstream,
                    NetworkDirection::Transmitter => &self.downstream,
                };
                (fold(&node.device) == point
                    && edges.get(identity).is_some_and(|edges| !edges.is_empty()))
                .then(|| identity.clone())
            })
            .collect::<Vec<_>>();
        found.sort_by(|first, second| natural(first, second).then_with(|| first.cmp(second)));
        found
    }

    fn loose_candidates(&self, words: &[String], prefix: bool) -> BTreeSet<String> {
        let mut found = BTreeSet::new();
        for (name, entries) in &self.names {
            let tokens = super::names::words(name);
            if words.iter().all(|word| {
                tokens
                    .iter()
                    .any(|token| token == word || (prefix && token.starts_with(word)))
            }) {
                found.extend(entries.iter().map(|(_, identity)| identity.clone()));
            }
        }
        if !words.iter().any(|word| word == "monitor") {
            found.retain(|identity| !self.nodes[identity].label.ends_with(".monitor"));
        }
        found
    }

    pub fn resolve_loosely(&self, point: &str) -> Vec<String> {
        let mut words = words(point);
        if words.is_empty() {
            return Vec::new();
        }
        let mut found = self.loose_candidates(&words, false);
        if found.is_empty() {
            words.retain(|word| !FILLER.contains(&word.as_str()));
            if words.is_empty() {
                return Vec::new();
            }
            found = self.loose_candidates(&words, false);
        }
        if found.is_empty() {
            let stems = words
                .iter()
                .map(|word| {
                    if word.len() >= 4 {
                        word[..word.len() - 1].to_owned()
                    } else {
                        word.clone()
                    }
                })
                .collect::<Vec<_>>();
            if stems != words {
                found = self.loose_candidates(&stems, true);
            }
        }
        let rank = |identity: &str| {
            let node = &self.nodes[identity];
            let kind = match &node.details {
                Details::Pulse(_) => 0,
                Details::Wireless(_) => 1,
                Details::Dante(_) => 2,
                Details::Jack(_) => 3,
            };
            (
                kind,
                node.host().is_some_and(|host| host != self.local_host),
            )
        };
        let Some(best) = found.iter().map(|identity| rank(identity)).min() else {
            return Vec::new();
        };
        let mut chosen = found
            .into_iter()
            .filter(|identity| rank(identity) == best)
            .collect::<Vec<_>>();
        if chosen.len() > 4 {
            let transmitters = chosen.iter().filter(|identity| matches!(&self.nodes[*identity].details, Details::Dante(node) if node.direction == NetworkDirection::Transmitter)).cloned().collect::<Vec<_>>();
            if !transmitters.is_empty() {
                chosen = transmitters;
            }
        }
        if chosen.len() > 4 {
            return Vec::new();
        }
        chosen.sort_by(|first, second| natural(first, second).then_with(|| first.cmp(second)));
        chosen
    }

    pub fn also_named(&self, point: &str, exclude: &BTreeSet<String>) -> Vec<String> {
        let words = words(point)
            .into_iter()
            .filter(|word| {
                word.len() > 1
                    && !GENERIC.contains(&word.as_str())
                    && !FILLER.contains(&word.as_str())
            })
            .collect::<Vec<_>>();
        if words.is_empty() {
            return Vec::new();
        }
        let mut found = BTreeSet::new();
        for (name, entries) in &self.names {
            let tokens = super::names::words(name);
            if words.iter().all(|word| tokens.contains(word)) {
                found.extend(
                    entries
                        .iter()
                        .filter(|(_, identity)| {
                            !exclude.contains(identity)
                                && !matches!(self.nodes[identity].details, Details::Dante(_))
                        })
                        .map(|(_, identity)| identity.clone()),
                );
            }
        }
        let mut found = found.into_iter().collect::<Vec<_>>();
        found.sort_by(|first, second| natural(first, second).then_with(|| first.cmp(second)));
        found
    }

    pub fn find(&self, point: &str) -> Vec<String> {
        let words = words(point);
        if words.is_empty() {
            return Vec::new();
        }
        let mut names = std::collections::BTreeMap::<&str, String>::new();
        for (name, entries) in &self.names {
            for (_, identity) in entries {
                let text = names.entry(identity).or_default();
                text.push(' ');
                text.push_str(name);
            }
        }
        let mut found = self
            .nodes
            .iter()
            .filter(|(_, node)| !matches!(node.details, Details::Dante(_)))
            .filter_map(|(identity, node)| {
                let text = super::names::words(&format!(
                    "{} {}",
                    node.label,
                    names
                        .get(identity.as_str())
                        .map(String::as_str)
                        .unwrap_or_default()
                ))
                .join(" ");
                words
                    .iter()
                    .all(|word| text.contains(word))
                    .then(|| identity.clone())
            })
            .collect::<Vec<_>>();
        found.sort_by(|first, second| {
            natural(&self.nodes[first].label, &self.nodes[second].label)
                .then_with(|| first.cmp(second))
        });
        found
    }

    pub fn stream_device(&self, identity: &str) -> Option<&str> {
        let node = self.nodes.get(identity)?;
        let Details::Pulse(pulse) = &node.details else {
            return Some(&node.identity);
        };
        let edges = match pulse.kind {
            PulseKind::Playback => &self.downstream,
            PulseKind::Recording => &self.upstream,
            _ => return Some(&node.identity),
        };
        Some(edges.get(identity).into_iter().flatten().find(|edge| matches!(&self.nodes[&edge.identity].details, Details::Pulse(node) if matches!(node.kind, PulseKind::Sink | PulseKind::Source))).map_or(node.identity.as_str(), |edge| &edge.identity))
    }
}
