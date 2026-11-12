use serde_json::Value;

// Upstreams that do not report usage (ollama, some llama.cpp builds) still need a
// number in the dashboard, so this is a deliberately rough BPE-style estimate:
// it is only used when the upstream itself does not return usage.
pub fn estimate_tokens(text: &str) -> i64 {
    let mut total = 0i64;
    for chunk in text.split_whitespace() {
        let len = chunk.chars().count() as i64;
        total += 1.max((len + 3) / 4);
    }
    total
}

// ChatML-style prompt overhead: one marker per message plus the generation prefix.
pub fn prompt_estimate(messages: &Value) -> i64 {
    let Some(arr) = messages.as_array() else {
        return 0;
    };
    let mut total = 1i64;
    for msg in arr {
        total += 3;
        total += estimate_tokens(msg.get("role").and_then(Value::as_str).unwrap_or(""));
        match msg.get("content") {
            Some(Value::String(s)) => total += estimate_tokens(s),
            Some(Value::Array(parts)) => {
                for part in parts {
                    if let Some(t) = part.get("text").and_then(Value::as_str) {
                        total += estimate_tokens(t);
                    }
                }
            }
            _ => {}
        }
    }
    total
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn estimates_short_text_as_subword_pieces() {
        // 4 chars per token floor, at least one token per whitespace chunk.
        assert_eq!(estimate_tokens("hello there"), 4);
        assert_eq!(estimate_tokens("character"), 3);
        assert_eq!(estimate_tokens(""), 0);
    }

    #[test]
    fn prompt_estimate_counts_message_overhead() {
        let messages = serde_json::json!([
            { "role": "system", "content": "be brief" },
            { "role": "user", "content": "hi" }
        ]);
        // 1 generation prefix + (3 + role tokens + content tokens) per message
        assert!(prompt_estimate(&messages) >= 12);
    }
}
