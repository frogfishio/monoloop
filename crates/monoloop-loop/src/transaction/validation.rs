//! Input payload and output-contract validation for linked tools.

use monoloop_contracts::{
    CanonicalToolError, CanonicalToolOutput, JsonSchema, ToolCompletion, ToolOutputContract,
    ToolSuccessContract,
};
use serde_json::Value;

/// Why input validation failed (caller maps to rejected tool result, not txn failure).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum InputValidationFailure {
    /// Payload exceeds byte limit.
    OversizedInput,
    /// JSON parse failed.
    InvalidJson,
    /// Nesting depth exceeded.
    DepthExceeded,
    /// Schema validation failed.
    SchemaInvalid,
}

/// Why output validation failed (maps to runtime failure / ToolExchangeFailed).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OutputValidationFailure {
    /// Encoded output exceeds max_output_bytes.
    OversizedOutput,
    /// Success body does not match declared success contract.
    SuccessShapeMismatch,
    /// Success schema invalid.
    SuccessSchemaInvalid,
    /// Domain error fields invalid or data schema mismatch.
    DomainErrorInvalid,
}

/// Maximum JSON nesting depth accepted for tool arguments/results.
#[allow(dead_code)] // used by deferred dispatcher until M5
pub const DEFAULT_MAX_JSON_DEPTH: u32 = 16;

/// Validate raw argument JSON string against size, depth, and schema.
pub fn validate_tool_input(
    payload: &str,
    schema: &JsonSchema,
    max_input_bytes: usize,
    max_depth: u32,
) -> Result<Value, InputValidationFailure> {
    if payload.len() > max_input_bytes {
        return Err(InputValidationFailure::OversizedInput);
    }
    let value: Value =
        serde_json::from_str(payload).map_err(|_| InputValidationFailure::InvalidJson)?;
    if !json_depth_ok(&value, 0, max_depth) {
        return Err(InputValidationFailure::DepthExceeded);
    }
    validate_against_schema(&value, schema).map_err(|_| InputValidationFailure::SchemaInvalid)?;
    Ok(value)
}

/// Validate a handler completion against the tool output contract.
pub fn validate_tool_completion(
    completion: ToolCompletion,
    contract: &ToolOutputContract,
    max_output_bytes: usize,
    max_error_message_bytes: usize,
    max_depth: u32,
) -> Result<ToolCompletion, OutputValidationFailure> {
    match completion {
        ToolCompletion::Succeeded(output) => {
            validate_success_output(&output, &contract.success, max_output_bytes, max_depth)?;
            Ok(ToolCompletion::Succeeded(output))
        }
        ToolCompletion::DomainFailed(err) => {
            validate_domain_error(
                &err,
                contract.error_data_schema.as_ref(),
                max_output_bytes,
                max_error_message_bytes,
                max_depth,
            )?;
            Ok(ToolCompletion::DomainFailed(err))
        }
        ToolCompletion::RuntimeFailed(e) => Ok(ToolCompletion::RuntimeFailed(e)),
    }
}

fn validate_success_output(
    output: &CanonicalToolOutput,
    success: &ToolSuccessContract,
    max_output_bytes: usize,
    max_depth: u32,
) -> Result<(), OutputValidationFailure> {
    match (output, success) {
        (CanonicalToolOutput::Json(v), ToolSuccessContract::Json { schema }) => {
            if encoded_len(v) > max_output_bytes {
                return Err(OutputValidationFailure::OversizedOutput);
            }
            if !json_depth_ok(v, 0, max_depth) {
                return Err(OutputValidationFailure::SuccessSchemaInvalid);
            }
            validate_against_schema(v, schema)
                .map_err(|_| OutputValidationFailure::SuccessSchemaInvalid)?;
            Ok(())
        }
        (CanonicalToolOutput::Text(t), ToolSuccessContract::Text { .. }) => {
            if t.len() > max_output_bytes {
                return Err(OutputValidationFailure::OversizedOutput);
            }
            if t.chars()
                .any(|c| c.is_control() && c != '\n' && c != '\t' && c != '\r')
            {
                return Err(OutputValidationFailure::SuccessShapeMismatch);
            }
            Ok(())
        }
        _ => Err(OutputValidationFailure::SuccessShapeMismatch),
    }
}

fn validate_domain_error(
    err: &CanonicalToolError,
    data_schema: Option<&JsonSchema>,
    max_output_bytes: usize,
    max_error_message_bytes: usize,
    max_depth: u32,
) -> Result<(), OutputValidationFailure> {
    // Re-validate bounds (handler may bypass try_new).
    if err.code.is_empty()
        || err.code.len() > 64
        || err.code.chars().any(|c| c.is_control())
        || err.message.is_empty()
        || err.message.len() > max_error_message_bytes
        || err.message.chars().any(|c| c.is_control())
    {
        return Err(OutputValidationFailure::DomainErrorInvalid);
    }
    if let Some(data) = &err.data {
        if encoded_len(data) > max_output_bytes {
            return Err(OutputValidationFailure::OversizedOutput);
        }
        if !json_depth_ok(data, 0, max_depth) {
            return Err(OutputValidationFailure::DomainErrorInvalid);
        }
        if let Some(schema) = data_schema {
            validate_against_schema(data, schema)
                .map_err(|_| OutputValidationFailure::DomainErrorInvalid)?;
        }
    } else if data_schema.is_some() {
        // Optional data when schema present is allowed (schema applies when data exists).
    }
    Ok(())
}

fn validate_against_schema(value: &Value, schema: &JsonSchema) -> Result<(), ()> {
    let validator = jsonschema::validator_for(schema.as_value()).map_err(|_| ())?;
    if validator.is_valid(value) {
        Ok(())
    } else {
        Err(())
    }
}

fn encoded_len(v: &Value) -> usize {
    serde_json::to_vec(v).map(|b| b.len()).unwrap_or(usize::MAX)
}

fn json_depth_ok(value: &Value, depth: u32, max: u32) -> bool {
    if depth > max {
        return false;
    }
    match value {
        Value::Array(items) => items.iter().all(|v| json_depth_ok(v, depth + 1, max)),
        Value::Object(map) => map.values().all(|v| json_depth_ok(v, depth + 1, max)),
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use monoloop_contracts::JsonSchema;

    #[test]
    fn rejects_oversized_input() {
        let schema = JsonSchema::try_new(serde_json::json!({"type": "object"})).unwrap();
        let big = format!("{{\"x\":\"{}\"}}", "a".repeat(100));
        let err = validate_tool_input(&big, &schema, 10, 16).unwrap_err();
        assert_eq!(err, InputValidationFailure::OversizedInput);
    }

    #[test]
    fn rejects_schema_invalid() {
        let schema = JsonSchema::try_new(serde_json::json!({
            "type": "object",
            "properties": { "n": { "type": "integer" } },
            "required": ["n"],
            "additionalProperties": false
        }))
        .unwrap();
        let err = validate_tool_input(r#"{"n":"nope"}"#, &schema, 1024, 16).unwrap_err();
        assert_eq!(err, InputValidationFailure::SchemaInvalid);
    }
}
