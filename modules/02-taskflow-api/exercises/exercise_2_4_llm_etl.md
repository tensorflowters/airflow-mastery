# Exercise 2.4: LLM-Powered Text Extraction Pipeline

Build an ETL pipeline that uses LLM APIs to extract structured information from unstructured text, demonstrating XCom for passing complex data and context for API key management.

## Learning Goals

- Pass complex data structures (embeddings, extracted entities) via XCom
- Use Airflow context to securely access API credentials
- Implement TaskFlow patterns for LLM-powered workflows
- Handle LLM response parsing and error recovery

## Scenario

You're building a pipeline that processes customer feedback documents. The pipeline should:

1. **Extract**: Load raw feedback text from a source
2. **Transform**: Use an LLM to extract structured entities (sentiment, topics, action items)
3. **Load**: Store the extracted data in a structured format

This exercise reinforces TaskFlow concepts while introducing AI/ML patterns.

## Requirements

### Task 1: Secure API Key Access

Create a task that demonstrates secure credential access:

- Use `Variable.get()` for API keys (mock for testing)
- Access connection info from Airflow context
- Never log or expose sensitive credentials

### Task 2: LLM Text Extraction

Create a TaskFlow task that:

- Accepts raw text as input (passed via XCom)
- Calls an LLM API (mock implementation provided)
- Returns structured extraction results
- Handles parsing errors gracefully

### Task 3: Complex XCom Data

Demonstrate passing complex structures:

- Pass extraction results (dicts with nested data)
- Handle list outputs from dynamic operations
- Validate data structure between tasks

### Task 4: Context-Aware Processing

Use Airflow context effectively:

- Access `task_instance` for XCom operations
- Use `dag_run` configuration for runtime parameters
- Log processing metadata with context info

## Success Criteria

- [ ] API keys are never exposed in logs
- [ ] Complex data structures pass correctly between tasks
- [ ] LLM extraction returns structured results
- [ ] Pipeline handles extraction failures gracefully
- [ ] Context is used appropriately for metadata

## Hints

<details>
<summary>Hint 1: Secure Variable Access</summary>

```python
from airflow import Variable


def get_api_key() -> str:
    """Securely retrieve API key."""
    # Use Variable for sensitive data - never hardcode
    api_key = Variable.get("OPENAI_API_KEY", default_var=None)

    if api_key is None:
        # Fallback for testing without real API
        return "mock-api-key-for-testing"

    return api_key
```

</details>

<details>
<summary>Hint 2: Complex XCom Data</summary>

```python
@task
def extract_entities(text: str) -> dict[str, Any]:
    """Extract entities - returns complex nested dict."""
    result = {
        "sentiment": {"label": "positive", "score": 0.85},
        "topics": ["product", "service"],
        "entities": [
            {"type": "PRODUCT", "text": "Widget Pro", "confidence": 0.92},
            {"type": "PERSON", "text": "John", "confidence": 0.88},
        ],
        "action_items": [
            {"priority": "high", "description": "Follow up with customer"},
        ],
    }
    return result  # Automatically serialized to XCom


@task
def process_extraction(extraction: dict[str, Any]) -> dict[str, Any]:
    """Process extraction result passed via XCom."""
    # XCom automatically deserializes the dict
    sentiment = extraction["sentiment"]["label"]
    topic_count = len(extraction["topics"])

    return {
        "summary": f"Found {topic_count} topics with {sentiment} sentiment",
        "requires_followup": len(extraction["action_items"]) > 0,
    }
```

</details>

<details>
<summary>Hint 3: Using Airflow Context</summary>

```python
from airflow.sdk import task


@task
def process_with_context(**context) -> dict[str, Any]:
    """Access Airflow context in TaskFlow."""
    # Access task instance
    ti = context["task_instance"]

    # Get DAG run configuration
    dag_conf = context["dag_run"].conf or {}
    custom_param = dag_conf.get("extraction_model", "default")

    # Access execution date
    execution_date = context["logical_date"]

    # Log with context (don't log sensitive data!)
    logger.info(f"Processing run from {execution_date}")

    return {
        "model_used": custom_param,
        "run_id": context["run_id"],
    }
```

</details>

<details>
<summary>Hint 4: Error Handling in LLM Tasks</summary>

```python
import json


@task
def safe_llm_extraction(text: str) -> dict[str, Any]:
    """LLM extraction with error handling."""
    try:
        # Call LLM (mock or real)
        response = call_llm(text)

        # Parse JSON response
        try:
            result = json.loads(response)
        except json.JSONDecodeError:
            # Fallback for non-JSON responses
            result = {"raw_response": response, "parse_error": True}

        return result

    except Exception as e:
        # Return error structure instead of failing
        return {
            "error": str(e),
            "input_text": text[:100],  # Truncate for safety
            "success": False,
        }
```

</details>

## Files

- **Starter**: `exercise_2_4_llm_etl_starter.py`
- **Solution**: `../solutions/solution_2_4_llm_etl.py`

## Estimated Time

45-60 minutes

## Next Steps

After completing this exercise:

1. Try with a real LLM API (OpenAI, Anthropic)
2. Add retry logic for API failures
3. Implement batching for multiple documents
4. See Module 15 for advanced LLM orchestration patterns
