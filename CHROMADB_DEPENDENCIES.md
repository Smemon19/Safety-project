# ChromaDB Dependencies and Impact Analysis

## Critical Workflows That Depend on ChromaDB

### 1. **EM385 Evidence Retrieval** (Collection: `em385_2024`)

- **Used by**: Streamlit UI, AHA generation, CSP generation, evidence-based retrieval
- **Impact**: If collection is missing or empty, workflows will still run but **with degraded or no evidence**
- **Location**: `streamlit_app.py:54-55` hardcodes fallback to `em385_2024` if default is "docs"

### 2. **CSP Document Indexing** (Collection: `csp_documents`)

- **Used by**: CSP generation pipeline
- **Impact**: If collection is missing, documents won't be indexed but pipeline continues
- **Location**: `pipelines/services/defaults.py:191` defaults to `csp_documents`

### 3. **MSF Document Indexing** (Collection: `msf_index`)

- **Used by**: MSF processing, AHA generation with MSF context
- **Impact**: MSF-specific retrieval will fail but fallback to general retrieval works
- **Location**: `scripts/msf_ingest.py:81` defaults to `msf_index`

### 4. **AHA Generation** (Queries multiple collections)

- **Used by**: Activity Hazard Analysis generation
- **Impact**: If collections are empty, AHA will generate but **without citations/evidence**
- **Location**: `generators/aha.py:90-100` queries collection for citations

### 5. **Evidence-Based CSP Sections** (Uses `csp_documents` or `em385_2024`)

- **Used by**: CSP generation with evidence retrieval
- **Impact**: Sections will be generated but **without evidence-backed content**
- **Location**: `generators/csp.py:147-157` conditionally creates evidence generator

## How the Codebase Handles Missing/Empty Collections

### ✅ **Good News: Collections Are Auto-Created**

The codebase uses `get_or_create_collection()` which:

- **Creates empty collections** if they don't exist (no crash)
- **Returns empty results** when querying empty collections (no crash)
- **Allows workflows to continue** with degraded functionality

### ⚠️ **What Happens When Collections Are Empty:**

1. **Query Functions** (`query_collection`, `keyword_search_collection`):

   - Return empty result dictionaries: `{"ids": [[]], "documents": [[]], "metadatas": [[]]}`
   - **No crashes** - workflows continue but with no evidence

2. **AHA Generation**:

   - Generates AHA documents but **without citations**
   - Hazards and controls are still generated from rules/mappings

3. **CSP Generation**:

   - Generates CSP sections but **without evidence-backed content**
   - Falls back to template-based generation

4. **Streamlit UI**:
   - Queries will return "No results found" or empty context
   - Agent responses will be less accurate without evidence

### ❌ **The Real Problem: Collection Name Mismatch**

**If you change collection names, workflows will query the WRONG (or empty) collections:**

1. **Streamlit UI** hardcodes `em385_2024`:

   ```python
   # streamlit_app.py:54-55
   if resolved_collection.strip() == "docs":
       resolved_collection = "em385_2024"  # Hardcoded!
   ```

2. **CSP Pipeline** defaults to `csp_documents`:

   ```python
   # pipelines/services/defaults.py:191
   collection_name = config.get("collection_name", "csp_documents")
   ```

3. **AHA Generation** uses whatever collection name is passed:
   ```python
   # generators/aha.py:96
   col = get_or_create_collection(client, collection_name)
   ```

## Collection Name Resolution Priority

Collection names are resolved in this order (highest to lowest priority):

1. **CLI argument** (e.g., `--collection my_collection`)
2. **Environment variable** `RAG_COLLECTION_NAME`
3. **Default** (hardcoded `"docs"` in code)

### Where Collection Names Are Set:

| Component      | Collection Name | How Set                                  |
| -------------- | --------------- | ---------------------------------------- |
| Streamlit UI   | `em385_2024`    | Hardcoded fallback (line 54-55)          |
| CSP Pipeline   | `csp_documents` | Config default (line 191)                |
| MSF Ingest     | `msf_index`     | Script default (line 81)                 |
| AHA Generation | Variable        | Passed as parameter                      |
| RAG Agent      | Variable        | Resolved via `resolve_collection_name()` |

## What Happens If You Change Collection Names

### Scenario 1: Wrong Collection Name During Reingestion

**Problem**: You reingest EM385 into `em385_new` instead of `em385_2024`

**Impact**:

- Streamlit UI will still query `em385_2024` (empty after reset)
- AHA generation won't find EM385 evidence
- CSP evidence retrieval won't work
- **Workflows run but produce poor results**

### Scenario 2: Collection Name Changed in Code

**Problem**: You change hardcoded `em385_2024` to `em385_2025` in `streamlit_app.py`

**Impact**:

- Streamlit will query `em385_2025` (likely empty)
- If you reingest into `em385_2024`, UI won't find it
- **Workflows will work once you reingest into the new name**

## Safe Reset and Reingestion Strategy

### Step 1: Document Current Collection Names

Before resetting, note where collection names are hardcoded:

```bash
# Check for hardcoded collection names
grep -r "em385_2024\|csp_documents\|msf_index" --include="*.py" .
```

### Step 2: Use Consistent Names During Reingestion

**CRITICAL**: Use the **exact same collection names** as before:

```bash
# EM385 (must match streamlit_app.py expectation)
python insert_docs.py uploads/EM385.pdf --collection em385_2024

# CSP documents (will auto-index during pipeline, but can manually ingest)
# Collection name set in pipeline config: "csp_documents"

# MSF documents
python scripts/msf_ingest.py uploads/MSF.pdf --collection msf_index
```

### Step 3: Verify Collection Names Match

After reingestion, verify:

```python
# Check what collections exist
from utils import get_chroma_client, get_default_chroma_dir
client = get_chroma_client(get_default_chroma_dir())
print([c.name for c in client.list_collections()])
```

### Step 4: Test Workflows

1. **Test Streamlit UI**: Should query `em385_2024` successfully
2. **Test AHA Generation**: Should find citations from EM385
3. **Test CSP Pipeline**: Should index and retrieve evidence

## Configuration Management

### Recommended: Use Environment Variables

Set collection names via environment variables to avoid hardcoding:

```bash
# In .env file or shell
export RAG_COLLECTION_NAME=em385_2024
```

Then update code to use resolved names consistently.

### Better: Centralize Collection Names

Create a configuration file:

```python
# config/collections.py
COLLECTIONS = {
    "em385": "em385_2024",
    "csp": "csp_documents",
    "msf": "msf_index",
}
```

## Summary: Will Your Project Break?

### ✅ **No, it won't completely break:**

- Collections are auto-created if missing
- Empty collections return empty results (no crashes)
- Workflows continue with degraded functionality

### ⚠️ **But it will be degraded:**

- No evidence retrieval → less accurate results
- No citations in AHA documents
- No evidence-backed CSP sections
- Poor quality responses in Streamlit UI

### ❌ **Yes, it WILL break if:**

- You change collection names and don't update all references
- You reingest into wrong collection names
- You change hardcoded collection names inconsistently

## Action Items for Safe Reset

1. ✅ **Document current collection names** (see Step 1 above)
2. ✅ **Use exact same names during reingestion**
3. ✅ **Verify collections after reingestion**
4. ✅ **Test each workflow** to ensure evidence retrieval works
5. ✅ **Consider centralizing collection names** in config for future maintenance
