# ChromaDB Reset and Reingestion Guide

## What Was in Your ChromaDB

Based on your codebase, your ChromaDB likely contained the following collections:

### Collections:

1. **`em385_2024`** - EM 385-1-1 safety documentation (2024 version)

   - Document chunks from EM385 PDF files
   - Used for evidence-based CSP generation
   - Default collection for the Streamlit UI

2. **`csp_documents`** - Construction Safety Plan documents

   - Document chunks from uploaded CSP PDFs/DOCX files
   - Used during CSP generation pipeline

3. **`msf_index`** - MSF (Master Specification File) documents

   - Processed MSF DOCX and PDF files
   - Contains division, section codes, and metadata

4. **`docs`** - General documentation (possibly)
   - Generic collection for various ingested documents

### What Each Collection Contains:

- **Document chunks**: Text split into ~1000-2000 character chunks
- **Metadata**: Source URLs, file paths, headings, page numbers, section codes
- **Embeddings**: Vector representations for semantic search

## Database Corruption Confirmed

The error `range start index 10 out of range for slice of length 9` is a Rust panic from ChromaDB indicating internal database corruption. This can happen due to:

- Unexpected shutdowns during writes
- Disk I/O errors
- Version incompatibilities
- Memory issues

## Resetting ChromaDB

### Option 1: Using the Reset Script (Recommended)

```bash
# Reset with automatic backup
python scripts/reset_chromadb.py

# Reset without backup (faster)
python scripts/reset_chromadb.py --no-backup
```

### Option 2: Manual Reset

```bash
# Backup first (optional but recommended)
cp -r chroma_db chroma_db_backup_$(date +%Y%m%d_%H%M%S)

# Delete the database
rm -rf chroma_db
```

## Reingesting Documents

After resetting, you'll need to reingest your documents. Here's how:

### 1. Reingest EM385 Documents

```bash
# If you have the EM385 PDF in uploads/
python insert_docs.py uploads/EM\ 385-1-1\ _EFFECTIVE\ 15\ March\ 2024.pdf \
    --collection em385_2024 \
    --db-dir ./chroma_db

# Or if you have a text version
python insert_docs.py uploads/EM385_min.txt \
    --collection em385_2024 \
    --db-dir ./chroma_db
```

### 2. Reingest MSF Documents (if applicable)

```bash
# For MSF DOCX files
python scripts/msf_ingest.py uploads/MSF_redacted_ready.pdf \
    --collection msf_index

# Or for DOCX files
python scripts/msf_ingest.py path/to/msf_file.docx \
    --collection msf_index
```

### 3. Reingest via CSP Pipeline

When you run the CSP generation pipeline with documents, it will automatically index them into the `csp_documents` collection. No manual action needed.

### 4. Reingest Other Documents

```bash
# For any PDF or text file
python insert_docs.py path/to/document.pdf \
    --collection your_collection_name \
    --db-dir ./chroma_db

# For URLs
python insert_docs.py https://example.com/docs \
    --collection your_collection_name \
    --db-dir ./chroma_db
```

## Verification

After reingestion, you can verify the database is working:

```bash
# Try the inspection script (should work after reset)
python scripts/inspect_chromadb.py
```

Or test via the Streamlit app:

```bash
python -m streamlit run streamlit_app.py
```

## Important Notes

1. **Backup First**: Always backup before resetting if you might need to recover data
2. **Collection Names**: Make sure to use the same collection names as before (e.g., `em385_2024`) to maintain compatibility
3. **Embedding Model**: The default model is `all-MiniLM-L6-v2`. Use the same model when reingesting to maintain consistency
4. **Reingestion Time**: Depending on document size, reingestion can take several minutes to hours

## Troubleshooting

If you encounter issues during reingestion:

1. **Check disk space**: ChromaDB needs space for vectors and metadata
2. **Check file paths**: Ensure all source files still exist
3. **Check permissions**: Ensure write permissions on the chroma_db directory
4. **Check logs**: Look for error messages during ingestion

## Recovery from Backup

If you need to recover from backup:

```bash
# Stop any running processes using ChromaDB
# Then restore from backup
rm -rf chroma_db
cp -r chroma_db_backup_YYYYMMDD_HHMMSS chroma_db
```

Note: If the backup is also corrupted, you'll need to reingest from source documents.
