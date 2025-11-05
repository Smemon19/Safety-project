#!/usr/bin/env python
"""Check collection name dependencies and verify they match expectations."""
import sys
from pathlib import Path
import re

# Add project root to path
project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from utils import get_chroma_client, get_default_chroma_dir, get_default_collection_name, resolve_collection_name

def find_collection_names_in_code():
    """Find all hardcoded collection names in the codebase."""
    patterns = {
        "em385_2024": r"em385_2024",
        "csp_documents": r"csp_documents",
        "msf_index": r"msf_index",
        "docs": r'"docs"|\'docs\'',
    }
    
    results = {}
    codebase_root = project_root
    
    for name, pattern in patterns.items():
        matches = []
        for py_file in codebase_root.rglob("*.py"):
            try:
                content = py_file.read_text(encoding='utf-8')
                for line_num, line in enumerate(content.splitlines(), 1):
                    if re.search(pattern, line):
                        matches.append((str(py_file.relative_to(codebase_root)), line_num, line.strip()))
            except Exception:
                pass
        
        if matches:
            results[name] = matches
    
    return results

def check_chromadb_collections():
    """Check what collections actually exist in ChromaDB."""
    try:
        db_dir = get_default_chroma_dir()
        client = get_chroma_client(db_dir)
        collections = client.list_collections()
        
        return {
            "exists": True,
            "collections": {c.name: c.count() for c in collections},
            "db_dir": db_dir,
        }
    except Exception as e:
        return {
            "exists": False,
            "error": str(e),
            "db_dir": get_default_chroma_dir(),
        }

def main():
    print("=" * 80)
    print("ChromaDB Collection Name Dependency Checker")
    print("=" * 80)
    
    # Check what collections exist
    print("\n1. ChromaDB Status:")
    print("-" * 80)
    db_status = check_chromadb_collections()
    if db_status["exists"]:
        print(f"   Database location: {db_status['db_dir']}")
        if db_status["collections"]:
            print(f"   Found {len(db_status['collections'])} collection(s):")
            for name, count in db_status["collections"].items():
                print(f"     - '{name}': {count} documents")
        else:
            print("   ⚠️  No collections found (database is empty or reset)")
    else:
        print(f"   ✗ Database error: {db_status.get('error', 'Unknown')}")
        print(f"   Location: {db_status['db_dir']}")
    
    # Check code references
    print("\n2. Hardcoded Collection Names in Code:")
    print("-" * 80)
    code_refs = find_collection_names_in_code()
    
    expected_collections = {
        "em385_2024": "Expected by Streamlit UI (line 54-55 in streamlit_app.py)",
        "csp_documents": "Default for CSP pipeline (pipelines/services/defaults.py:191)",
        "msf_index": "Default for MSF ingestion (scripts/msf_ingest.py:81)",
    }
    
    for coll_name, description in expected_collections.items():
        print(f"\n   '{coll_name}':")
        print(f"     {description}")
        if coll_name in code_refs:
            print(f"     Found in {len(code_refs[coll_name])} location(s):")
            for file_path, line_num, code_line in code_refs[coll_name][:5]:  # Show first 5
                print(f"       - {file_path}:{line_num}")
                if len(code_line) > 80:
                    code_line = code_line[:77] + "..."
                print(f"         {code_line}")
            if len(code_refs[coll_name]) > 5:
                print(f"       ... and {len(code_refs[coll_name]) - 5} more")
        else:
            print(f"     ⚠️  Not found in codebase (may be set via env/config)")
    
    # Check environment variable
    print("\n3. Environment Configuration:")
    print("-" * 80)
    default_name = get_default_collection_name()
    resolved_name = resolve_collection_name(None)
    print(f"   Default collection name: '{default_name}'")
    print(f"   Resolved collection name: '{resolved_name}'")
    if default_name != "docs":
        print(f"   ℹ️  RAG_COLLECTION_NAME is set to '{default_name}'")
    
    # Verify expectations vs reality
    print("\n4. Verification:")
    print("-" * 80)
    if db_status["exists"]:
        existing = set(db_status["collections"].keys())
        expected = set(expected_collections.keys())
        
        missing = expected - existing
        extra = existing - expected
        
        if missing:
            print(f"   ⚠️  Missing expected collections: {', '.join(missing)}")
            print(f"      These workflows will query empty collections:")
            for name in missing:
                print(f"        - '{name}': {expected_collections.get(name, 'Unknown usage')}")
        
        if extra:
            print(f"   ℹ️  Extra collections found: {', '.join(extra)}")
            print(f"      These may not be used by current workflows")
        
        if not missing and not extra:
            print("   ✓ All expected collections exist")
    else:
        print("   ⚠️  Cannot verify - database not accessible")
    
    print("\n" + "=" * 80)
    print("Recommendations:")
    print("=" * 80)
    
    if not db_status["exists"] or not db_status.get("collections"):
        print("1. Database is empty or reset. Reingest documents using:")
        print("   - EM385: python insert_docs.py <file> --collection em385_2024")
        print("   - MSF:   python scripts/msf_ingest.py <file> --collection msf_index")
        print("   - CSP:   Will auto-index during pipeline run")
    else:
        missing = set(expected_collections.keys()) - set(db_status.get("collections", {}).keys())
        if missing:
            print(f"1. Missing collections: {', '.join(missing)}")
            print("   Reingest documents into these collections")
        else:
            print("1. All expected collections exist ✓")
    
    print("2. Verify collection names match hardcoded expectations in code")
    print("3. Test workflows after reingestion to ensure evidence retrieval works")
    print("\nSee CHROMADB_DEPENDENCIES.md for detailed impact analysis")

if __name__ == "__main__":
    main()

