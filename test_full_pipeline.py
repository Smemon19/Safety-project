#!/usr/bin/env python3
"""Full end-to-end test of Section 11 pipeline with actual document."""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent))

from section11.pipeline import run_pipeline
from section11.parser import parse_document_text, parse_spec
from section11.rag_code_extractor import extract_codes_with_rag, check_codes_against_firebase, verify_document_parsing
from utils import resolve_collection_name

def test_full_pipeline():
    """Test the complete pipeline with the test document."""
    print("=" * 70)
    print("FULL PIPELINE END-TO-END TEST")
    print("=" * 70)
    
    # Path to test document
    test_doc = Path(__file__).parent / "test_documents" / "AHA_Test_Spec_Document.docx"
    
    if not test_doc.exists():
        print(f"ERROR: Test document not found at {test_doc}")
        return False
    
    print(f"\n📄 Test Document: {test_doc.name}")
    print(f"   Size: {test_doc.stat().st_size} bytes")
    
    # Step 1: Parse document
    print("\n" + "=" * 70)
    print("STEP 1: Parsing Document")
    print("=" * 70)
    
    try:
        document_text = parse_document_text(test_doc)
        print(f"✅ Document text extracted: {len(document_text)} characters")
        
        # Verify parsing
        verification = verify_document_parsing(document_text, [])
        print(f"   Document length: {verification['document_length']} chars")
        print(f"   Has content: {verification['has_content']}")
        if verification['issues']:
            print(f"   ⚠️  Issues: {verification['issues']}")
        else:
            print(f"   ✅ No parsing issues")
        
        # Show first 500 chars
        print(f"\n   First 500 characters:")
        print(f"   {document_text[:500]}...")
        
    except Exception as e:
        print(f"❌ ERROR parsing document: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Step 2: Extract codes
    print("\n" + "=" * 70)
    print("STEP 2: Extracting Codes")
    print("=" * 70)
    
    try:
        # Parse using full parser
        work_dir = Path("/tmp/test_section11_full")
        work_dir.mkdir(exist_ok=True)
        
        parsed = parse_spec(test_doc, work_dir)
        regex_codes = [c.code for c in parsed.codes]
        print(f"✅ Regex extraction found {len(regex_codes)} codes: {regex_codes}")
        
        # RAG extraction
        collection_name = resolve_collection_name(None)
        if collection_name.strip() == "docs":
            collection_name = "em385_2024"
        rag_codes = extract_codes_with_rag(document_text, collection_name)
        print(f"✅ RAG extraction found {len(rag_codes)} codes: {rag_codes}")
        
        all_codes = list(set(regex_codes + rag_codes))
        print(f"✅ Total unique codes: {len(all_codes)} - {all_codes}")
        
        if len(all_codes) == 0:
            print("⚠️  WARNING: No codes found in document!")
        
    except Exception as e:
        print(f"❌ ERROR extracting codes: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Step 3: Check Firebase
    print("\n" + "=" * 70)
    print("STEP 3: Checking Codes Against Firebase")
    print("=" * 70)
    
    try:
        firebase_results = check_codes_against_firebase(all_codes)
        print(f"✅ Firebase check complete:")
        print(f"   Codes requiring AHA: {len(firebase_results['codes_requiring_aha'])}")
        print(f"   Codes NOT requiring: {len(firebase_results['codes_not_requiring'])}")
        print(f"   Codes unknown: {len(firebase_results['codes_unknown'])}")
        
        if firebase_results['codes_requiring_aha']:
            print(f"   ✅ Codes requiring AHA: {firebase_results['codes_requiring_aha']}")
        if firebase_results['codes_not_requiring']:
            print(f"   - Codes NOT requiring: {firebase_results['codes_not_requiring'][:5]}")
        if firebase_results['codes_unknown']:
            print(f"   ? Codes unknown: {firebase_results['codes_unknown'][:5]}")
        
    except Exception as e:
        print(f"❌ ERROR checking Firebase: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Step 4: Run full pipeline
    print("\n" + "=" * 70)
    print("STEP 4: Running Full Pipeline")
    print("=" * 70)
    
    try:
        print(f"   Using collection: {collection_name}")
        print(f"   This may take 2-3 minutes...")
        
        run = run_pipeline(
            source_path=test_doc,
            collection_name=collection_name,
            overrides={},
            upload_artifacts=False,
        )
        
        print(f"\n✅ Pipeline completed successfully!")
        print(f"   Run ID: {run.run_id}")
        print(f"   Codes processed: {len(run.parsed.codes)}")
        print(f"   Categories created: {len(run.bundles)}")
        print(f"   Matrix rows: {len(run.matrix)}")
        
        # Show results
        print(f"\n📊 Results Summary:")
        for bundle in run.bundles:
            if bundle.codes:
                print(f"   Category: {bundle.category}")
                print(f"     Codes: {bundle.codes}")
                print(f"     AHA Status: {bundle.aha.status.value}")
                print(f"     Plan Status: {bundle.plan.status.value}")
                print(f"     Hazards: {len(bundle.aha.hazards)}")
                print(f"     Controls: {len(bundle.plan.controls)}")
                print()
        
        # Check artifacts
        if run.artifacts.markdown_path.exists():
            print(f"✅ Artifacts generated:")
            print(f"   Markdown: {run.artifacts.markdown_path}")
            print(f"   DOCX: {run.artifacts.docx_path}")
            print(f"   JSON: {run.artifacts.json_report_path}")
            
            # Show markdown preview
            markdown_content = run.artifacts.markdown_path.read_text(encoding="utf-8")
            print(f"\n📄 Markdown preview (first 500 chars):")
            print(markdown_content[:500])
        
        return True
        
    except Exception as e:
        print(f"❌ ERROR in pipeline: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_full_pipeline()
    print("\n" + "=" * 70)
    if success:
        print("✅ ALL TESTS PASSED - Pipeline is working correctly!")
    else:
        print("❌ TESTS FAILED - Check errors above")
    print("=" * 70)
    sys.exit(0 if success else 1)

