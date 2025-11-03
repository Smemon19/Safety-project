from __future__ import annotations

def test_basic_imports():
    # Smoke tests that modules load
    import generators.aha as aha  # noqa: F401
    import generators.csp as csp  # noqa: F401

def test_clean_text_block_removes_boilerplate():
    from generators.aha import _clean_text_block
    raw = """
    DEPARTMENT OF THE ARMY EM 385-1-1
    [OCR Merge] 
    Safety and Occupational Health Requirements
    This is a useful sentence about PPE and controls.
    """.strip()
    out = _clean_text_block(raw)
    assert "useful sentence" in out.lower()
    assert "department of the army" not in out.lower()
    assert "ocr" not in out.lower()


