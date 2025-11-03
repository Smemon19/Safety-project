from dotenv import load_dotenv
import streamlit as st
import asyncio
import json
import os
from pathlib import Path
from typing import Optional

# Import all the message part classes
from pydantic_ai.messages import (
    ModelMessage,
    ModelRequest,
    ModelResponse,
    SystemPromptPart,
    UserPromptPart,
    TextPart,
    ToolCallPart,
    ToolReturnPart,
    RetryPromptPart,
    ModelMessagesTypeAdapter
)
from pydantic_ai.exceptions import ModelHTTPError

# Resolve and load .env from writable AppData directory; create scaffold on first run
from utils import get_chroma_client, resolve_embedding_backend_and_model, get_env_file_path, ensure_appdata_scaffold, get_default_chroma_dir, resolve_collection_name
from config import log_active_config, get_namespace

ensure_appdata_scaffold()
# Load appdata .env first; then load repo .env with override=True so repo wins
load_dotenv(dotenv_path=get_env_file_path(), override=False)
load_dotenv(override=True)
log_active_config(prefix="[ui-config]", allow_missing=False)

# Reduce file watcher issues with certain heavy libraries (torch, etc.)
try:
    st.set_option("server.fileWatcherType", "none")
except Exception:
    pass

from rag_agent import get_agent, RAGDeps
from generators.analyze import analyze_scope
from generators.aha import generate_full_aha
from generators.csp import generate_csp
from export.docx_writer import write_aha_book, write_aha_single, write_csp_docx
from export.html_writer import write_aha_book_html, write_csp_html, write_aha_single_html
from export.markdown_writer import write_aha_book_md, write_csp_md, write_aha_single_md
from pipelines.csp_pipeline import DocumentSourceChoice, MetadataSourceChoice, ValidationError
from pipelines.decision_providers import StreamlitDecisionProvider
from pipelines.runtime import build_pipeline, generate_run_id

async def get_agent_deps(header_contains: Optional[str], source_contains: Optional[str]):
    resolved_collection = resolve_collection_name(None)
    # Prefer the freshly ingested EM385 collection by default for the UI
    if resolved_collection.strip() == "docs":
        resolved_collection = "em385_2024"
    # Log once on startup via Streamlit status text and server log
    print(f"[ui] Using ChromaDB collection: '{resolved_collection}'")
    st.sidebar.caption(f"Active collection: {resolved_collection}")
    backend, model = resolve_embedding_backend_and_model()
    # Also display embeddings info once
    st.sidebar.caption(f"Embeddings: {backend} / {model}")
    ns = get_namespace()
    if ns:
        st.sidebar.caption(f"Namespace: {ns}")
    return RAGDeps(
        chroma_client=get_chroma_client(get_default_chroma_dir()),
        collection_name=resolved_collection,
        embedding_model="all-MiniLM-L6-v2",
        header_contains=(header_contains or None),
        source_contains=(source_contains or None),
    )


def display_message_part(part):
    """
    Display a single part of a message in the Streamlit UI.
    Customize how you display system prompts, user prompts,
    tool calls, tool returns, etc.
    """
    # user-prompt
    if part.part_kind == 'user-prompt':
        with st.chat_message("user"):
            st.markdown(part.content)
    # text
    elif part.part_kind == 'text':
        with st.chat_message("assistant"):
            st.markdown(part.content)
    elif part.part_kind == 'tool-return':
        # Enhance display if metadata is present in context (non-breaking; retrieve format unchanged)
        payload = getattr(part, 'content', None)
        if isinstance(payload, dict):
            # Best-effort display of title and section_path if present
            title = payload.get('title')
            section_path = payload.get('section_path')
            source_url = payload.get('source_url')
            if title or section_path or source_url:
                with st.chat_message("assistant"):
                    if title:
                        st.markdown(f"**{title}**")
                    if section_path:
                        st.caption(section_path)
                    if source_url and source_url.startswith('http'):
                        st.markdown(f"[Source]({source_url})")
                    elif source_url and source_url.startswith('file://'):
                        st.caption(source_url.replace('file://', ''))

async def run_agent_with_streaming(user_input):
    try:
        async with get_agent().run_stream(
            user_input, deps=st.session_state.agent_deps, message_history=st.session_state.messages
        ) as result:
            async for message in result.stream_text(delta=True):
                yield message

        # Add the new messages to the chat history (including tool calls and responses)
        st.session_state.messages.extend(result.new_messages())
    except ModelHTTPError as e:
        # Friendly message for invalid/missing API keys or HTTP errors
        err = str(getattr(e, 'status_code', ''))
        if '401' in err or getattr(e, 'status_code', None) == 401:
            yield "OpenAI authentication failed (401). Please set a valid OPENAI_API_KEY in your .env and restart."
        else:
            yield f"Model request failed ({getattr(e, 'status_code', 'error')}). Please try again later."
    except Exception as e:
        yield "An unexpected error occurred while contacting the model. Check logs and your .env configuration."


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~ Main Function with UI Creation ~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

async def main():
    st.title("CAL AI Agent")
    # Masked key tail for debugging which env is active
    try:
        k = os.getenv("OPENAI_API_KEY", "")
        if k:
            st.sidebar.caption(f"OPENAI key tail: …{k[-4:]}")
        else:
            st.sidebar.caption("OPENAI key not set")
    except Exception:
        pass

    # Initialize chat history in session state if not present
    if "messages" not in st.session_state:
        st.session_state.messages = []
    # Sidebar controls: collection display + optional filters
    # Preserve last values via session_state keys
    if "header_contains" not in st.session_state:
        st.session_state.header_contains = ""
    if "source_contains" not in st.session_state:
        st.session_state.source_contains = ""
    if "last_msf_doc_id" not in st.session_state:
        st.session_state.last_msf_doc_id = None

    # Filters (collection is fixed to docs_ibc_v2)
    st.sidebar.markdown("### Retrieval Filters")
    st.sidebar.text_input("Header contains", key="header_contains", placeholder="e.g., Section 1507")
    st.sidebar.text_input("Source contains", key="source_contains", placeholder="e.g., pydantic.dev")

    # Recreate deps each render so filters are applied
    st.session_state.agent_deps = await get_agent_deps(
        st.session_state.header_contains.strip() or None,
        st.session_state.source_contains.strip() or None,
    )

    # Show active collection and filters summary
    st.sidebar.markdown(f"**Collection:** {st.session_state.agent_deps.collection_name}")
    if st.session_state.header_contains or st.session_state.source_contains:
        st.sidebar.caption(
            f"Filters: header='{st.session_state.header_contains or ''}', source='{st.session_state.source_contains or ''}'"
        )
    active_msf = st.session_state.get("last_msf_doc_id")
    st.sidebar.caption(f"MSF doc id: {active_msf}" if active_msf else "MSF doc id: none")

    st.subheader("ENG Form 6293 CSP Pipeline")

    st.subheader("Quick CSP Generator")
    uploaded_files = st.file_uploader(
        "Upload project documents",
        type=["pdf", "docx", "txt"],
        accept_multiple_files=True,
        key="csp_pipeline_uploads",
    )

    pipeline_manual_metadata: dict[str, str] = {}
    existing_paths_input: str = ""
    allow_placeholders = True
    use_existing_docs = False
    metadata_choice = MetadataSourceChoice.FILE

    with st.expander("Advanced options", expanded=False):
        use_existing_docs = st.checkbox(
            "Use previously ingested document paths",
            value=False,
            key="csp_use_existing_docs",
        )
        if use_existing_docs:
            existing_paths_input = st.text_area(
                "Existing document paths (one per line)",
                value="\n".join(st.session_state.get("csp_existing_paths", [])),
                key="csp_existing_paths_input",
            )

        metadata_mode = st.selectbox(
            "Metadata source",
            (
                "Auto (extract from uploaded documents)",
            ),
            index=0,
            key="csp_metadata_choice",
        )
        st.caption("📄 All metadata will be automatically extracted from your uploaded documents. No manual entry needed!")

        # Always extract from files - automatic extraction only
        allow_placeholders = True  # Allow as fallback if extraction fails (validation will still catch missing fields)
        metadata_choice = MetadataSourceChoice.FILE
        pipeline_manual_metadata = {}  # No manual entry needed

    generate_clicked = st.button("Generate CSP", type="primary")

    if generate_clicked:
        run_id = generate_run_id("csp-ui")

        document_choice = DocumentSourceChoice.PLACEHOLDER
        upload_paths: list[str] = []
        existing_paths: list[str] = []

        if uploaded_files:
            document_choice = DocumentSourceChoice.UPLOAD
            upload_dir = Path("outputs/uploads/csp_pipeline") / run_id
            upload_dir.mkdir(parents=True, exist_ok=True)
            for upload in uploaded_files:
                dest = upload_dir / upload.name
                dest.write_bytes(upload.getbuffer())
                upload_paths.append(str(dest.resolve()))
        elif use_existing_docs and existing_paths_input:
            document_choice = DocumentSourceChoice.EXISTING
            existing_paths = [line.strip() for line in existing_paths_input.splitlines() if line.strip()]
            st.session_state.csp_existing_paths = existing_paths

        provider = StreamlitDecisionProvider(
            document_choice=document_choice,
            metadata_choice=metadata_choice,
            upload_paths=upload_paths,
            metadata_overrides=pipeline_manual_metadata,
            allow_placeholders=allow_placeholders,
        )

        # Determine collection name for evidence-based generation
        collection_name = st.session_state.get("collection_name") or "csp_documents"
        
        config = {
            "existing_document_paths": existing_paths,
            "output_dir": str(Path("outputs/Compiled_CSP_Final").resolve()),
            "run_mode": "streamlit",
            "collection_name": collection_name,
            "use_evidence_based_generation": True,  # Enable evidence-based generation
        }

        try:
            pipeline = build_pipeline(
                decision_provider=provider,
                config=config,
                run_id=run_id,
            )
            with st.spinner("Running CSP pipeline…"):
                result = pipeline.run()
            
            # Show extracted metadata
            extracted_metadata = result.metadata.data
            if extracted_metadata:
                with st.expander("📋 Extracted Metadata", expanded=True):
                    metadata_cols = st.columns(3)
                    with metadata_cols[0]:
                        if extracted_metadata.get("project_name"):
                            st.metric("Project Name", extracted_metadata["project_name"])
                        if extracted_metadata.get("location"):
                            st.metric("Location", extracted_metadata["location"])
                    with metadata_cols[1]:
                        if extracted_metadata.get("owner"):
                            st.metric("Owner", extracted_metadata["owner"])
                        if extracted_metadata.get("prime_contractor"):
                            st.metric("Prime Contractor", extracted_metadata["prime_contractor"])
                    with metadata_cols[2]:
                        if extracted_metadata.get("project_manager"):
                            st.metric("Project Manager", extracted_metadata["project_manager"])
                        if extracted_metadata.get("ssho"):
                            st.metric("SSHO", extracted_metadata["ssho"])
            
            st.success("✅ CSP pipeline completed successfully!")
            st.json(
                {
                    "run_id": run_id,
                    "documents": result.ingestion.documents,
                    "metadata_source": result.metadata.source.value,
                    "metadata_extracted": {k: v for k, v in extracted_metadata.items() if v},
                    "warnings": result.validation.warnings,
                    "outputs": {
                        "docx": result.outputs.docx_path,
                        "pdf": result.outputs.pdf_path,
                        "manifest": result.outputs.manifest_path,
                    },
                }
            )

            if result.validation.warnings:
                st.warning("**Warnings:**\n\n" + "\n".join(f"- {w}" for w in result.validation.warnings))
            
            # Show validation errors if any (though these should block export)
            if result.validation.errors:
                st.error("**Validation Errors:**\n\n" + "\n".join(f"- {e}" for e in result.validation.errors))

            downloads = st.container()
            with downloads:
                docx_path = result.outputs.docx_path
                pdf_path = result.outputs.pdf_path
                manifest_path = result.outputs.manifest_path
                package_path = result.outputs.extra.get("package_path")

                if docx_path and Path(docx_path).exists():
                    with open(docx_path, "rb") as fh:
                        st.download_button(
                            label="Download CSP (DOCX)",
                            data=fh.read(),
                            file_name=Path(docx_path).name,
                            mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                            key="download_csp_docx",
                        )
                if pdf_path and Path(pdf_path).exists():
                    with open(pdf_path, "rb") as fh:
                        st.download_button(
                            label="Download CSP (PDF)",
                            data=fh.read(),
                            file_name=Path(pdf_path).name,
                            mime="application/pdf",
                            key="download_csp_pdf",
                        )
                if manifest_path and Path(manifest_path).exists():
                    with open(manifest_path, "rb") as fh:
                        st.download_button(
                            label="Download manifest.json",
                            data=fh.read(),
                            file_name=Path(manifest_path).name,
                            mime="application/json",
                            key="download_manifest",
                        )
                if package_path and Path(package_path).exists():
                    with open(package_path, "rb") as fh:
                        st.download_button(
                            label="Download CSP package (.zip)",
                            data=fh.read(),
                            file_name=Path(package_path).name,
                            mime="application/zip",
                            key="download_csp_package",
                        )
        except ValidationError as exc:
            error_msg = str(exc)
            st.error(f"**Validation Failed - Export Blocked**\n\n{error_msg}")
            
            # Extract missing fields from error message
            missing_fields = []
            if "project_name" in error_msg:
                missing_fields.append("Project Name")
            if "location" in error_msg:
                missing_fields.append("Location")
            if "owner" in error_msg:
                missing_fields.append("Owner")
            if "prime_contractor" in error_msg:
                missing_fields.append("Prime Contractor")
            
            if missing_fields:
                st.warning(
                    f"**Missing Required Fields:** {', '.join(missing_fields)}\n\n"
                    "Please use 'Manual entry' mode to fill these in, or ensure your uploaded documents "
                    "contain these fields in a recognizable format."
                )
            
            st.info(
                "💡 **How to fix:**\n\n"
                "The system couldn't automatically extract all required metadata from your document. "
                "This usually means the information is present but in a format that wasn't recognized.\n\n"
                "**Options:**\n"
                "1. **Check your document** - Ensure it contains clear labels like:\n"
                "   - 'Project Name: [name]' or 'Project: [name]'\n"
                "   - 'Location: [location]'\n"
                "   - 'Owner: [owner]'\n"
                "   - 'Prime Contractor: [contractor]' or 'General Contractor: [contractor]'\n"
                "   - 'Project Manager: [name]' or 'PM: [name]'\n"
                "   - 'SSHO: [name]' or 'Site Safety and Health Officer: [name]'\n\n"
                "2. **Re-upload** with the metadata clearly labeled in the first few pages\n\n"
                "3. **Check extraction logs** in the diagnostics directory for details"
            )
        except Exception as exc:  # pragma: no cover - defensive UI handling
            st.error(f"**Pipeline Error**\n\n{str(exc)}")
            import traceback
            with st.expander("Technical details"):
                st.code(traceback.format_exc())

    # Upload and process a design/spec document
    st.subheader("Process a Design Spec")
    uploaded = st.file_uploader("Upload spec (.pdf, .docx, .txt, .spec, .sec)", type=["pdf", "docx", "txt", "spec", "sec"], accept_multiple_files=False)
    if uploaded is not None:
        # Save to a temp path under outputs/uploads
        tmp_dir = Path("outputs/uploads")
        tmp_dir.mkdir(parents=True, exist_ok=True)
        tmp_path = tmp_dir / uploaded.name
        tmp_path.write_bytes(uploaded.getbuffer())
        if st.button("Process", type="primary", key="btn_process_spec"):
            from scripts.process_design_spec import process_design_spec
            with st.status("Processing document…", expanded=True) as status:
                try:
                    res = process_design_spec(
                        str(tmp_path),
                        collection_name=st.session_state.agent_deps.collection_name,
                        ocr_threshold=100,
                        classify_only=False,
                        aha_mode="code",
                        include_admin_ufgs=True,
                        msf_doc_id=st.session_state.get("last_msf_doc_id"),
                    )
                    status.update(label="Processing complete", state="complete")
                    st.success("Done. Outputs below.")
                    # Display downloads similar to quick builder
                    run_dir = Path("outputs/runs") / res.run_id
                    # AHA Book and CSP if present in manifest
                    try:
                        manifest = json.loads((run_dir / "manifest.json").read_text(encoding="utf-8"))
                    except Exception:
                        manifest = {}
                    # Persist outputs in session so clicks/reruns don't clear the UI
                    st.session_state.process_outputs = {
                        "run_id": res.run_id,
                        "run_dir": str(run_dir),
                        "manifest": str(run_dir / "manifest.json"),
                        "aha_files": list(res.aha_files or []),
                        "aha_markdown_files": list(getattr(res, "aha_markdown_files", []) or []),
                        "aha_book_docx": manifest.get("aha_book_docx", str(run_dir / "AHA_Book.docx")),
                        "aha_book_md": manifest.get("aha_book_md", str(run_dir / "AHA_Book.md")),
                        "csp_docx": manifest.get("csp_docx", str(run_dir / "CSP.docx")),
                        "csp_md": manifest.get("csp_md", str(run_dir / "CSP.md")),
                        "project_meta": manifest.get("project_meta", {}),
                        "warnings": manifest.get("warnings", []),
                        "msf_doc_id": manifest.get("msf_doc_id", getattr(res, "msf_doc_id", None)),
                        "auto_classified_codes": manifest.get("auto_classified_codes", []),
                        "code_decisions": manifest.get("code_decisions", list(getattr(res, "code_decisions", []))),
                    }
                except Exception as e:
                    status.update(label="Processing failed", state="error")
                    st.error(f"Processing error: {e}")

    # Optional: Ingest MSF into index for project-grounded retrieval (no expander to avoid nesting issues)
    st.subheader("Ingest MSF (.docx or .pdf) into Index")
    msf_up = st.file_uploader("Upload MSF file", type=["docx", "pdf"], accept_multiple_files=False, key="msf_doc")
    if msf_up is not None:
        msf_dir = Path("outputs/uploads/msf")
        msf_dir.mkdir(parents=True, exist_ok=True)
        msf_path = msf_dir / msf_up.name
        msf_path.write_bytes(msf_up.getbuffer())
        if st.button("Ingest MSF", key="btn_ingest_msf", type="primary"):
            try:
                from scripts.msf_ingest import ingest_msf_docx, ingest_msf_pdf
                with st.status("Indexing MSF…", expanded=True) as s:
                    if msf_path.suffix.lower() == ".pdf":
                        n = ingest_msf_pdf(str(msf_path), collection_name="msf_index", doc_id=msf_path.stem)
                    else:
                        n = ingest_msf_docx(str(msf_path), collection_name="msf_index", doc_id=msf_path.stem)
                    s.update(label=f"Indexed {n} chunks to msf_index", state="complete")
                    st.success("MSF ingestion complete.")
                    st.session_state.last_msf_doc_id = msf_path.stem
            except Exception as e:
                st.error(f"MSF ingestion failed: {e}")

    # Quick builder for CSP & AHAs
    with st.expander("Exports (optional): Build CSP & AHAs from Scope"):
        scope_text = st.text_area("Paste scope text or JSON", key="scope_text", height=180, placeholder="Paste your scope of work here (text or JSON)")
        default_collection = getattr(st.session_state.agent_deps, "collection_name", "docs")
        collection_input = st.text_input("Target collection", value=default_collection, key="collection_input")
        if st.button("Generate CSP & AHAs", type="primary", key="btn_generate_exports"):
            if not scope_text.strip():
                st.warning("Please paste a scope first.")
            else:
                # Direct pipeline for deterministic outputs + download buttons
                with st.status("Generating documents…", expanded=True) as status:
                    try:
                        st.write("Analyzing scope…")
                        analysis = analyze_scope(scope_text)
                        activities = analysis.get("activities", [])
                        if not activities:
                            st.error("No activities detected in the scope. Please add more detail.")
                        else:
                            st.write(f"Detected activities: {', '.join(activities)}")
                            # Generate AHAs
                            st.write("Generating AHAs…")
                            ahas = [generate_full_aha(a, collection_input) for a in activities]
                            # Write AHA Book
                            book_docx = write_aha_book(ahas, "outputs/AHA_Book.docx")
                            book_html = write_aha_book_html(ahas, "outputs/AHA_Book.html")
                            book_md = write_aha_book_md(ahas, "outputs/AHA_Book.md")
                            # Per-activity AHAs disabled
                            # Generate CSP
                            st.write("Generating CSP…")
                            import json
                            try:
                                spec = json.loads(scope_text)
                            except Exception:
                                spec = {"project_name": "Project", "project_number": "", "location": "", "owner": "", "gc": "", "work_packages": [], "deliverables": [], "assumptions": []}
                            csp = generate_csp(spec, collection_input)
                            csp_docx = write_csp_docx(csp, "outputs/CSP.docx")
                            csp_html = write_csp_html(csp, "outputs/CSP.html")
                            csp_md = write_csp_md(csp, "outputs/CSP.md")

                            # Persist outputs so downloads survive reruns (e.g., after a click)
                            st.session_state.build_outputs = {
                                "book": {"docx": book_docx, "html": book_html, "md": book_md},
                                "csp": {"docx": csp_docx, "html": csp_html, "md": csp_md},
                                "per_files": [],
                            }
                            status.update(label="Generation complete. See Downloads section below.", state="complete")
                    except Exception as e:
                        status.update(label="Generation failed", state="error")
                        st.error(f"Generation error: {e}")

    # Persistent Downloads section (survives reruns triggered by download buttons)
    outputs = st.session_state.get("build_outputs")
    if outputs:
        st.subheader("Downloads")
        def add_download(path: str, label: str, key_suffix: str):
            try:
                if path.endswith(".docx"):
                    data = open(path, "rb").read()
                    st.download_button(label=label, data=data, file_name=os.path.basename(path), mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document", key=f"dl_{key_suffix}")
                else:
                    data = open(path, "rb").read()
                    mime = "text/html" if path.endswith(".html") else "text/markdown"
                    st.download_button(label=label, data=data, file_name=os.path.basename(path), mime=mime, key=f"dl_{key_suffix}")
            except Exception:
                st.caption(f"(File not found) {path}")

        import os
        st.markdown("**AHA Book**")
        add_download(outputs["book"]["docx"], "Download AHA_Book.docx", "book_docx")
        add_download(outputs["book"]["html"], "Download AHA_Book.html", "book_html")
        add_download(outputs["book"]["md"], "Download AHA_Book.md", "book_md")

        st.markdown("**CSP**")
        add_download(outputs["csp"]["docx"], "Download CSP.docx", "csp_docx")
        add_download(outputs["csp"]["html"], "Download CSP.html", "csp_html")
        add_download(outputs["csp"]["md"], "Download CSP.md", "csp_md")

        # Per‑Activity AHAs disabled

    # Persistent Downloads for processed upload
    processed = st.session_state.get("process_outputs")
    if processed:
        st.subheader("Processed Spec Downloads")
        st.caption(processed.get("run_dir", ""))
        if processed.get("msf_doc_id"):
            st.caption(f"MSF doc id: {processed['msf_doc_id']}")
        meta = processed.get("project_meta", {})
        if meta:
            st.markdown("**Project**")
            st.write(
                f"{meta.get('project_name','Project')} — {meta.get('project_number','')}\n\n"
                f"Location: {meta.get('location','')}\n\n"
                f"Owner: {meta.get('owner','')} | GC: {meta.get('gc','')}"
            )
        warns = processed.get("warnings", [])
        if warns:
            st.warning("\n".join(warns))
        auto_classified = processed.get("auto_classified_codes", []) or []
        if auto_classified:
            st.info(
                "The system auto-classified the following codes for AHA generation (review recommended):\n" +
                "\n".join(auto_classified)
            )
        code_summary = processed.get("code_decisions", []) or []
        if code_summary:
            st.markdown("**Code Decisions Summary**")
            table_rows = [
                {
                    "Code": item.get("code", ""),
                    "Requires AHA": item.get("requires_aha"),
                    "Source": item.get("decision_source", ""),
                    "Activity": item.get("activity", ""),
                    "Activity Source": item.get("activity_source", ""),
                    "AHA Generated": item.get("aha_generated"),
                    "Confidence": item.get("confidence"),
                }
                for item in code_summary
            ]
            st.table(table_rows)
            rationales = [item for item in code_summary if item.get("rationale")]
            if rationales:
                with st.expander("Decision Rationales"):
                    st.markdown("\n\n".join(f"**{item.get('code', '')}:** {item.get('rationale', '')}" for item in rationales))
        # AHA Book
        for key in ["aha_book_docx", "aha_book_md"]:
            p = processed.get(key)
            if p and Path(p).exists():
                data = open(p, "rb").read()
                label = f"Download {Path(p).name}"
                mime = "application/vnd.openxmlformats-officedocument.wordprocessingml.document" if p.endswith(".docx") else "text/markdown"
                st.download_button(label=label, data=data, file_name=Path(p).name, mime=mime, key=f"dl_proc_{Path(p).name}")
        # CSP
        for key in ["csp_docx", "csp_md"]:
            p = processed.get(key)
            if p and Path(p).exists():
                data = open(p, "rb").read()
                label = f"Download {Path(p).name}"
                mime = "application/vnd.openxmlformats-officedocument.wordprocessingml.document" if p.endswith(".docx") else "text/markdown"
                st.download_button(label=label, data=data, file_name=Path(p).name, mime=mime, key=f"dl_proc_{Path(p).name}")
        # Individual AHAs
        aha_docx_files = processed.get("aha_files", []) or []
        aha_md_files = processed.get("aha_markdown_files", []) or []
        if aha_docx_files or aha_md_files:
            st.markdown("**Activity Hazard Analyses**")
            for idx, p in enumerate(aha_docx_files):
                if p and Path(p).exists():
                    data = open(p, "rb").read()
                    label = f"Download {Path(p).name}"
                    st.download_button(
                        label=label,
                        data=data,
                        file_name=Path(p).name,
                        mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                        key=f"dl_proc_aha_docx_{idx}"
                    )
            for idx, p in enumerate(aha_md_files):
                if p and Path(p).exists():
                    data = open(p, "rb").read()
                    st.download_button(
                        label=f"Download {Path(p).name}",
                        data=data,
                        file_name=Path(p).name,
                        mime="text/markdown",
                        key=f"dl_proc_aha_md_{idx}"
                    )
        else:
            st.caption("No individual AHA files generated yet.")

    # Simple run history (last 10)
    with st.expander("Run History"):
        try:
            from scripts.report_counts import _project_root  # reuse root
            import firebase_admin
            from firebase_admin import credentials, firestore
            creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or os.path.join(_project_root(), "firebase-admin.json")
            try:
                firebase_admin.get_app()
            except ValueError:
                firebase_admin.initialize_app(credentials.Certificate(creds_path))
            db = firestore.client()
            docs = list(db.collection("runs").order_by("created_at", direction=firestore.Query.DESCENDING).limit(10).stream())
            for d in docs:
                r = d.to_dict() or {}
                st.write(f"{r.get('run_id')} → {r.get('input_file')}")
                for k in ["csp_docx", "aha_book_docx", "manifest_path"]:
                    v = r.get(k)
                    if v:
                        st.caption(v)
        except Exception:
            st.caption("Run history unavailable.")

    # Display all messages from the conversation so far
    # Each message is either a ModelRequest or ModelResponse.
    # We iterate over their parts to decide how to display them.
    for msg in st.session_state.messages:
        if isinstance(msg, ModelRequest) or isinstance(msg, ModelResponse):
            for part in msg.parts:
                display_message_part(part)

    # Chat input for the user
    user_input = st.chat_input("What do you want to know?")

    if user_input:
        # Display user prompt in the UI
        with st.chat_message("user"):
            st.markdown(user_input)

        # Display the assistant's partial response while streaming
        with st.chat_message("assistant"):
            # Create a placeholder for the streaming text
            message_placeholder = st.empty()
            full_response = ""
            
            # Properly consume the async generator with async for
            generator = run_agent_with_streaming(user_input)
            async for message in generator:
                full_response += message
                message_placeholder.markdown(full_response + "▌")
            
            # Final response without the cursor
            message_placeholder.markdown(full_response)


if __name__ == "__main__":
    asyncio.run(main())
