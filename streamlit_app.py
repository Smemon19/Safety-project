from dotenv import load_dotenv
import streamlit as st
import asyncio
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
                            # Per-activity AHAs
                            per_files = []
                            for aha in ahas:
                                slug = aha.activity.lower().replace(' ', '_')
                                per_files.append(write_aha_single(aha, f"outputs/ahas/{slug}.docx"))
                                per_files.append(write_aha_single_html(aha, f"outputs/ahas/{slug}.html"))
                                per_files.append(write_aha_single_md(aha, f"outputs/ahas/{slug}.md"))
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
                                "per_files": per_files,
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

        st.markdown("**Per‑Activity AHAs**")
        for idx, p in enumerate(outputs.get("per_files", [])):
            add_download(p, f"Download {os.path.basename(p)}", f"per_{idx}")

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
