# Section 11 Generator — Test Tab Plan

## 0. Tab purpose
The **Section 11 Generator — Test** tab accepts a user-supplied project specification (PDF or DOCX), mines it for scope, codes, and hazards, determines which codes mandate Activity Hazard Analyses (AHAs), organizes everything by EM 385 Part 11 categories, generates evidence-backed AHAs and Safety Plans, and records a complete audit trail of the run.

## 1. UI layout
The tab uses a linear, top-to-bottom layout that keeps the workflow obvious and minimal:
- **Upload (PDF/DOCX)** – file input control with filename, size, and checksum preview.
- **Parse & Detect** – button kicks off document parsing; below it show cards for scope summary, table of detected codes with references, and optional hazard phrase snippets.
- **AHA Requirements** – table view of all detected codes annotated with whether an AHA is required, suggested category, notes, and discovery source; include filter chips such as “Requires AHA = Yes”.
- **Category Review** – table where each AHA-required code is mapped to an EM 385 category, with dropdown overrides and contextual hints; display an Unmapped count badge and disable downstream actions until it reaches zero.
- **Generate** – consolidated button that first creates AHAs for each category, then runs Safety Plan generation; show per-category progress bars/status pills (Required, Pending – Insufficient Evidence, Complete).
- **Results** – present the Section 11.0 Compliance Matrix, expandable previews for each AHA and Safety Plan, and download links for markdown, DOCX, and JSON artifacts.
- **Run Log & Exports** – diagnostic JSON view, manifest preview, and links to the persisted artifacts bundle.
Sticky header chips summarize counts: Codes found, Require AHA, Categories mapped, Unmapped, AHAs completed/pending, Plans completed/pending.

## 2. Upload & parsing
1. Accept a single PDF or DOCX via Streamlit’s uploader; compute checksum and store metadata in session state.
2. When the user clicks **Parse & Detect**, send the document to the parsing service.
3. Extract:
   - Project scope passages (paragraphs, bullets, activity descriptions).
   - Codes/identifiers (UFGS, MSF, ENG, etc.) with page/heading context.
   - Optional hazard-ish phrases for later hints.
4. Display:
   - A scope summary card (concise text, expand for highlights).
   - Codes table with code, title (if resolvable), and source references.
   - Optional hazard phrase list.
5. On failure, show a retry-friendly error (include stack trace in logs only) and keep the upload intact for another attempt.

## 3. Code requirement check
1. Use Firebase reference data to determine whether each code requires an AHA.
2. For every parsed code, fetch: `requires_aha`, canonical title, default category (if known), and notes.
3. Render the **Codes in this spec** table with columns: Code, Title/Short Name, Requires AHA (Yes/No), Suggested Category, Notes, Source (page/heading).
4. Provide quick filters/chips (e.g., “Requires AHA = Yes”) to focus on actionable items.
5. Persist the lookup results so downstream steps reuse the same snapshot.

## 4. Category grouping and review
1. Build a mapping for codes where `requires_aha == Yes` using the EM 385 category dictionary (e.g., Electrical/Energy Control, Fall Protection & Prevention).
2. Any code without a known mapping defaults to **Unmapped**.
3. Show the **Category Review** table with columns: Code, Suggested Category, Override (dropdown of EM 385 categories), Why (keywords or rule snippet explaining the suggestion).
4. Provide per-category counts and highlight rows flagged as Unmapped.
5. Disable the **Generate** button while any code remains Unmapped; encourage the user to select overrides until Unmapped count hits zero.
6. After review, compute the final structure: `Category -> [codes requiring AHA]` and cache it for generation.

## 5. Generate AHAs (hazard analysis only)
1. Clicking **Generate** triggers a pipeline that, for each category, first creates the AHA.
2. Compose the retrieval context from project scope excerpts plus the codes within the category.
3. Query the EM 385 RAG index for hazard-specific information (definitions, risk conditions, triggers) relevant to the category and scope.
4. Draft an AHA narrative that:
   - Names discrete hazards (e.g., arc flash, fall from elevation).
   - Explains why each hazard applies to the project scope.
   - Describes conditions elevating likelihood/severity.
   - Excludes controls or mitigations entirely.
5. Include normalized EM 385 citations for each hazard statement; if insufficient evidence, mark status **Pending – Insufficient Evidence** and explain what was missing.
6. Save each category AHA as `ahas/<category_slug>.md` and expose download links.

## 6. Generate Safety Plans (solutions/controls)
1. After an AHA completes for a category, generate the corresponding Safety Plan.
2. Build context from the category’s hazard list, project scope, and codes.
3. Query EM 385 RAG for controls, required procedures, PPE, training, permits, and inspection obligations tied to the hazards; supplement with project document references when available.
4. Enforce evidence quotas: at least two project citations and two EM 385 citations, maximum of five total distinct citations, normalized and deduplicated.
5. Structure the Safety Plan with sections for Controls & Procedures, PPE, Training, Permits/LOTO/Inspections, and Compliance Notes.
6. If quotas cannot be met, set status **Pending – Insufficient Evidence** with explicit reasons (e.g., “Need ≥2 EM 385 sources on arc-flash PPE”).
7. Save each plan as `plans/11.x_<category_slug>.md` (incremental numbering per run) and link in the UI.

## 7. Compliance Matrix (Section 11.0)
1. Assemble a matrix summarizing each category.
2. Columns: Category, Codes Included, AHA Status, Safety Plan Status, Project Evidence Count, EM Evidence Count, Open AHA, Open Plan.
3. Display the matrix at the top of the Results section; provide buttons to jump to each detailed markdown.
4. Include the matrix as the opening section of the combined `section11.md`/DOCX output.

## 8. Persistence and audit trail
1. Assign a unique `run_id` when generation starts.
2. Store Firebase records:
   - `runs/{run_id}` metadata (timestamps, user, source file hash, counts).
   - `runs/{run_id}/codes` documents capturing each code’s data and sources.
   - `runs/{run_id}/categories` documents with code lists, hazards, statuses, evidence counts, citations, and pending reasons.
   - `runs/{run_id}/overrides` capturing any user mapping overrides.
   - `runs/{run_id}/artifacts` referencing saved files with paths and checksums.
3. Generate artifacts for downloads:
   - `section11.md` containing Section 11.0 matrix followed by AHAs and Safety Plans.
   - `section11.docx` mirroring the markdown content.
   - `section11_report.json` with full diagnostics and evidence metadata.
   - `section11_bundle/manifest.json` and related subfiles (`plans/`, `ahas/`, etc.).
4. Surface the artifact bundle in the **Run Log & Exports** section and ensure the manifest validates against internal stitcher expectations.

## 9. Buttons, states, and gating logic
- **Upload** – accepts/replaces the source document; resets downstream state.
- **Parse & Detect** – runs parsing, populates scope/codes tables.
- **Check AHA Requirements** – toggles the table to reveal AHA requirement lookups.
- **Review Categories** – exposes mapping overrides and Unmapped counter.
- **Generate** – sequentially executes AHA generation then Safety Plan generation; show progress spinners per category.
- **Results & Exports** – becomes visible once generation finishes or pending statuses exist.
- **Save Run** – auto-triggered post-generation to persist Firebase records and artifacts; also offer a manual “Re-save” button for retries.
- Disable Generate and downstream views until parsing, AHA requirement check, and category review steps complete with zero Unmapped items.

## 10. Quality enforcement rules
- AHAs describe hazards only—no mitigations or controls.
- Safety Plans focus solely on solutions (controls, PPE, training, permits, inspections).
- All content derives from EM 385 RAG (plus project docs for context); avoid boilerplate.
- Safety Plans must meet evidence quotas (≥2 project sources, ≥2 EM sources, ≤5 citations total, normalized/deduped).
- Pending statuses must call out what evidence is missing.
- Generation cannot proceed while any category is Unmapped.

## 11. Metrics displayed in UI
- Codes found
- Require AHA
- Categories mapped
- Unmapped
- AHAs created / AHAs pending
- Safety Plans created / Plans pending

## 12. Edge cases
- **No codes found** – show guidance encouraging upload of full spec or appendices.
- **All codes = No AHA** – show confirmation that no generation is required.
- **Unmapped items remain** – keep Generate disabled, highlight rows needing attention.
- **Low evidence** – allow Pending outputs with clear “missing evidence” messaging.
- **Duplicate codes** – deduplicate while preserving provenance entries.

## 13. Acceptance checklist
- Parser extracts scope, codes, and optional hazard phrases.
- Firebase lookup correctly identifies which codes require AHAs.
- All AHA-required codes are mapped to EM 385 categories; no Unmapped items at generate time.
- AHA generation produces hazard-only narratives with EM 385 citations or Pending statuses.
- Safety Plan generation produces control-focused content meeting evidence quotas or Pending statuses.
- Compliance Matrix reflects accurate statuses and links to detailed sections.
- All artifacts (markdown, DOCX, JSON, manifest) download successfully and pass validation.
- Firebase run records capture full lineage: codes → categories → AHA → Safety Plan with evidence details.
