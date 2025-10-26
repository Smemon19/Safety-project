from __future__ import annotations

import argparse
import csv
import json
import os
from pathlib import Path
from typing import Any, Dict, List, Tuple


def _project_root() -> str:
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _load_env() -> None:
    try:
        from dotenv import load_dotenv
    except Exception:
        def load_dotenv(*_args, **_kwargs):  # type: ignore
            return False
    load_dotenv(override=False)
    load_dotenv(override=True)


def _init_db():
    _load_env()
    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or os.path.join(_project_root(), "firebase-admin.json")
    project_id = os.getenv("FIREBASE_PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT") or None
    import firebase_admin
    from firebase_admin import credentials, firestore
    try:
        firebase_admin.get_app()
    except ValueError:
        kwargs = {"projectId": project_id} if project_id else {}
        firebase_admin.initialize_app(credentials.Certificate(creds_path), kwargs)
    return firestore.client()


def _collect(db, prefix: str | None) -> Tuple[List[Dict[str, Any]], List[str]]:
    """Return list of decisions and list of missing codes (codes without decisions)."""
    # decisions
    decisions: List[Dict[str, Any]] = []
    dec_ids = set()
    for doc in db.collection("decisions").stream():
        cid = doc.id
        if prefix and not cid.startswith(prefix):
            continue
        d = doc.to_dict() or {}
        d["code_token"] = cid
        decisions.append(d)
        dec_ids.add(cid)

    # missing vs codes collection
    missing: List[str] = []
    try:
        for doc in db.collection("codes").stream():
            cid = doc.id
            if prefix and not cid.startswith(prefix):
                continue
            if cid not in dec_ids:
                missing.append(cid)
    except Exception:
        pass

    # Sort decisions by token
    decisions.sort(key=lambda d: str(d.get("code_token", "")))
    missing.sort()
    return decisions, missing


def _to_markdown(decisions: List[Dict[str, Any]], missing: List[str]) -> str:
    yes = [d for d in decisions if d.get("requiresAha") is True]
    no = [d for d in decisions if d.get("requiresAha") is False]
    unknown = [d for d in decisions if "requiresAha" not in d]

    lines: List[str] = []
    lines.append("# AHA Decision Report")
    lines.append("")
    lines.append("## Totals")
    lines.append("")
    lines.append("| Metric | Count |")
    lines.append("| --- | ---: |")
    lines.append(f"| Decisions | {len(decisions)} |")
    lines.append(f"| Requires AHA (Yes) | {len(yes)} |")
    lines.append(f"| Does Not Require AHA (No) | {len(no)} |")
    lines.append(f"| Unknown in decisions | {len(unknown)} |")
    lines.append(f"| Missing decisions (codes without decisions) | {len(missing)} |")
    lines.append("")

    def section(title: str, rows: List[Dict[str, Any]]):
        lines.append(f"## {title} ({len(rows)})")
        lines.append("")
        if not rows:
            lines.append("(none)")
            lines.append("")
            return
        lines.append("| Code | Confidence | Rationale | Citations |")
        lines.append("| --- | ---: | --- | ---: |")
        for d in rows:
            code = str(d.get("code_token") or "")
            conf = d.get("confidence")
            conf_s = f"{float(conf):.2f}" if isinstance(conf, (int, float)) else ""
            rat = (d.get("rationale") or "").strip().replace("\n", " ")
            if len(rat) > 140:
                rat = rat[:137] + "..."
            cits = d.get("citations") or []
            lines.append(f"| {code} | {conf_s} | {rat} | {len(cits)} |")
        lines.append("")

    section("Requires AHA", yes)
    section("Does Not Require AHA", no)

    if missing:
        lines.append("## Missing Decisions (present in codes, not in decisions)")
        lines.append("")
        for cid in missing:
            lines.append(f"- {cid}")
        lines.append("")

    return "\n".join(lines)


def _to_csv(decisions: List[Dict[str, Any]], missing: List[str], output_path: str) -> None:
    fieldnames = [
        "code_token",
        "requiresAha",
        "confidence",
        "rationale",
        "citations_count",
    ]
    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for d in decisions:
            w.writerow({
                "code_token": d.get("code_token", ""),
                "requiresAha": d.get("requiresAha"),
                "confidence": d.get("confidence"),
                "rationale": (d.get("rationale") or "").replace("\n", " "),
                "citations_count": len(d.get("citations") or []),
            })
    # Optional: write missing as a sidecar list
    if missing:
        miss_path = out.with_suffix(".missing.txt")
        miss_path.write_text("\n".join(missing), encoding="utf-8")


def _parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Export AHA decision report from Firestore")
    ap.add_argument("--format", choices=["md", "csv", "json"], default="md")
    ap.add_argument("--output", default="outputs/reports/aha_decisions_report.md")
    ap.add_argument("--prefix", default=None, help="Only include codes/decisions whose id starts with this prefix (e.g., UFGS-)")
    return ap.parse_args()


def main() -> int:
    args = _parse_args()
    db = _init_db()
    decisions, missing = _collect(db, args.prefix)

    if args.format == "json":
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps({"decisions": decisions, "missing": missing}, indent=2), encoding="utf-8")
        print(f"[report] wrote {out}")
        return 0

    if args.format == "csv":
        _to_csv(decisions, missing, args.output)
        print(f"[report] wrote {args.output}")
        return 0

    # md
    md = _to_markdown(decisions, missing)
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(md, encoding="utf-8")
    print(f"[report] wrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())


