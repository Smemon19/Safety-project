from __future__ import annotations

import json
import os
import sys
from typing import Dict, Any


def _project_root() -> str:
    import os as _os
    return _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))


def _load_env() -> None:
    try:
        from dotenv import load_dotenv
    except Exception:
        def load_dotenv(*args, **kwargs):  # type: ignore
            return False
    # Load local .env without overwriting env that may be set by the platform
    load_dotenv(override=False)
    load_dotenv(override=True)


def main() -> int:
    _load_env()

    # Firebase Admin init
    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS") or os.path.join(_project_root(), "firebase-admin.json")
    project_id = os.getenv("FIREBASE_PROJECT_ID") or os.getenv("GOOGLE_CLOUD_PROJECT") or None
    try:
        import firebase_admin
        from firebase_admin import credentials, firestore
    except Exception as e:
        print(f"[counts] Missing dependency: {e}")
        print("[counts] Please install 'firebase-admin'.")
        return 1

    try:
        try:
            firebase_admin.get_app()
        except ValueError:
            kwargs = {"projectId": project_id} if project_id else {}
            firebase_admin.initialize_app(credentials.Certificate(creds_path), kwargs)
    except Exception as e:
        print(f"[counts] Firebase init failed: {e}")
        return 1

    db = firestore.client()

    def _count(coll_name: str) -> int:
        n = 0
        for _ in db.collection(coll_name).stream():
            n += 1
        return n

    # Totals
    total_codes = _count("codes")
    total_decisions = _count("decisions")
    total_runs = _count("runs")

    # Decisions breakdown and missing
    yes = no = unknown = 0
    decisions_ids = set()
    try:
        for doc in db.collection("decisions").stream():
            decisions_ids.add(doc.id)
            d = doc.to_dict() or {}
            if "requiresAha" in d:
                if bool(d.get("requiresAha")):
                    yes += 1
                else:
                    no += 1
            else:
                unknown += 1
    except Exception:
        pass

    codes_ids = set()
    try:
        for doc in db.collection("codes").stream():
            codes_ids.add(doc.id)
    except Exception:
        pass

    missing = sorted(codes_ids - decisions_ids)

    summary: Dict[str, Any] = {
        "counts": {
            "codes": total_codes,
            "decisions": total_decisions,
            "runs": total_runs,
        },
        "decisions_breakdown": {
            "yes": yes,
            "no": no,
            "unknown": unknown,
        },
        "missing_decisions": {
            "count": len(missing),
            "sample": missing[:20],
        },
    }

    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())


