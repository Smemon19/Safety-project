import json
import os
import sys
import time

try:
    from dotenv import load_dotenv
except Exception:
    def load_dotenv(*args, **kwargs):  # type: ignore
        return False


def _project_root() -> str:
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _default_credentials_path() -> str:
    return os.path.join(_project_root(), "firebase-admin.json")


def _load_env() -> None:
    # Load env from the app's persisted path if present, then local .env
    load_dotenv(override=False)
    load_dotenv(override=True)


def main() -> int:
    _load_env()

    creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip()
    if not creds_path:
        creds_path = _default_credentials_path()

    if not os.path.isabs(creds_path):
        creds_path = os.path.abspath(creds_path)

    if not os.path.exists(creds_path):
        print(f"[sanity] Credentials file not found at: {creds_path}")
        print("[sanity] Set GOOGLE_APPLICATION_CREDENTIALS in your .env or place firebase-admin.json in the project root.")
        return 1

    # Read project id from env or from the credentials file
    project_id = (
        os.getenv("FIREBASE_PROJECT_ID")
        or os.getenv("GOOGLE_CLOUD_PROJECT")
        or ""
    ).strip()
    try:
        with open(creds_path, "r", encoding="utf-8") as f:
            creds_obj = json.load(f)
        if not project_id:
            project_id = (creds_obj.get("project_id") or "").strip()
    except Exception:
        creds_obj = {}

    if not project_id:
        print("[sanity] Could not resolve project id. Set FIREBASE_PROJECT_ID or GOOGLE_CLOUD_PROJECT in your .env.")
        return 1

    print(f"[sanity] Using project: {project_id}")
    print(f"[sanity] Using credentials: {creds_path}")

    try:
        import firebase_admin
        from firebase_admin import credentials, firestore
    except Exception as e:
        print(f"[sanity] Missing dependency: {e}")
        print("[sanity] Please install 'firebase-admin' (it will also install firestore clients).")
        return 1

    try:
        try:
            firebase_admin.get_app()
        except ValueError:
            cred = credentials.Certificate(creds_path)
            firebase_admin.initialize_app(cred, {"projectId": project_id})
    except Exception as e:
        print(f"[sanity] Failed to initialize Firebase Admin: {e}")
        return 1

    # Firestore write/read sanity check
    try:
        db = firestore.client()
        doc_ref = db.collection("sanity_check").document("connectivity")
        payload = {"checked_at": time.time(), "host": os.uname().nodename}
        doc_ref.set(payload)
        got = doc_ref.get()
        if not got.exists:
            print("[sanity] Firestore read failed: document not found after write.")
            return 1
        data = got.to_dict() or {}
        print("[sanity] Firestore OK. Document:", {k: data.get(k) for k in ("checked_at", "host")})
    except Exception as e:
        print(f"[sanity] Firestore check failed: {e}")
        return 1

    # Optional: Storage bucket discovery (no write)
    bucket_name = (os.getenv("FIREBASE_STORAGE_BUCKET") or "").strip()
    if bucket_name:
        try:
            from firebase_admin import storage
            b = storage.bucket(bucket_name)
            print(f"[sanity] Storage bucket configured: {b.name}")
        except Exception as e:
            print(f"[sanity] Storage check skipped/failed: {e}")

    print("[sanity] All checks passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())


