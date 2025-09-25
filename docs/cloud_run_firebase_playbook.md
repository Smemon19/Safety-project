## Cloud Run + Firebase Hosting Playbook (Streamlit + WebSockets)

This playbook deploys a Python Streamlit app on Google Cloud Run and fronts it with Firebase Hosting using a path-preserving 302 redirect so WebSockets connect directly to Cloud Run.

Replace placeholders in ALL_CAPS.

### Prereqs

- gcloud SDK installed and logged in
- Firebase CLI installed and logged in
- Project details:
  - GCP project: YOUR_GCP_PROJECT_ID (region: YOUR_REGION, e.g., us-central1)
  - Firebase project: YOUR_FIREBASE_PROJECT_ID
  - Cloud Run service name: YOUR_SERVICE_NAME
  - Required secrets: OPENAI_API_KEY

### App expectations

- App listens on 0.0.0.0:8080
- Streamlit started with:
  - --server.enableCORS=false
  - --server.enableXsrfProtection=false
  - --server.port=8080
  - --server.address=0.0.0.0
- .env load never overrides existing env vars
- When Cloud Run is detected (K_SERVICE set), app data lives under /tmp/.calrag

### 1) Enable APIs and configure defaults

```bash
PROJECT=YOUR_GCP_PROJECT_ID
REGION=YOUR_REGION
SERVICE=YOUR_SERVICE_NAME

gcloud auth login --quiet
gcloud config set project $PROJECT
gcloud config set run/region $REGION

gcloud services enable run.googleapis.com cloudbuild.googleapis.com artifactregistry.googleapis.com --quiet
```

### 2) Build and deploy to Cloud Run (from repo root)

- Ensure Dockerfile and .dockerignore exist (they are in this repo).
- Deploy from source with unauthenticated access on port 8080.

```bash
# Using Cloud Build + Cloud Run source deployment
gcloud run deploy $SERVICE \
  --source . \
  --allow-unauthenticated \
  --port 8080 \
  --region $REGION \
  --project $PROJECT
```

Set environment variables (OPENAI_API_KEY at minimum). You can do this interactively in Console or via CLI:

```bash
# Set required env vars
OPENAI_API_KEY=YOUR_OPENAI_API_KEY

gcloud run services update $SERVICE \
  --update-env-vars OPENAI_API_KEY=$OPENAI_API_KEY \
  --region $REGION \
  --project $PROJECT
```

Optionally bump resources if needed (example 2Gi/2 vCPU):

```bash
gcloud run services update $SERVICE \
  --memory 2Gi --cpu 2 \
  --region $REGION --project $PROJECT
```

Get the service URL:

```bash
CLOUD_RUN_URL=$(gcloud run services describe $SERVICE --format='value(status.url)' --region $REGION --project $PROJECT)
echo $CLOUD_RUN_URL
```

### 3) Configure Firebase Hosting redirect

In `firebase.json`, set a path-preserving 302 redirect:

```json
{
  "hosting": {
    "public": "public",
    "ignore": ["firebase.json", "**/.*", "**/node_modules/**"],
    "redirects": [
      { "source": "/:path*", "destination": "CLOUD_RUN_URL/:path", "type": 302 }
    ]
  }
}
```

Replace CLOUD_RUN_URL with your actual URL (e.g., https://SERVICE-PROJECTNUM.REGION.run.app).

Set default project in `.firebaserc`:

```json
{ "projects": { "default": "YOUR_FIREBASE_PROJECT_ID" } }
```

Make sure `public/404.html` exists (it does in this repo). Then deploy:

```bash
firebase use YOUR_FIREBASE_PROJECT_ID
firebase deploy --only hosting
```

### 4) Verify

- Open Cloud Run URL; confirm Streamlit UI loads.
- In DevTools Network, confirm WebSocket 101 upgrade on `/_stcore/stream` at the Cloud Run domain.
- Open the Hosting URL; confirm it 302 redirects path-preserving to Cloud Run and the app works.
- Check Cloud Run logs for errors; fix env or resource issues as needed.

### Troubleshooting

- WebSocket errors: ensure Hosting uses path-preserving 302 redirect, not proxy rewrite. Test in a fresh/incognito tab.
- App stuck loading: confirm OPENAI_API_KEY set on Cloud Run; .env loading must not override process env; Streamlit CORS/XSRF disabled.
- Write errors: ensure app writes only under /tmp when on Cloud Run.
- Performance/timeouts: increase memory/CPU; lazy-load heavy deps or choose smaller models.

### Reuse checklist for new projects

- Ensure `.env` loading does not override env.
- Detect Cloud Run (K_SERVICE) and write under `/tmp/.calrag`.
- Streamlit run flags: CORS/XSRF disabled, bind 0.0.0.0:8080.
- Dockerfile: Python 3.12 slim, install needed system packages (tesseract if OCR), pip install `requirements.txt`, run Streamlit.
- .dockerignore: exclude venvs, caches, VCS, local DB/data.
- Cloud Run: unauthenticated, set env vars, adjust resources.
- Firebase: `/:path*` -> `CLOUD_RUN_URL/:path` 302 redirect; deploy and verify.
