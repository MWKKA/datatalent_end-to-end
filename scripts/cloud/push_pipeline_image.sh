#!/usr/bin/env bash
# Build local → Artifact Registry → met à jour le Cloud Run Job d’ingestion.
# Prérequis : Docker démarré, gcloud connecté au bon projet, droits AR + Run.
#
# Registre : Artifact Registry (pas Container Registry legacy).
# Image    : europe-west1-docker.pkg.dev/<project>/datatalent/pipeline:latest

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT}"

PROJECT="${GCP_PROJECT_ID:-datatalent-simplon}"
REGION="${GCP_REGION:-europe-west1}"
REPO="datatalent"
IMAGE_NAME="pipeline"
TAG="${IMAGE_TAG:-latest}"
IMAGE="${REGION}-docker.pkg.dev/${PROJECT}/${REPO}/${IMAGE_NAME}:${TAG}"
JOB="${CLOUD_RUN_JOB_NAME:-datatalent-ingestion}"

echo "==> Build ${IMAGE}"
docker build -t "${IMAGE}" .

echo "==> Auth Docker vers Artifact Registry"
gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet

echo "==> Push"
docker push "${IMAGE}"

echo "==> Cloud Run Job : pointer sur l’image (digest résolu au prochain execute)"
gcloud run jobs update "${JOB}" \
  --project="${PROJECT}" \
  --region="${REGION}" \
  --image="${IMAGE}"

echo "OK — image poussée et job ${JOB} mis à jour."
