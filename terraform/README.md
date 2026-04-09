# Terraform - DataTalent

Cette configuration Terraform provisionne une base d'infrastructure GCP pour le projet DataTalent :

- bucket GCS `raw`
- datasets BigQuery `staging`, `intermediate`, `marts`
- service account pipeline + IAM
- secrets de base dans Secret Manager
- Cloud Run Job pour l'ingestion
- Cloud Scheduler pour déclencher le job

## Prérequis

- Terraform >= 1.6
- `gcloud auth application-default login`
- droits suffisants sur le projet `datatalent-simplon`

## Utilisation

```bash
cd terraform
terraform init
terraform plan -var-file=dev.tfvars
terraform apply -var-file=dev.tfvars
```

## Variables importantes

- `project_id` : `datatalent-simplon`
- `project_name` : `datatalent`
- `region` : `europe-west1`
- `bq_location` : `EU`
- `pipeline_image` : image du job d'ingestion

## Notes

- Les valeurs sensibles (contenu des secrets) ne sont pas versionnées ici.
- Les secrets sont créés, puis l'alimentation des versions peut se faire via CI/CD.
