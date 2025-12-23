# Airflow ML Retrain Pipeline + Notify + IaC + CI

## Что сделано
- Airflow DAG `ml_retrain_pipeline`: train -> evaluate -> branch -> (deploy + notify_success) / (skip + notify_rejected)
- Уведомление о деплое отправляется в Telegram (задача `notify_success`)
- Версия модели берётся из переменной окружения `MODEL_VERSION`
- MLflow используется для логирования метрик/артефактов (experiment `retraining`)
- MinIO используется как S3-хранилище артефактов MLflow
- IaC: terraform поднимает сервисы (MinIO + MLflow) через docker compose
- CI/CD: GitHub Actions делает `ruff`, `compileall` для DAG'ов и `terraform init/validate`

## Как запустить локально
1. Запустить Airflow:
   - `docker compose up -d`
2. Запустить IaC (MLflow + MinIO):
   - `cd infra`
   - `terraform init`
   - `terraform apply -auto-approve`

## Переменные окружения
- `MODEL_VERSION` — версия модели для уведомления
- `TELEGRAM_BOT_TOKEN` — токен Telegram-бота
- `TELEGRAM_CHAT_ID` — id чата/группы для уведомлений
