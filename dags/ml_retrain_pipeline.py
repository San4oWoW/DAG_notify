import json
import os
import tempfile
from datetime import datetime

import mlflow
import requests
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator

MODEL_VERSION = os.getenv("MODEL_VERSION", "v1.0.0")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

QUALITY_THRESHOLD = float(os.getenv("QUALITY_THRESHOLD", "0.80"))

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5000")
MLFLOW_EXPERIMENT = os.getenv("MLFLOW_EXPERIMENT", "retraining")

MLFLOW_S3_ENDPOINT_URL = os.getenv("MLFLOW_S3_ENDPOINT_URL", "http://minio:9000")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "minioadmin")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123")


def _send_telegram(text: str) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        raise AirflowException("Не заданы TELEGRAM_BOT_TOKEN и/или TELEGRAM_CHAT_ID")

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    r = requests.post(url, data={"chat_id": TELEGRAM_CHAT_ID, "text": text}, timeout=30)
    if r.status_code != 200:
        raise AirflowException(f"Telegram error {r.status_code}: {r.text}")


def train_model(**context):
    print("Модель обучена")
    print(f"Model candidate: {MODEL_VERSION}")


def evaluate_model(**context):
    accuracy = float(os.getenv("CANDIDATE_ACCURACY", "0.85"))
    context["ti"].xcom_push(key="accuracy", value=accuracy)
    print(f"Candidate accuracy={accuracy}, threshold={QUALITY_THRESHOLD}")

    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(MLFLOW_EXPERIMENT)

    os.environ["MLFLOW_S3_ENDPOINT_URL"] = MLFLOW_S3_ENDPOINT_URL
    os.environ["AWS_ACCESS_KEY_ID"] = AWS_ACCESS_KEY_ID
    os.environ["AWS_SECRET_ACCESS_KEY"] = AWS_SECRET_ACCESS_KEY

    with mlflow.start_run(run_name=f"candidate_{MODEL_VERSION}") as run:
        mlflow.log_param("model_version", MODEL_VERSION)
        mlflow.log_metric("accuracy", accuracy)
        mlflow.log_metric("quality_threshold", QUALITY_THRESHOLD)

        payload = {
            "model_version": MODEL_VERSION,
            "accuracy": accuracy,
            "threshold": QUALITY_THRESHOLD,
        }

        with tempfile.TemporaryDirectory() as d:
            metrics_path = os.path.join(d, "metrics.json")
            with open(metrics_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False, indent=2)
            mlflow.log_artifact(metrics_path)

        context["ti"].xcom_push(key="mlflow_run_id", value=run.info.run_id)
        print(f"Logged to MLflow run_id={run.info.run_id}")


def branch_on_quality(**context):
    ti = context["ti"]
    acc = ti.xcom_pull(task_ids="evaluate_model", key="accuracy")

    if acc is None:
        acc = float(os.getenv("CANDIDATE_ACCURACY", "0.85"))
    else:
        acc = float(acc)

    if acc >= QUALITY_THRESHOLD:
        return "deploy_model"
    return "skip_deploy"


def deploy_model(**context):
    print(f"Модель {MODEL_VERSION} выведена в продакшен")

    run_id = context["ti"].xcom_pull(task_ids="evaluate_model", key="mlflow_run_id")
    if run_id:
        mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

        os.environ["MLFLOW_S3_ENDPOINT_URL"] = MLFLOW_S3_ENDPOINT_URL
        os.environ["AWS_ACCESS_KEY_ID"] = AWS_ACCESS_KEY_ID
        os.environ["AWS_SECRET_ACCESS_KEY"] = AWS_SECRET_ACCESS_KEY

        with mlflow.start_run(run_id=run_id):
            mlflow.set_tag("deployment", "prod")


def notify_success(**context):
    text = f"Новая модель в продакшене: {MODEL_VERSION}"
    _send_telegram(text)
    print("Уведомление о деплое отправлено")


def notify_rejected(**context):
    ti = context["ti"]
    acc = ti.xcom_pull(task_ids="evaluate_model", key="accuracy")
    if acc is None:
        acc = float(os.getenv("CANDIDATE_ACCURACY", "0.85"))
    else:
        acc = float(acc)

    text = (
        f"Деплой отменён: кандидат {MODEL_VERSION} не прошёл quality gate. "
        f"accuracy={acc} < threshold={QUALITY_THRESHOLD}"
    )
    _send_telegram(text)
    print("Уведомление об отклонении отправлено")


with DAG(
    dag_id="ml_retrain_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["ml", "retrain"],
) as dag:
    train = PythonOperator(task_id="train_model", python_callable=train_model)
    evaluate = PythonOperator(task_id="evaluate_model", python_callable=evaluate_model)

    branch = BranchPythonOperator(task_id="branch_on_quality", python_callable=branch_on_quality)

    deploy = PythonOperator(task_id="deploy_model", python_callable=deploy_model)
    notify_ok = PythonOperator(task_id="notify_success", python_callable=notify_success)

    skip = EmptyOperator(task_id="skip_deploy")
    notify_bad = PythonOperator(task_id="notify_rejected", python_callable=notify_rejected)

    end = EmptyOperator(task_id="end")

    train >> evaluate >> branch
    branch >> deploy >> notify_ok >> end
    branch >> skip >> notify_bad >> end
