import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.trigger_rule import TriggerRule

from game_deals.constants import (
    POSTGRES_CONN_ID,
    PATH_TO_SQL,
    STORES_ENDPOINT,
    DEALS_ENDPOINT,
    DEALS_MIN_REVIEW_COUNT,
    STORES_SOURCE,
    DEALS_STORE_SOURCE
)
from game_deals.common import start_run
from game_deals.sheapshark import RateLimitReached, fetch_and_save_raw, close_run

logger = logging.getLogger(__name__)

DAG_ID = "dag_initial_load"


def fetch_stores(ti):
    return fetch_and_save_raw(
        endpoint=STORES_ENDPOINT,
        source=STORES_SOURCE,
        ti=ti,
        paginate=False,
    )


def fetch_deals(ti):
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    stores = pg_hook.get_records("""
        SELECT store_id
        FROM core.stores
        WHERE is_active IS TRUE
        ORDER BY store_id;
    """)

    if not stores:
        raise ValueError("В core.stores не найдено активных магазинов")

    logging.info("Найдено %s активных магазинов", len(stores))

    try:
        for (store_id,) in stores:
            logging.info(
                "Получение предложений для store_id=%s",
                store_id,
            )

            fetch_and_save_raw(
                endpoint=DEALS_ENDPOINT,
                source=DEALS_STORE_SOURCE + str(store_id),
                ti=ti,
                params={
                    "storeID": str(store_id),
                    "minimumReviewCount": DEALS_MIN_REVIEW_COUNT,
                },
                paginate=True,
                page_size=60,
            )

            logging.info(
                "Завершено получение предложений для store_id=%s",
                store_id,
            )

    except RateLimitReached as exc:
        logger.warning("Прием предложений остановлен: %s", exc)

        ti.xcom_push(
            key="ingestion_rate_limited",
            value=True,
        )

        return {
            "status": "partial",
            "reason": str(exc),
        }


with DAG(
    dag_id=DAG_ID,
    schedule_interval=None,
    template_searchpath=[PATH_TO_SQL]
) as dag:

    start_run_task = PythonOperator(
        task_id="start_run",
        python_callable=start_run
    )

    fetch_stores_task = PythonOperator(
        task_id="fetch_stores",
        python_callable=fetch_stores
    )

    load_stores_to_core_task = SQLExecuteQueryOperator(
        task_id="load_stores_to_core",
        conn_id=POSTGRES_CONN_ID,
        sql="/load_stores_to_core.sql",
        parameters={
            "run_id": "{{ ti.xcom_pull(task_ids='start_run') }}"
        },
    )

    fetch_deals_task = PythonOperator(
        task_id="fetch_deals",
        python_callable=fetch_deals,
    )

    load_games_to_core_task = SQLExecuteQueryOperator(
        task_id="load_games_to_core",
        conn_id=POSTGRES_CONN_ID,
        sql="/load_games_to_core.sql",
        parameters={
                "run_id": "{{ ti.xcom_pull(task_ids='start_run') }}"
        },
    )

    load_deals_to_core_task = SQLExecuteQueryOperator(
        task_id="load_deals_to_core",
        conn_id=POSTGRES_CONN_ID,
        sql="/load_deals_to_core.sql",
        parameters={
            "run_id": "{{ ti.xcom_pull(task_ids='start_run') }}"
        },
    )

    close_run_task = PythonOperator(
        task_id="close_run",
        python_callable=close_run,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    start_run_task
    start_run_task >> fetch_stores_task
    fetch_stores_task >> load_stores_to_core_task
    load_stores_to_core_task >> fetch_deals_task
    fetch_deals_task >> load_games_to_core_task
    load_games_to_core_task >> load_deals_to_core_task
    load_deals_to_core_task >> close_run_task
