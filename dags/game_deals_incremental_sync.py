import json
import logging
import pendulum
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.trigger_rule import TriggerRule

from game_deals.constants import (
    POSTGRES_CONN_ID,
    PATH_TO_SQL,
    DEALS_ENDPOINT,
    DEALS_MIN_REVIEW_COUNT,
    PAGE_SIZE,
    DEALS_STORE_SOURCE,
    CHANGES_SOURCE
)
from game_deals.common import start_run, get_run_id
from game_deals.sheapshark import (
    RateLimitReached,
    fetch_and_save_raw,
    close_run)


logger = logging.getLogger(__name__)

DAG_ID = "dag_incremental_deals"

local_tz = pendulum.timezone("Europe/Moscow")


def fetch_store_changes(ti):
    """Получаем отметки изменений магазинов и сохраняем их в raw."""
    fetch_and_save_raw(
        endpoint="stores",
        source=CHANGES_SOURCE,
        ti=ti,
        params={
            "lastChange": ""
        },
        paginate=False
    )


def select_changed_stores(ti):
    """Сравниваем API timestamps с контрольными отметками в core."""
    run_id = get_run_id(ti)
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    row = pg.get_first(
        """
        SELECT payload
        FROM raw.raw_api_responses
        WHERE run_id = %s
          AND source = %s
        ORDER BY response_id DESC
        LIMIT 1;
        """,
        parameters=(run_id, CHANGES_SOURCE),
    )

    if not row:
        raise AirflowException("Ответ store changes не найден в raw")

    payload = row[0]
    if isinstance(payload, str):
        payload = json.loads(payload)

    checkpoints = pg.get_records("""
        SELECT store_id, last_change_unix
        FROM core.stores
        WHERE is_active IS TRUE;
    """)

    known = {
        int(store_id): timestamp
        for store_id, timestamp in checkpoints
    }

    changed = []

    for api_store_id, api_timestamp in payload.items():
        try:
            store_id = int(api_store_id)
            timestamp = int(api_timestamp)
        except (TypeError, ValueError):
            logger.warning(
                "Пропускаем некорректную отметку: %r=%r",
                api_store_id,
                api_timestamp,
            )
            continue

        if store_id not in known:
            logger.warning(
                "Магазин %s отсутствует в core.stores; пропускаем",
                store_id,
            )
            continue

        previous = known[store_id]

        if previous is None or int(previous) != timestamp:
            changed.append({
                "store_id": store_id,
                "last_change_unix": timestamp,
            })

    changed.sort(key=lambda item: item["store_id"])

    logger.info(
        "Изменились магазины: %s",
        [item["store_id"] for item in changed],
    )

    return changed


def fetch_deals(ti):
    """Загружаем предложения только для изменившихся магазинов."""
    changed_stores = ti.xcom_pull(
        task_ids="select_changed_stores"
    ) or []

    if not changed_stores:
        logger.info("Нет изменившихся магазинов. Загрузка deals не требуется.")
        return {
            "status": "success",
            "loaded_stores": [],
            "loaded_store_ids_json": [],
            "loaded_stores_json": [],
        }

    logger.info(
        "Начинаем загрузку deals для %s магазинов",
        len(changed_stores),
    )

    loaded_stores = []

    try:
        for store in changed_stores:
            store_id = store["store_id"]

            logger.info("Загружаем deals для store_id=%s", store_id)

            result = fetch_and_save_raw(
                endpoint=DEALS_ENDPOINT,
                source=DEALS_STORE_SOURCE + str(store_id),
                ti=ti,
                params={
                    "storeID": str(store_id),
                    "minimumReviewCount": DEALS_MIN_REVIEW_COUNT,
                },
                paginate=True,
                page_size=PAGE_SIZE,
            )

            loaded_stores.append({
                "store_id": store_id,
                "last_change_unix": store["last_change_unix"],
                "total_pages": result["total_pages"],
            })

            logger.info(
                "Загрузка завершена для store_id=%s",
                store_id,
            )

    except RateLimitReached as exc:
        logger.warning("Загрузка deals остановлена: %s", exc)

        ti.xcom_push(
            key="ingestion_rate_limited",
            value=True,
        )

        return {
            "status": "partial",
            "reason": str(exc),
            "loaded_stores": loaded_stores,
            "loaded_store_ids_json": json.dumps(
                [store["store_id"] for store in loaded_stores]
            ),
            "loaded_stores_json": json.dumps(loaded_stores),
        }

    return {
        "status": "success",
        "loaded_stores": loaded_stores,
        "loaded_store_ids_json": json.dumps(
            [store["store_id"] for store in loaded_stores]
        ),
        "loaded_stores_json": json.dumps(loaded_stores),
    }


def has_loaded_stores(ti):
    """Проверяем, есть ли магазины для обработки в CORE."""
    result = ti.xcom_pull(task_ids="fetch_deals") or {}
    loaded_stores = result.get("loaded_stores", [])

    if not loaded_stores:
        logger.info(
            "Нет полностью загруженных магазинов. "
            "SQL-обработка будет пропущена."
        )
        return False

    logger.info(
        "Будут обработаны магазины: %s",
        [store["store_id"] for store in loaded_stores],
    )

    return True


with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 */2 * * *",
    start_date=pendulum.datetime(2026, 10, 1, tz=local_tz),
    catchup=False,
    max_active_runs=1,
    template_searchpath=[PATH_TO_SQL]
) as dag:

    start_run_task = PythonOperator(
        task_id="start_run",
        python_callable=start_run
    )

    fetch_store_changes_task = PythonOperator(
        task_id="fetch_store_changes",
        python_callable=fetch_store_changes
    )

    select_changed_stores_task = PythonOperator(
        task_id="select_changed_stores",
        python_callable=select_changed_stores
    )

    fetch_deals_task = PythonOperator(
        task_id="fetch_deals",
        python_callable=fetch_deals
    )

    has_loaded_stores_task = ShortCircuitOperator(
        task_id="has_loaded_stores",
        python_callable=has_loaded_stores,
        ignore_downstream_trigger_rules=False,
    )

    load_games_to_core_task = SQLExecuteQueryOperator(
        task_id="load_games_to_core",
        conn_id=POSTGRES_CONN_ID,
        sql="/load_games_incremental.sql",
        parameters={
            "run_id": "{{ ti.xcom_pull(task_ids='start_run') }}",
            "fetch_deals_result": "{{ ti.xcom_pull(task_ids='fetch_deals') | default({}, true) | tojson }}"
        },
    )

    archive_price_changes_task = SQLExecuteQueryOperator(
        task_id="archive_price_changes",
        conn_id=POSTGRES_CONN_ID,
        sql="/archive_price_changes.sql",
        parameters={
            "run_id": "{{ ti.xcom_pull(task_ids='start_run') }}",
            "fetch_deals_result": "{{ ti.xcom_pull(task_ids='fetch_deals') | default({}, true) | tojson }}"
        },
    )

    load_deals_to_core_task = SQLExecuteQueryOperator(
        task_id="load_deals_to_core",
        conn_id=POSTGRES_CONN_ID,
        sql="/load_deals_incremental.sql",
        parameters={
            "run_id": "{{ ti.xcom_pull(task_ids='start_run') }}",
            "fetch_deals_result": "{{ ti.xcom_pull(task_ids='fetch_deals') | default({}, true) | tojson }}"
        },
    )

    update_store_checkpoints_task = SQLExecuteQueryOperator(
        task_id="update_store_checkpoints",
        conn_id=POSTGRES_CONN_ID,
        sql="/update_store_checkpoints.sql",
        parameters={
            "stores_json": (
                "{{ ti.xcom_pull(task_ids='fetch_deals')"
                "['loaded_stores_json'] }}"
            ),
        },
    )

    close_run_task = PythonOperator(
        task_id="close_run",
        python_callable=close_run,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    start_run_task >> fetch_store_changes_task
    fetch_store_changes_task >> select_changed_stores_task
    select_changed_stores_task >> fetch_deals_task
    fetch_deals_task >> has_loaded_stores_task
    has_loaded_stores_task >> load_games_to_core_task
    load_games_to_core_task >> archive_price_changes_task
    archive_price_changes_task >> load_deals_to_core_task
    load_deals_to_core_task >> update_store_checkpoints_task
    update_store_checkpoints_task >> close_run_task
