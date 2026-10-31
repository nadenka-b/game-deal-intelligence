import json
import logging

from airflow.exceptions import AirflowException
from airflow.providers.postgres.hooks.postgres import PostgresHook

from game_deals.constants import POSTGRES_CONN_ID

logger = logging.getLogger(__name__)


def get_run_id(ti, task_id="start_run"):
    """Получить ID запуска, созданный задачей task_id."""
    run_id = ti.xcom_pull(task_ids=task_id)

    if run_id is None:
        raise AirflowException(
            f"Не найден run_id в XCom задачи {task_id}"
        )

    return int(run_id)


def start_run():
    """Создать запись о запуске pipeline."""
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    row = pg.get_first("""
        INSERT INTO raw.pipeline_runs (status)
        VALUES ('running')
        RETURNING run_id;
    """)

    run_id = int(row[0])
    logger.info("Начат пайплайн run_id=%s", run_id)

    return run_id


def save_response(
    run_id,
    source,
    payload,
    page_number=0,
    page_size=None,
):
    """Сохранить исходный ответ API в raw.raw_api_responses."""
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    count_result = (
        len(payload)
        if isinstance(payload, (list, dict))
        else None
    )

    pg.run(
        """
        INSERT INTO raw.raw_api_responses (
            run_id,
            source,
            page_number,
            page_size,
            count_result,
            payload
        )
        VALUES (%s, %s, %s, %s, %s, %s::jsonb);
        """,
        parameters=(
            run_id,
            source,
            page_number,
            page_size,
            count_result,
            json.dumps(payload),
        ),
    )
