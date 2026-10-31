import logging
import time

from airflow.exceptions import AirflowException
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

from game_deals.constants import CHEAPSHARK_CONN_ID, PAGE_SIZE, API_RETRY_DELAY_SECONDS
from game_deals.common import save_response, get_run_id

logger = logging.getLogger(__name__)


class RateLimitReached(AirflowException):
    """CheapShark вернул HTTP 429."""

    pass


def fetch_and_save_raw(endpoint, source, ti, params=None, paginate=False, page_size=PAGE_SIZE, retry_delay=API_RETRY_DELAY_SECONDS):
    """
    Получает данные CheapShark API и сохраняет ответы в raw.

    endpoint: API endpoint, например 'deals' или 'stores'
    source: имя источника для записи в raw
    ti: контекст задачи Airflow для получения run_id из XCom
    params: дополнительные параметры API
    paginate: нужно ли загружать все страницы
    page_size: количество записей на странице для пагинации
    """
    run_id = get_run_id(ti)

    http_hook = HttpHook(method="GET", http_conn_id=CHEAPSHARK_CONN_ID)

    base_params = dict(params or {})
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }

    def fetch_page(page_number, request_params):
        """Запрашивает одну страницу и сохраняет исходный ответ."""
        time.sleep(retry_delay)

        response = http_hook.run(
            endpoint=endpoint,
            data=request_params,
            headers=headers,
            extra_options={"check_response": False},
        )

        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After")

            logger.warning(
                "HTTP 429 от CheapShark. "
                "Endpoint=%s, source=%s, page=%s, Retry-After=%s. "
                "Прекращение дальнейших API-запросов.",
                endpoint,
                source,
                page_number,
                retry_after,
            )

            raise RateLimitReached(
                f"HTTP 429 для {source}, страница {page_number}; "
                f"Retry-After={retry_after}"
            )

        response.raise_for_status()

        try:
            payload = response.json()
        except ValueError as exc:
            raise ValueError(
                f"API вернул невалидный JSON: endpoint={endpoint}, "
                f"page={page_number}"
            ) from exc

        if not isinstance(payload, (list, dict)):
            raise ValueError(
                f"Неожиданный формат ответа: endpoint={endpoint}, "
                f"page={page_number}"
            )

        save_response(
            run_id=run_id,
            source=source,
            page_number=page_number,
            page_size=page_size if paginate else None,
            payload=payload,
        )

        records_count = (
            len(payload) if isinstance(payload, list) else 1
        )

        logging.info(
            "Сохранено: source=%s page=%s records=%s run_id=%s",
            source,
            page_number,
            records_count,
            run_id,
        )

        return response, payload

    if not paginate:
        # Endpoint без пагинации, например stores.
        _, payload = fetch_page(
            page_number=0,
            request_params=base_params,
        )

        return {
            "run_id": run_id,
            "source": source,
            "total_pages": 1,
            "records": (
                len(payload) if isinstance(payload, list) else 1
            ),
        }

    first_params = {
        **base_params,
        "pageNumber": 0,
        "pageSize": page_size,
    }

    response, first_payload = fetch_page(
        page_number=0,
        request_params=first_params,
    )

    total_pages_header = response.headers.get(
        "X-Total-Page-Count"
    )

    if total_pages_header is None:
        raise ValueError(
            "API не вернул заголовок X-Total-Page-Count "
            f"для endpoint={endpoint}"
        )

    try:
        total_pages = int(total_pages_header)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"Некорректный X-Total-Page-Count: "
            f"{total_pages_header!r}"
        ) from exc

    if total_pages < 0:
        raise ValueError(
            f"Некорректное количество страниц: {total_pages}"
        )

    for page_number in range(1, total_pages + 1):
        request_params = {
            **base_params,
            "pageNumber": page_number,
            "pageSize": page_size,
        }

        fetch_page(
            page_number=page_number,
            request_params=request_params,
        )

    logging.info(
        "Завершено: source=%s run_id=%s total_pages=%s",
        source,
        run_id,
        total_pages,
    )

    return {
        "run_id": run_id,
        "source": source,
        "total_pages": total_pages,
        "page_size": page_size,
    }


def close_run(ti):
    """Закрыть запуск, учитывая состояние остальных задач."""
    run_id = get_run_id(ti)
    dag_run = ti.get_dagrun()

    failed_states = {"failed", "upstream_failed"}

    task_instances = dag_run.get_task_instances()
    task_states = {
        task_instance.task_id: task_instance.state for task_instance in task_instances}

    failed_tasks = [
        task_id for task_id, state in task_states.items()
        if task_id != "close_run" and state in failed_states
    ]

    rate_limited = bool(
        ti.xcom_pull(
            task_ids="fetch_deals",
            key="ingestion_rate_limited",
        )
    )

    pg_hook = PostgresHook(postgres_conn_id="game_deals_postgres")

    if failed_tasks:
        logging.error(
            "Пайплайн run_id=%s столкнулся с ошибками в тасках: tasks=%s",
            run_id,
            failed_tasks,
        )
        pg_hook.run(
            """
            UPDATE raw.pipeline_runs
            SET status = %s,
                finished_at = NOW(),
                error_message = %s
            WHERE run_id = %s
              AND status = 'running';
            """,
            parameters=(
                "failed",
                f"Failed tasks={failed_tasks}",
                run_id,
            ),
        )

    elif rate_limited:
        logging.info(
            "Пайплайн run_id=%s частично завершен",
            run_id,
        )
        rate_limit_result = ti.xcom_pull(
            task_ids="fetch_deals",
            key="return_value",
        ) or {}
        pg_hook.run(
            """
            UPDATE raw.pipeline_runs
            SET status = %s,
                finished_at = NOW(),
                error_message = %s
            WHERE run_id = %s
              AND status = 'running';
            """,
            parameters=("partial",
                        rate_limit_result.get(
                            "reason", "CheapShark API rate limit (HTTP 429)"),
                        run_id),
        )
    else:
        logging.info(
            "Пайплайн run_id=%s успешно завершен",
            run_id,
        )
        pg_hook.run(
            """
            UPDATE raw.pipeline_runs
            SET status = %s,
                finished_at = NOW(),
                error_message = NULL
            WHERE run_id = %s
              AND status = 'running';
            """,
            parameters=("success", run_id),
        )
