# Airflow connections
POSTGRES_CONN_ID = "game_deals_postgres"
CHEAPSHARK_CONN_ID = "api_cheapshark"
PATH_TO_SQL = "/opt/airflow/sql"

# CheapShark API
STORES_ENDPOINT = "stores"
DEALS_ENDPOINT = "deals"

DEALS_MIN_REVIEW_COUNT = 10_000

PAGE_SIZE = 60
API_RETRY_DELAY_SECONDS = 8

STORES_SOURCE = "cheapshark_stores"
DEALS_STORE_SOURCE = "cheapshark_deals_store_"
CHANGES_SOURCE = "cheapshark_store_changes"
