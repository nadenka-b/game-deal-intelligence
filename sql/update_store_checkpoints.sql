UPDATE core.stores AS stores
SET last_change_unix = incoming.last_change_unix

FROM jsonb_to_recordset(
    %(stores_json)s::jsonb
) AS incoming (
    store_id INT,
    last_change_unix BIGINT
)

WHERE stores.store_id = incoming.store_id;