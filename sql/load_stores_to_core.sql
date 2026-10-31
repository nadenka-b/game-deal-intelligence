INSERT INTO core.stores (
    store_id,
    store_name,
    is_active
)
SELECT
    (item.store_item->>'storeID')::INT,
    item.store_item->>'storeName',
    (item.store_item->>'isActive')::INT = 1
FROM raw.raw_api_responses AS r
CROSS JOIN LATERAL jsonb_array_elements(r.payload)
    AS item(store_item)
WHERE r.source = 'cheapshark_stores'
  AND r.run_id = %(run_id)s
  AND item.store_item->>'storeID' IS NOT NULL
  AND item.store_item->>'storeName' IS NOT NULL

ON CONFLICT (store_id)
DO UPDATE SET
    store_name = EXCLUDED.store_name,
    is_active = EXCLUDED.is_active
