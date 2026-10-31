WITH successful_stores AS (
    SELECT value::INT AS store_id
    FROM jsonb_array_elements_text(
        COALESCE(
            ((%(fetch_deals_result)s::jsonb)->>'loaded_store_ids_json')::jsonb, 
            '[]'::jsonb
        )
    )
),

parsed_deals AS (
    SELECT
        item.deal_item->>'dealID' AS deal_id,
        NULLIF(item.deal_item->>'gameID', '')::INT AS game_id,
        NULLIF(item.deal_item->>'storeID', '')::INT AS store_id,
        NULLIF(item.deal_item->>'salePrice', '')::NUMERIC
            AS sale_price,
        NULLIF(item.deal_item->>'normalPrice', '')::NUMERIC
            AS normal_price,
        NULLIF(item.deal_item->>'savings', '')::NUMERIC
            AS savings,
        to_timestamp(
            NULLIF(item.deal_item->>'lastChange', '')::BIGINT
        ) AS source_last_change

    FROM raw.raw_api_responses AS r

    JOIN successful_stores AS ss
        ON r.source = 'cheapshark_deals_store_' || ss.store_id::TEXT

    CROSS JOIN LATERAL jsonb_array_elements(r.payload)
        AS item(deal_item)

    WHERE r.run_id = %(run_id)s
),

unique_deals AS (
    SELECT DISTINCT ON (game_id, store_id)
        deal_id,
        game_id,
        store_id,
        sale_price,
        normal_price,
        savings,
        source_last_change
    FROM parsed_deals
    WHERE game_id IS NOT NULL
      AND store_id IS NOT NULL
      AND sale_price IS NOT NULL
      AND normal_price IS NOT NULL
      AND source_last_change IS NOT NULL
    ORDER BY
        game_id,
        store_id,
        source_last_change DESC NULLS LAST
)

INSERT INTO core.deals_price_history (
    game_id,
    store_id,
    deal_id,
    sale_price,
    normal_price,
    savings,
    is_on_sale,
    source_last_change,
    observed_at,
    run_id
)

SELECT
    current_deal.game_id,
    current_deal.store_id,
    current_deal.deal_id,
    current_deal.sale_price,
    current_deal.normal_price,
    current_deal.savings,
    current_deal.is_on_sale,
    current_deal.source_last_change,
    NOW(),
    %(run_id)s

FROM core.deals_current AS current_deal

JOIN unique_deals AS incoming
    ON incoming.game_id = current_deal.game_id
   AND incoming.store_id = current_deal.store_id

WHERE ROW(
    current_deal.sale_price,
    current_deal.normal_price,
    current_deal.savings,
    current_deal.is_on_sale
) IS DISTINCT FROM ROW(
    incoming.sale_price,
    incoming.normal_price,
    COALESCE(incoming.savings, 0),
    incoming.sale_price < incoming.normal_price
)

AND NOT EXISTS (
    SELECT 1
    FROM core.deals_price_history AS history
    WHERE history.run_id = %(run_id)s
      AND history.game_id = current_deal.game_id
      AND history.store_id = current_deal.store_id
);