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
        COALESCE(
            NULLIF(item.deal_item->>'savings', '')::NUMERIC,
            0
        ) AS savings,
        NULLIF(item.deal_item->>'dealRating', '')::NUMERIC
            AS deal_rating,
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
        deal_rating,
        source_last_change
    FROM parsed_deals
    WHERE deal_id IS NOT NULL
      AND game_id IS NOT NULL
      AND store_id IS NOT NULL
      AND sale_price IS NOT NULL
      AND normal_price IS NOT NULL
      AND source_last_change IS NOT NULL
    ORDER BY
        game_id,
        store_id,
        source_last_change DESC NULLS LAST
),

valid_deals AS (
    SELECT d.*
    FROM unique_deals AS d
    JOIN core.games AS g
        ON g.game_id = d.game_id
    JOIN core.stores AS s
        ON s.store_id = d.store_id
)

INSERT INTO core.deals_current (
    deal_id,
    game_id,
    store_id,
    sale_price,
    normal_price,
    savings,
    is_on_sale,
    source_last_change,
    discount_calculable,
    deal_rating
)

SELECT
    deal_id,
    game_id,
    store_id,
    sale_price,
    normal_price,
    savings,
    sale_price < normal_price,
    source_last_change,
    normal_price > 0,
    deal_rating
FROM valid_deals

ON CONFLICT (game_id, store_id)
DO UPDATE SET
    deal_id = EXCLUDED.deal_id,
    sale_price = EXCLUDED.sale_price,
    normal_price = EXCLUDED.normal_price,
    savings = EXCLUDED.savings,
    is_on_sale = EXCLUDED.is_on_sale,
    source_last_change = EXCLUDED.source_last_change,
    discount_calculable = EXCLUDED.discount_calculable,
    deal_rating = EXCLUDED.deal_rating

WHERE ROW(
    core.deals_current.deal_id,
    core.deals_current.sale_price,
    core.deals_current.normal_price,
    core.deals_current.savings,
    core.deals_current.is_on_sale,
    core.deals_current.source_last_change,
    core.deals_current.discount_calculable,
    core.deals_current.deal_rating
) IS DISTINCT FROM ROW(
    EXCLUDED.deal_id,
    EXCLUDED.sale_price,
    EXCLUDED.normal_price,
    EXCLUDED.savings,
    EXCLUDED.is_on_sale,
    EXCLUDED.source_last_change,
    EXCLUDED.discount_calculable,
    EXCLUDED.deal_rating
);