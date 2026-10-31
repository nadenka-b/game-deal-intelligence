WITH parsed_deals AS (
    SELECT
        item.deal_item->>'dealID' AS deal_id,
        (item.deal_item->>'gameID')::INT AS game_id,
        (item.deal_item->>'storeID')::INT AS store_id,
        (item.deal_item->>'salePrice')::NUMERIC AS sale_price,
        (item.deal_item->>'normalPrice')::NUMERIC AS normal_price,
        (item.deal_item->>'savings')::NUMERIC AS savings,
        (item.deal_item->>'isOnSale')::BOOLEAN AS is_on_sale,
        (item.deal_item->>'dealRating')::NUMERIC AS deal_rating,
        to_timestamp(
            (item.deal_item->>'lastChange')::BIGINT
        ) AS source_last_change
    FROM raw.raw_api_responses AS r
    CROSS JOIN LATERAL jsonb_array_elements(r.payload)
        AS item(deal_item)
    WHERE r.run_id = %(run_id)s
      AND r.source LIKE 'cheapshark_deals_store_%%'
),
unique_deals AS (
    SELECT DISTINCT ON (game_id, store_id)
        deal_id,
        game_id,
        store_id,
        sale_price,
        normal_price,
        savings,
        is_on_sale,
        deal_rating,
        source_last_change
    FROM parsed_deals
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
    deal_rating,
    source_last_change,
    discount_calculable
)
SELECT
    deal_id,
    game_id,
    store_id,
    sale_price,
    normal_price,
    COALESCE(savings, 0),
    is_on_sale,
    deal_rating,
    source_last_change,
    normal_price > 0 AS discount_calculable
FROM valid_deals

ON CONFLICT (game_id, store_id)
DO UPDATE SET
    deal_id = COALESCE(
        EXCLUDED.deal_id,
        core.deals_current.deal_id
    ),
    sale_price = COALESCE(
        EXCLUDED.sale_price,
        core.deals_current.sale_price
    ),
    normal_price = COALESCE(
        EXCLUDED.normal_price,
        core.deals_current.normal_price
    ),
    savings = COALESCE(
        EXCLUDED.savings,
        core.deals_current.savings
    ),
    deal_rating = COALESCE(
        EXCLUDED.deal_rating,
        core.deals_current.deal_rating
    ),
    source_last_change = COALESCE(
        EXCLUDED.source_last_change,
        core.deals_current.source_last_change
    )
WHERE ROW(
    core.deals_current.deal_id,
    core.deals_current.sale_price,
    core.deals_current.normal_price,
    core.deals_current.savings,
    core.deals_current.deal_rating,
    core.deals_current.source_last_change
) IS DISTINCT FROM ROW(
    COALESCE(EXCLUDED.deal_id, core.deals_current.deal_id),
    COALESCE(EXCLUDED.sale_price, core.deals_current.sale_price),
    COALESCE(EXCLUDED.normal_price, core.deals_current.normal_price),
    COALESCE(EXCLUDED.savings, core.deals_current.savings),
    COALESCE(EXCLUDED.deal_rating, core.deals_current.deal_rating),
    COALESCE(
        EXCLUDED.source_last_change,
        core.deals_current.source_last_change
    )
);