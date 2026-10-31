CREATE OR REPLACE VIEW marts.mart_game_price_history AS
SELECT
    h.history_id AS record_id,
    h.game_id,
    g.title AS game_title,
    g.thumb AS game_thumb,
    h.store_id,
    s.store_name,
    h.deal_id,
    h.sale_price,
    h.normal_price,
    h.savings,
    h.is_on_sale,
    h.observed_at AS price_date,
    'history' AS record_type,
    h.run_id
FROM core.deals_price_history AS h
JOIN core.games AS g
    ON g.game_id = h.game_id
JOIN core.stores AS s
    ON s.store_id = h.store_id
UNION ALL
SELECT
    NULL::BIGINT AS record_id,
    c.game_id,
    g.title AS game_title,
    g.thumb AS game_thumb,
    c.store_id,
    s.store_name,
    c.deal_id,
    c.sale_price,
    c.normal_price,
    c.savings,
    c.is_on_sale,
    c.source_last_change AS price_date,
    'current' AS record_type,
    NULL::BIGINT AS run_id
FROM core.deals_current AS c
JOIN core.games AS g
    ON g.game_id = c.game_id
JOIN core.stores AS s
    ON s.store_id = c.store_id;
