CREATE OR REPLACE VIEW marts.mart_current_deals AS 
SELECT 
	d.game_id,
    g.title AS game_name,
    d.store_id,
    s.store_name,
    d.normal_price,
    d.sale_price,
    d.savings AS discount_percent,
    d.normal_price - d.sale_price AS benefit,
    d.deal_id
FROM core.deals_current d 
	JOIN core.games g USING (game_id)
	JOIN core.stores s USING(store_id)
