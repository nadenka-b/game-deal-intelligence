INSERT INTO core.games (
	game_id,
    title,
    internal_name ,
    metacritic_score ,
    steam_rating_text ,
    steam_rating_percent ,
    steam_rating_count ,
    steam_app_id ,
    release_date ,
    thumb
)
SELECT DISTINCT ON (game_id)
    game_id,
    title,
    internal_name,
    metacritic_score,
    steam_rating_text,
    steam_rating_percent,
    steam_rating_count,
    steam_app_id,
    release_date,
    thumb
FROM (
SELECT NULLIF(item.game_item ->>'gameID', '')::INT AS game_id,
	item.game_item ->>'title' AS title,
	item.game_item ->>'internalName' AS internal_name,
	NULLIF(item.game_item ->>'metacriticScore', '')::INT AS metacritic_score,
	item.game_item ->>'steamRatingText' AS steam_rating_text,
	NULLIF(item.game_item ->>'steamRatingPercent', '')::INT AS steam_rating_percent,
	NULLIF(item.game_item ->>'steamRatingCount', '')::INT AS steam_rating_count,
	NULLIF(item.game_item ->>'steamAppID', '')::INT AS steam_app_id,
	CASE
        WHEN NULLIF(item.game_item->>'releaseDate', '')::BIGINT > 0
        THEN TO_TIMESTAMP((item.game_item->>'releaseDate')::BIGINT)
    END AS release_date,
	item.game_item ->>'thumb' AS thumb
FROM raw.raw_api_responses AS r
	CROSS JOIN LATERAL JSONB_ARRAY_ELEMENTS(r.payload) AS item(game_item)
WHERE r.run_id = %(run_id)s
    AND r.source LIKE 'cheapshark_deals_store_%%'
)
WHERE game_id IS NOT NULL 
    AND title IS NOT NULL
ORDER BY game_id

ON CONFLICT (game_id)
DO UPDATE SET
    title = COALESCE(EXCLUDED.title, core.games.title),
    internal_name = COALESCE(EXCLUDED.internal_name, core.games.internal_name),
    metacritic_score = COALESCE(EXCLUDED.metacritic_score, core.games.metacritic_score),
    steam_rating_text = COALESCE(EXCLUDED.steam_rating_text, core.games.steam_rating_text),
    steam_rating_percent = COALESCE(EXCLUDED.steam_rating_percent, core.games.steam_rating_percent),
    steam_rating_count = COALESCE(EXCLUDED.steam_rating_count, core.games.steam_rating_count),
    steam_app_id = COALESCE(EXCLUDED.steam_app_id, core.games.steam_app_id),
    release_date = COALESCE(EXCLUDED.release_date, core.games.release_date),
    thumb = COALESCE(EXCLUDED.thumb, core.games.thumb)
WHERE ROW(
    core.games.title,
    core.games.internal_name,
    core.games.metacritic_score,
    core.games.steam_rating_text,
    core.games.steam_rating_percent,
    core.games.steam_rating_count,
    core.games.steam_app_id,
    core.games.release_date,
    core.games.thumb
) IS DISTINCT FROM ROW(
    COALESCE(EXCLUDED.title, core.games.title),
    COALESCE(EXCLUDED.internal_name, core.games.internal_name),
    COALESCE(EXCLUDED.metacritic_score, core.games.metacritic_score),
    COALESCE(EXCLUDED.steam_rating_text, core.games.steam_rating_text),
    COALESCE(EXCLUDED.steam_rating_percent, core.games.steam_rating_percent),
    COALESCE(EXCLUDED.steam_rating_count, core.games.steam_rating_count),
    COALESCE(EXCLUDED.steam_app_id, core.games.steam_app_id),
    COALESCE(EXCLUDED.release_date, core.games.release_date),
    COALESCE(EXCLUDED.thumb, core.games.thumb)
)