-- Unencrypted site url
CREATE VIEW v_site_url AS SELECT 'https://metabase.rdxz2.site' AS site_url;

-- MV: collections with structure
CREATE MATERIALIZED VIEW mv_collection_parent AS
WITH cte_collection AS (
    SELECT collection.id AS id
        , collection.location AS "location"
        , collection.name AS "name"
    FROM collection
)
, cte_ordinality AS (
    SELECT cte_collection.id
        , cte_collection.name
        , NULLIF(t.folder_location_part, '')::INTEGER AS folder_location_part
        , t.ordinality
    FROM cte_collection,
    LATERAL REGEXP_SPLIT_TO_TABLE(TRIM(BOTH '/' FROM cte_collection.location), '/') WITH ORDINALITY t (folder_location_part, ORDINALITY)
)
, cte_collection_structure AS (
    SELECT cte_ordinality.id
        , CASE WHEN min(cte_ordinality.folder_location_part) IS NOT NULL THEN  ARRAY_AGG(collection.name) || ARRAY[MIN(cte_ordinality.name)] ELSE NULL END AS parents
    FROM cte_ordinality
    LEFT JOIN collection ON collection.id = cte_ordinality.folder_location_part
    GROUP BY cte_ordinality.id, cte_ordinality.name
)
SELECT CURRENT_TIMESTAMP AS data_ts
    , cte_collection_structure.id
    , cte_collection_structure.parents
FROM cte_collection_structure;
CREATE INDEX ix__mv_collection_structure__id ON mv_collection_parent(id);

-- Fun: generate complete Metabase url (question, dashboard, collection)
CREATE OR REPLACE FUNCTION fun_generate_metabase_url(in_site_url TEXT, in_type TEXT, in_id INT, in_name TEXT)
RETURNS TEXT AS $$
BEGIN
    RETURN in_site_url || '/' || in_type || '/' || in_id || '-' || LOWER(REGEXP_REPLACE(in_name, '[^a-zA-Z0-9]', '-', 'g'));
END;
$$ LANGUAGE plpgsql;
