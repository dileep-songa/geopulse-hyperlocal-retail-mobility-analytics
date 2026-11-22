-- Asserts that (ping_id, store_id) pairs are strictly unique in stg_store_catchment_memberships.
-- Fails if any pair appears more than once, as Snowflake does not enforce standard PRIMARY KEY constraints.
SELECT
    ping_id,
    store_id,
    COUNT(*) AS occurrence_count
FROM {{ ref('stg_store_catchment_memberships') }}
GROUP BY
    ping_id,
    store_id
HAVING COUNT(*) > 1
