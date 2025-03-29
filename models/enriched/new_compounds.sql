WITH new_event AS (
    SELECT 
        id
    FROM {{ ref("new_event") }}
    LIMIT 1
)

SELECT 
    nc.name, 
    nc.max_wear, 
    nc.max_performance,
    ne.id AS event_id
FROM source.log_compounds nc
CROSS JOIN new_event ne
