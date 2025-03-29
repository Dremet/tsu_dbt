WITH new_event AS (
    SELECT 
        id
    FROM {{ ref("new_event") }}
    LIMIT 1
)
SELECT 
    {{ dbt_utils.generate_surrogate_key(['ne.id','nd.id','nv.id']) }} as id,
    ne.id as event_id,
    nd.id as driver_id,
    nv.id as vehicle_id
FROM source.json_drivers jd 
CROSS JOIN new_event ne
LEFT JOIN {{ ref("new_drivers") }} nd 
    on 
        (not nd.ai and jd.steam_id = nd.steam_id)
        or 
        (nd.ai and jd.steam_id = nd.steam_id and jd.local_index = nd.local_index and jd.name = nd.name)
LEFT JOIN {{ ref("new_vehicles") }} nv on jd.vehicle_guid = nv.vehicle_guid