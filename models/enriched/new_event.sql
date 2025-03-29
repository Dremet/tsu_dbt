
SELECT 
    {{ dbt_utils.generate_surrogate_key(['utc_start_time', 'host']) }} as id,
    utc_start_time::timestamptz as utc_timestamp, 
    nt.id as track_id,
    host, 
    "eventType" as event_type,
    finished_state, 
    max_laps, 
    max_time_without_start_time, 
    start_time, 
    hotlapping, 
    participants,
    server
FROM source.json_event je
left join {{ ref("new_track") }} nt on nt.track_guid = je.track_guid