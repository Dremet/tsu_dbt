
SELECT 
    {{ dbt_utils.generate_surrogate_key(['track_guid']) }} as id,
    track_name, 
    track_guid, 
    track_maker_id, 
    track_type
FROM source.json_event