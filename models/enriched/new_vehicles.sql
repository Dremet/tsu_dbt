
SELECT 
    distinct
    {{ dbt_utils.generate_surrogate_key(['vehicle_guid']) }} as id,
    vehicle_name, 
    vehicle_guid
FROM source.json_drivers