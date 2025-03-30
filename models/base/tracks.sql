{{
    config(
        unique_key=['tr_id']
    )
}}

SELECT 
    id as tr_id,
    track_name as tr_name, 
    track_guid as tr_guid, 
    track_maker_id as tr_maker_id, 
    track_type as tr_type
FROM {{ ref("new_track") }}