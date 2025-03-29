{{
    config(
        unique_key=['t_id']
    )
}}

SELECT 
    id as t_id,
    track_name as t_name, 
    track_guid as t_guid, 
    track_maker_id as t_maker_id, 
    track_type as t_type
FROM {{ ref("new_track") }}