{{
    config(
        unique_key=['d_id']
    )
}}

SELECT 
    id as d_id,
    team_id as te_te_id,
    name as d_name, 
    steam_id as d_steam_id, 
    ai as d_is_ai, 
    flag as d_flag
FROM {{ ref("new_drivers") }}
