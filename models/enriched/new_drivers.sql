
SELECT 
    case 
        when ai then {{ dbt_utils.generate_surrogate_key(['local_index','steam_id','name']) }}
        else {{ dbt_utils.generate_surrogate_key(['steam_id']) }}
    end as id,
    nt.id as team_id,
    index, 
    local_index,
    name, 
    steam_id, 
    ai, 
    flag, 
    vehicle_name, 
    vehicle_guid, 
    start_position
FROM source.json_drivers jd 
left join {{ ref("new_teams") }} nt on jd.clan = nt.name
