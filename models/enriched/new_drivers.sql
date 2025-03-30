
SELECT 
    case 
        when ai then {{ dbt_utils.generate_surrogate_key(['jd.local_index','jd.steam_id','jd.name']) }}
        else {{ dbt_utils.generate_surrogate_key(['jd.steam_id']) }}
    end as id,
    nt.id as team_id,
    jd.index, 
    jd.local_index,
    jd.name, 
    jd.steam_id, 
    jd.ai, 
    jd.flag, 
    jd.vehicle_name, 
    jd.vehicle_guid, 
    jd.start_position
FROM source.json_drivers jd 
left join {{ ref("new_teams") }} nt on jd.clan = nt.name
