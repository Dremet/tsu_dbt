with teams_uppercase as (
    SELECT 
        upper(clan) as team_upper,
        clan as team
    FROM source.json_drivers
)
SELECT 
    {{ dbt_utils.generate_surrogate_key(['team_upper']) }} as id,
    team as name
FROM teams_uppercase
