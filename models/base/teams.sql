{{
    config(
        unique_key=['te_id']
    )
}}

SELECT 
    -- team name has been converted to uppercase before surrogate key was generated
    -- so vsr and VSR would be one entry
    id as te_id,
    name as te_name
FROM {{ ref("new_teams") }}