{{ config(
    materialized='view'
) }}

SELECT *
FROM {{ ref('vehicles') }}