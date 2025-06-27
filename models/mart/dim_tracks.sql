{{ config(
    materialized='view'
) }}

SELECT *
FROM {{ ref('tracks') }}