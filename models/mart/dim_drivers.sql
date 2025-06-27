{{ config(
    materialized='view'
) }}

SELECT *
FROM {{ ref('drivers') }}