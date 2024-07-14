{{
    config(
        materialized='table'
    )
}}

select
    Number as community_area_id,
    Name as community_area_name
from {{ ref('community_areas') }}