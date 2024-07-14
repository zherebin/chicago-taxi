{{
    config(
        materialized='table',
        partition_by={
            "field": "pickup_datetime",
            "data_type": "timestamp",
            "granularity": "month"
        }
    )
}}

with trips as (
    select * from {{ ref('stg__trips_data') }}
),

community_areas as (
    select * from {{ ref('dim__community_areas') }}
),

final as (
    select
        trips.trip_id,
        trips.taxi_id,
        trips.pickup_community_area_id,
        pickup_area.community_area_name as pickup_community_area_name,
        trips.dropoff_community_area_id,
        dropoff_area.community_area_name as dropoff_community_area_name,
        trips.pickup_year,
        trips.pickup_month,
        trips.pickup_datetime,
        trips.dropoff_datetime,
        trips.trip_seconds,
        trips.trip_miles,
        trips.company_name,
        trips.fare_amount,
        trips.tips_amount,
        trips.tolls_amount,
        trips.extras_amount,
        trips.total_amount,
        trips.payment_type,
        trips.pickup_latitude,
        trips.pickup_longitude,
        trips.dropoff_latitude,
        trips.dropoff_longitude
    from trips
        left join community_areas as pickup_area
            on trips.pickup_community_area_id = pickup_area.community_area_id
        left join community_areas as dropoff_area
            on trips.dropoff_community_area_id = dropoff_area.community_area_id
)

select * from final