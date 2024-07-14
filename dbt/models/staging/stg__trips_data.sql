{{
    config(
        materialized='view'
    )
}}

with trips_data as (

    select * from {{ source('staging', 'taxi_data_external') }}

    where True
        and trip_start_timestamp is not null
        and trip_end_timestamp is not null
        and fare is not null
        and trip_seconds is not null
        and trip_miles is not null
        and not (trip_total = 0 and payment_type <> 'No Charge')
        and taxi_id <> 'NA'

),

companies_mapping as (
    select * from {{ ref('companies_mapping') }}
),

final as (

    select
        -- identifiers
        trip_id,
        taxi_id,
        coalesce(cast(pickup_community_area as integer), 0) as pickup_community_area_id,
        coalesce(cast(dropoff_community_area as integer), 0) as dropoff_community_area_id,

        -- timestamps
        year as pickup_year,
        month as pickup_month,
        timestamp_micros(cast(trip_start_timestamp / 1000 as integer)) as pickup_datetime,
        timestamp_micros(cast(trip_end_timestamp / 1000 as integer)) as dropoff_datetime,
        
        -- trip info
        cast(trip_seconds as integer) as trip_seconds,
        cast(trip_miles as numeric) as trip_miles,
        companies_mapping.company_name_new as company_name,

        -- payment info
        cast(fare as numeric) as fare_amount,
        cast(tips as numeric) as tips_amount,
        cast(tolls as numeric) as tolls_amount,
        cast(extras as numeric) as extras_amount,
        cast(trip_total as numeric) as total_amount,
        case payment_type
            when 'Pcard' then 'Prcard'
            else payment_type
        end as payment_type,
        
        -- geo info
        cast(pickup_centroid_latitude as numeric) as pickup_latitude,
        cast(pickup_centroid_longitude as numeric) as pickup_longitude,
        cast(dropoff_centroid_latitude as numeric) as dropoff_latitude,
        cast(dropoff_centroid_longitude as numeric) as dropoff_longitude

    from trips_data
        join companies_mapping
            on trips_data.company = companies_mapping.company_name_old

)

select * from final


-- {% if var('is_test_run', default=True) %}

--     limit 100

-- {% endif %}
