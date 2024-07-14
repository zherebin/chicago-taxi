
-- trips per month
select 
  date_trunc(pickup_datetime, month) as month,
  count(trip_id) as trips
from `vertical-orbit-426819-f8.trips_data_dev.fact__trips`
where extract(year from pickup_datetime) >= 2019
group by month;

-- active taxi per day
with data as (
select 
  date(pickup_datetime) as date,
  count(distinct taxi_id) as taxis
from `vertical-orbit-426819-f8.trips_data_dev.fact__trips`
where extract(year from pickup_datetime) >= 2019
group by date
)

select 
  date,
  taxis,
  avg(taxis) over(order by date asc rows between 29 preceding and current row) as rolling_avg_taxis_30
from data;

-- trips per active taxi per day
with data as (
select 
  date(pickup_datetime) as date,
  taxi_id,
  count(trip_id) as trips
from `vertical-orbit-426819-f8.trips_data_dev.fact__trips`
where extract(year from pickup_datetime) >= 2019
group by date, taxi_id
),

final as (
select
  date,
  avg(trips) as avg_trips
from data
group by date
)

select 
  date,
  avg_trips,
  avg(avg_trips) over(order by date asc rows between 29 preceding and current row) as rolling_avg_trips_30
from final


-- companies grouped by fleet size

with data as (
select
  company_name,
  count(distinct taxi_id) as taxis
from `vertical-orbit-426819-f8.trips_data_dev.fact__trips`
where extract(year from pickup_datetime) = 2023
group by company_name
)

select
  case
    when taxis <= 10 then '1 - 10'
    when taxis <= 100 then '11 - 100'
    when taxis <= 250 then '101 - 250'
    when taxis <= 500 then '251 - 500'
    else '500+'
  end as company_size,
  sum(taxis) as taxis,
  count(company_name)
from data
group by company_size


-- trips share by company

with total as (
  select 
    date(date_trunc(pickup_datetime, month)) as month,
    count(trip_id) as total_trips
  from `vertical-orbit-426819-f8.trips_data_dev.fact__trips`
  where extract(year from pickup_datetime) = 2023
  group by month
),

companies_trips as (
select
  date(date_trunc(pickup_datetime, month)) as month,
  case
    when company_name in (
      'Taxi Affiliation Services',
      'Flash Cab',
      'Taxicab Insurance Agency LLC',
      'Sun Taxi',
      'City Service',
      'Chicago Independents',
      '5 Star Taxi',
      'Globe Taxi',
      'Medallion Leasin',
      'Blue Ribbon Taxi Association Inc'
    ) then company_name
    else 'Others'
  end as company_name,
  count(trip_id) as trips
from `vertical-orbit-426819-f8.trips_data_dev.fact__trips`
where extract(year from pickup_datetime) = 2023
group by month, company_name
)

select
  companies_trips.month,
  companies_trips.company_name,
  companies_trips.trips,
  companies_trips.trips / total.total_trips
from companies_trips
  join total on companies_trips.month = total.month

-- 

with data_2022 as (
  select 
    company_name,
    count(distinct taxi_id) as active_taxis_22,
    count(trip_id) as total_trips_22
  from `vertical-orbit-426819-f8.trips_data_dev.fact__trips`
  where extract(year from pickup_datetime) = 2022
    and company_name in (
    'Taxi Affiliation Services',
    'Flash Cab',
    'Taxicab Insurance Agency LLC',
    'Sun Taxi',
    'City Service',
    'Chicago Independents',
    '5 Star Taxi',
    'Globe Taxi',
    'Medallion Leasin',
    'Blue Ribbon Taxi Association Inc'
  )
  group by company_name
),

data_2023 as (
  select 
    company_name,
    count(distinct taxi_id) as active_taxis_23,
    count(trip_id) as total_trips_23
  from `vertical-orbit-426819-f8.trips_data_dev.fact__trips`
  where extract(year from pickup_datetime) = 2023
      and company_name in (
    'Taxi Affiliation Services',
    'Flash Cab',
    'Taxicab Insurance Agency LLC',
    'Sun Taxi',
    'City Service',
    'Chicago Independents',
    '5 Star Taxi',
    'Globe Taxi',
    'Medallion Leasin',
    'Blue Ribbon Taxi Association Inc'
  )
  group by company_name
)

select
  data_2023.company_name,
  data_2022.active_taxis_22,
  data_2023.active_taxis_23,
  data_2022.total_trips_22,
  data_2023.total_trips_23
from data_2023
  join data_2022 using(company_name)


-- popular areas and routes

with data as (
select
  pickup_community_area_name,
  dropoff_community_area_name,
  count(trip_id) as route_trips
from `vertical-orbit-426819-f8.trips_data_dev.fact__trips`
where extract(year from pickup_datetime) = 2023
group by pickup_community_area_name, dropoff_community_area_name
),

final as (
select
  pickup_community_area_name,
  dropoff_community_area_name,
  route_trips,
  row_number() over(partition by pickup_community_area_name order by route_trips desc) as rn,
  sum(route_trips) over(partition by pickup_community_area_name) as area_trips,
from data
)

select 
  pickup_community_area_name,
  dropoff_community_area_name,
  route_trips,
  area_trips
from final
where rn < 4
order by area_trips desc, route_trips desc


-- areas 2022 and 2023 trips

with data_2022 as (
select
  pickup_community_area_name,
  count(trip_id) as area_trips_2022
from `vertical-orbit-426819-f8.trips_data_dev.fact__trips`
where extract(year from pickup_datetime) = 2022
group by pickup_community_area_name
),

data_2023 as (
select
  pickup_community_area_name,
  count(trip_id) as area_trips_2023
from `vertical-orbit-426819-f8.trips_data_dev.fact__trips`
where extract(year from pickup_datetime) = 2023
group by pickup_community_area_name
)

select
  data_2023.pickup_community_area_name,
  area_trips_2022,
  area_trips_2023,
  round((area_trips_2023 - area_trips_2022) / area_trips_2022 * 100, 0) as yoy
from data_2023
  join data_2022 using(pickup_community_area_name)
order by yoy desc

-- HEATMAP for area popeular hours within day

select
  pickup_community_area_name,
  format_date('%a', pickup_datetime) as weekday,
  extract(hour from pickup_datetime) as hour,
  count(trip_id) as trips
from `vertical-orbit-426819-f8.trips_data_dev.fact__trips`
where extract(year from pickup_datetime) = 2023
group by pickup_community_area_name, weekday, hour

-- payment methods

select
  pickup_community_area_name,
  payment_type,
  count(trip_id) as area_trips
from `vertical-orbit-426819-f8.trips_data_dev.fact__trips`
where extract(year from pickup_datetime) = 2023
group by
  pickup_community_area_name,
  payment_type