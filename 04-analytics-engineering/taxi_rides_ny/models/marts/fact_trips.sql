{{
    config(
        materialized='table'
    )
}}

with
green_trips as (
    select
        *,
        'green' as service_type
    from {{ ref('stg_green_tripdata') }}
),

yellow_trips as (
    select
        *,
        'yellow' as service_type
    from {{ ref('stg_yellow_tripdata') }}
),

-- Union both datasets
all_trips as (
    select * from green_trips
    union all
    select * from yellow_trips
),

-- Deduplicate: raw data can contain exact duplicate rows.
-- We partition by the natural key columns that identify a unique trip
-- and keep only the first occurrence.
deduplicated as (
    select
        *,
        row_number() over (
            partition by
                service_type,
                vendor_id,
                pickup_datetime,
                dropoff_datetime,
                pickup_location_id,
                dropoff_location_id,
                passenger_count,
                trip_distance,
                fare_amount,
                total_amount
            order by pickup_datetime
        ) as row_num
    from all_trips
),

unique_trips as (
    select * from deduplicated
    where row_num = 1
)

select
    -- Surrogate primary key: hash of service_type + natural key columns
    {{ dbt_utils.generate_surrogate_key([
        'service_type',
        'vendor_id',
        'pickup_datetime',
        'dropoff_datetime',
        'pickup_location_id',
        'dropoff_location_id',
        'passenger_count',
        'trip_distance',
        'fare_amount',
        'total_amount'
    ]) }} as trip_id,

    -- Enriched columns
    {{ get_vendors_names('vendor_id') }} as vendor_name,
    {{ get_payment_type_name('payment_type') }} as payment_type_name,

    -- All original columns
    service_type,
    vendor_id,
    rate_code_id,
    pickup_location_id,
    dropoff_location_id,
    pickup_datetime,
    dropoff_datetime,
    store_and_fwd_flag,
    passenger_count,
    trip_distance,
    trip_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    ehail_fee,
    improvement_surcharge,
    total_amount,
    payment_type
from unique_trips
