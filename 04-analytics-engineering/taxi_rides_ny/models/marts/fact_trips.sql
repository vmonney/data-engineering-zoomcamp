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

-- Deduplicate using GROUP BY instead of ROW_NUMBER() window function.
-- GROUP BY uses hash aggregation which DuckDB can spill to disk,
-- while window functions over 100M+ rows exceed the memory limit.
unique_trips as (
    select
        service_type,
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        pickup_location_id,
        dropoff_location_id,
        passenger_count,
        trip_distance,
        fare_amount,
        total_amount,
        min(rate_code_id) as rate_code_id,
        min(store_and_fwd_flag) as store_and_fwd_flag,
        min(trip_type) as trip_type,
        min(extra) as extra,
        min(mta_tax) as mta_tax,
        min(tip_amount) as tip_amount,
        min(tolls_amount) as tolls_amount,
        min(ehail_fee) as ehail_fee,
        min(improvement_surcharge) as improvement_surcharge,
        min(payment_type) as payment_type
    from all_trips
    group by
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
)

select
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

    {{ get_vendors_names('vendor_id') }} as vendor_name,
    {{ get_payment_type_name('payment_type') }} as payment_type_name,

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
