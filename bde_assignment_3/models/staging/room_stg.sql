{{
    config(
        unique_key=['listing_id','scraped_date']
    )
}}

with

source  as (

    select * from {{ ref('room_snapshot') }}

),

cleaned as (
    select
        listing_id,
        room_type,
        dbt_valid_from,
        dbt_valid_to
    from source
),

unknown as (
    select
        0 as listing_id,
        'unknown' as room_type,
        '1900-01-01'::timestamp  as dbt_valid_from,
        null::timestamp as dbt_valid_to

)
select * from unknown
union all
select * from cleaned