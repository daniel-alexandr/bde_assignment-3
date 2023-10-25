{{
    config(
        unique_key='listing_id'
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
        dbt_valid_from
    from source
),

unknown as (
    select
        0 as listing_id,
        'unknown' as room_type,
        '1900-01-01'::timestamp  as dbt_valid_from

)
select * from unknown
union all
select * from cleaned