{{
    config(
        unique_key='listing_id'
    )
}}

with

source  as (

    select * from {{ ref('property_snapshot') }}

),

cleaned as (
    select
        listing_id,
        property_type,
        accomodates,
        dbt_valid_from
    from source
),

unknown as (
    select
        0 as listing_id,
        'unknown' as property_type,
        0 as accomodates,
        '1900-01-01'::timestamp  as dbt_valid_from

)
select * from unknown
union all
select * from cleaned