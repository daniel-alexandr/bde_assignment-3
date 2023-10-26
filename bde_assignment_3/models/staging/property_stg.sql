{{
    config(
        unique_key=['listing_id','scraped_date']
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
        accommodates,
        dbt_valid_from,
        dbt_valid_to
    from source
),

unknown as (
    select
        0 as listing_id,
        'unknown' as property_type,
        0 as accomodates,
        '1900-01-01'::timestamp  as dbt_valid_from,
        null::timestamp as dbt_valid_to

)
select * from unknown
union all
select * from cleaned