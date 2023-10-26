{{
    config(
        unique_key=['listing_id','scraped_date']
    )
}}

with

source  as (

    select * from {{ ref('neighbourhood_snapshot') }}

),

cleaned as (
    select
        listing_id,
        listing_neighbourhood,
        CASE 
            WHEN host_neighbourhood ='NaN' THEN 'unknown'
            ELSE host_neighbourhood
        END AS host_neighbourhood,
        dbt_valid_from,
        dbt_valid_to
    from source
),

unknown as (
    select
        0 as listing_id,
        'unknown' as listing_neighbourhood,
        'unknown' as host_neighbourhood,
        '1900-01-01'::timestamp  as dbt_valid_from,
        null::timestamp as dbt_valid_to

)
select * from unknown
union all
select * from cleaned