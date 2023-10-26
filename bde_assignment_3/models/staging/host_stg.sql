{{
    config(
        unique_key=['host_id','scraped_date']
    )
}}

with

source  as (

    select * from {{ ref('host_snapshot') }}

),

cleaned as (
    select
        host_id,
        host_name,
        to_date(host_since, 'DD/MM/YYYY') AS host_since,
        CASE 
            WHEN host_is_superhost IS NULL THEN 'unknown'
            ELSE host_is_superhost
        END AS host_is_superhost,
        dbt_valid_from,
        dbt_valid_to
    from source
),

unknown as (
    select
        '0' as host_id,
        'unknown' as host_name,
        '1900-01-01'::timestamp  as host_since,
        'unknown' as host_is_superhost,
        '1900-01-01'::timestamp  as dbt_valid_from,
        null::timestamp as dbt_valid_to

)
select * from unknown
union all
select * from cleaned