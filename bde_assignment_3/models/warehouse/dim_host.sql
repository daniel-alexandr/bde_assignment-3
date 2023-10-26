{{
    config(
        unique_key=['host_id','scraped_date']
    )
}}

select * from {{ ref('host_stg') }}