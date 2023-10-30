{{
    config(
        unique_key=['listing_neighbourhood','month','year']
    )
}}


with source as (
    select *,
    EXTRACT(MONTH FROM scraped_date) AS extracted_month,
    EXTRACT(YEAR FROM scraped_date) AS extracted_year
   
    from {{ ref('fact_listings') }}
)

select
extracted_month as month,
extracted_year as year,
host_lga,
host_lga_code,
count(DISTINCT host_id) as number_distinct_host,
sum (price * (30 - availability_30) ) as estimated_revenue,
AVG (price * (30 - availability_30) ) as estimated_revenue_per_host

from source
group by host_lga,host_lga_code,extracted_month,extracted_year




