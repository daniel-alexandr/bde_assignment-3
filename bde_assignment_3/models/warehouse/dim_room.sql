{{
    config(
        unique_key=['listing_id','scraped_date']
    )
}}

select * from {{ ref('room_snapshot') }}