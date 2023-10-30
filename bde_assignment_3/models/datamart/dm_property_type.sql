
{{
    config(
        unique_key=['property_type','accomodates','room_type','month','year']
    )
}}



with source as (
    select *,
    EXTRACT(MONTH FROM scraped_date) AS extracted_month,
    EXTRACT(YEAR FROM scraped_date) AS extracted_year
   
    from {{ ref('fact_listings') }}
),

active_listing as (

select 
property_type,
accomodates,
room_type,
extracted_year,
extracted_month,
COUNT (*) as active_listing,
MIN(price) AS min_price_active_listing,
MAX(price) AS max_price_active_listing,
PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price) AS median_price_active_listing,
AVG(price) AS avg_price_active_listing,
COALESCE(AVG(CASE WHEN review_scores_rating = 'NaN' THEN 0 ELSE review_scores_rating END), 0) AS avg_review_scores_rating,
AVG (price * (30 - availability_30) ) as average_estimated_revenue_active_listing

FROM source
where source.has_availability='t'
GROUP BY property_type, accomodates,room_type, extracted_year,extracted_month

),


inactive_listing as (

select 
property_type,
accomodates,
room_type,
extracted_month,
extracted_year,
COUNT (*) as inactive_listing

FROM source
where source.has_availability='f'
GROUP BY property_type, accomodates,room_type, extracted_year,extracted_month


),


all_listing as (

select 
property_type,
accomodates,
room_type,
extracted_month,
extracted_year,
COUNT (*) as total_listing,
count(DISTINCT host_id) as number_distinct_host,
SUM(CASE WHEN host_is_superhost = 't' THEN 1 ELSE 0 END) AS superhost_count,
SUM(30 - availability_30) AS total_number_of_stays


FROM source
GROUP BY property_type, accomodates,room_type, extracted_year,extracted_month

)
SELECT 

all_listing.property_type,
all_listing.accomodates,
all_listing.room_type,
all_listing.extracted_month as month,
all_listing.extracted_year as year,
active_listing.active_listing*100/all_listing.total_listing as active_listing_rate,
active_listing.min_price_active_listing,
active_listing.max_price_active_listing,
active_listing.median_price_active_listing,
active_listing.avg_price_active_listing,
all_listing.number_distinct_host,
all_listing.superhost_count*100/all_listing.number_distinct_host as superhost_rate,
active_listing.avg_review_scores_rating,
(active_listing.active_listing - LAG(active_listing.active_listing, 1) OVER (partition by all_listing.property_type,all_listing.accomodates, all_listing.room_type ORDER BY all_listing.extracted_year,all_listing.extracted_month))*100/LAG(active_listing.active_listing, 1) OVER (partition by all_listing.property_type,all_listing.accomodates, all_listing.room_type ORDER BY all_listing.extracted_year,all_listing.extracted_month) as percentage_change_active_listing, 
(inactive_listing.inactive_listing - LAG(inactive_listing.inactive_listing, 1) OVER (partition by all_listing.property_type,all_listing.accomodates, all_listing.room_type ORDER BY inactive_listing.extracted_year,all_listing.extracted_month))*100/LAG(inactive_listing.inactive_listing, 1) OVER (partition by all_listing.property_type,all_listing.accomodates, all_listing.room_type ORDER BY all_listing.extracted_year,all_listing.extracted_month) as percentage_change_inactive_listing,


all_listing.total_number_of_stays,
active_listing.average_estimated_revenue_active_listing




FROM
all_listing 
left join active_listing on all_listing.property_type  = active_listing.property_type
	AND all_listing.accomodates = active_listing.accomodates
	and all_listing.room_type = active_listing.room_type
    AND all_listing.extracted_month = active_listing.extracted_month
    AND all_listing.extracted_year = active_listing.extracted_year
left join inactive_listing on all_listing.property_type  = inactive_listing.property_type
	AND all_listing.accomodates = inactive_listing.accomodates
	and all_listing.room_type = inactive_listing.room_type
    AND all_listing.extracted_month = inactive_listing.extracted_month
    AND all_listing.extracted_year = inactive_listing.extracted_year
    


      

    
    
 