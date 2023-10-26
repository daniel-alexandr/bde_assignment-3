{{
    config(
        unique_key='lga_code'
    )
}}

with host as
(
    select * from {{ ref('host_stg') }}
),
neighbourhood as 
(
    select * from {{ ref('neighbourhood_stg') }}
),
property as 
(
    select * from {{ ref('property_stg') }}
),
room as 
(
    select * from {{ ref('room_stg') }}
),
suburb_lga as 
(
    select * from {{ ref('dim_lga_suburb') }}
)

select
*
from suburb_lga 