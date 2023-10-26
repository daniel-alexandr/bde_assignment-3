{{
    config(
        unique_key='suburb_name'
    )
}}


with

suburb_stg  as (

    select * from {{ ref('suburb_stg') }}

),


lga_stg as (

    select * from {{ ref('lga_stg') }}

)

select suburb.*,
lga_code.lga_code
from suburb_stg suburb left join lga_stg lga_code on UPPER(suburb.lga_name) = UPPER(lga_code.lga_name)
