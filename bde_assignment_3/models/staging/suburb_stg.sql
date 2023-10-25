{{
    config(
        unique_key='suburb_name'
    )
}}

with

source  as (

    select * from {{ source('raw','nsw_lga_suburb') }}

),

unknown as (
    select
        'unknown' as suburb_name,
        'unknown' as lga_name
)
select * from unknown
union all
select * from source

