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
        'unknown' as lga_name,
        'unknown' as suburb_name
),
extra_cleaning as (
    SELECT 'BAYSIDE' AS lga_name, 'BAYSIDE' AS suburb_name
    UNION ALL
    SELECT 'CANTERBURY-BANKSTOWN' AS lga_name, 'CANTERBURY-BANKSTOWN' AS suburb_name
    UNION ALL
    SELECT 'CUMBERLAND' AS lga_name, 'CUMBERLAND' AS suburb_name
    UNION ALL
    SELECT 'GEORGES RIVER' AS lga_name, 'GEORGES RIVER' AS suburb_name
    UNION ALL
    SELECT 'INNER WEST' AS lga_name, 'INNER WEST' AS suburb_name
    UNION ALL
    SELECT 'NORTHERN BEACHES' AS lga_name, 'NORTHERN BEACHES' AS suburb_name
    UNION ALL
    SELECT 'SUTHERLAND SHIRE' AS lga_name, 'SUTHERLAND SHIRE' AS suburb_name
    UNION ALL
    SELECT 'THE HILLS SHIRE' AS lga_name, 'THE HILLS SHIRE' AS suburb_name
)

select * from source
union all
select * from unknown
union all
select * from extra_cleaning

