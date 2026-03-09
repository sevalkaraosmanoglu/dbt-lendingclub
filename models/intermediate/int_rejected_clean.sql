{{ config(materialized='table') }}

with source as (
    select *
    from {{ ref('stg_rejected_raw') }}
),

cleaned as (
    select
        loan_amount,
        application_date,
        nullif(trim(loan_title), '') as loan_title,
        risk_score,
        case when dti < 0 then null else dti end as dti,
        zip_code,
        state,

        -- 🔹 employment_length_years integer
        case
            when employment_length is null then null
            when employment_length like '%10+%' or employment_length like '%+%' then 10
            when employment_length like '%< 1%' or employment_length like '%<%' then 0
            else cast(regexp_extract(employment_length, r'\d+') as int64)
        end as employment_length_years,

        -- 🔹 employment_length_bucket standard (Accepted ile birebir aynı)
        case
            when employment_length is null then 'unknown'
            when employment_length like '%10+%' or employment_length like '%+%' then '10+'
            when employment_length like '%< 1%' or employment_length like '%<%' then '<1'
            when cast(regexp_extract(employment_length, r'\d+') as int64) between 1 and 3 then '1-3'
            when cast(regexp_extract(employment_length, r'\d+') as int64) between 4 and 6 then '4-6'
            when cast(regexp_extract(employment_length, r'\d+') as int64) between 7 and 9 then '7-9'
        end as employment_length_bucket,

        policy_code

    from source
    where risk_score is not null
)

select * from cleaned