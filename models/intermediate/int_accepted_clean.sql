{{ config(materialized='table') }}

with base as (

    select
        loan_amount,

        -- loan_term: "36 months" → 36
        cast(regexp_extract(loan_term, r'\d+') as int64) as loan_term,

        interest_rate,
        grade,
        sub_grade,

        -- employment_length_years (string → int)
        case
            when employment_length like '%+%' then 10
            when employment_length like '%<%' then 0
            when employment_length is null then null
            else cast(regexp_extract(employment_length, r'\d+') as int64)
        end as employment_length_years,

        annual_income,
        purpose,
        state,
        dti,

        parse_date('%b-%Y', issue_date) as issue_date,
        parse_date('%b-%Y', earliest_credit_line) as earliest_credit_line

    from {{ ref('stg_accepted_raw') }}
    where interest_rate is not null
)

select
    *,

    -- employment_length_bucket
    case
        when employment_length_years is null then 'unknown'
        when employment_length_years < 1 then '<1'
        when employment_length_years between 1 and 3 then '1-3'
        when employment_length_years between 4 and 6 then '4-6'
        when employment_length_years between 7 and 9 then '7-9'
        else '10+'
    end as employment_length_bucket

from base