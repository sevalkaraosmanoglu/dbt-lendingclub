{{ config(materialized='table') }}

select
    loan_amount,

    -- loan_term: "36 months" → 36
    cast(regexp_extract(loan_term, r'\d+') as int64) as loan_term,

    interest_rate,
    grade,
    sub_grade,

    employment_length,

    -- employment_length_years
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