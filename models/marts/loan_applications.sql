{{ config(materialized='table') }}

with accepted as (

    select
        loan_amount,
        loan_term,
        interest_rate,
        grade,
        sub_grade,
        employment_length_years,
        employment_length_bucket,
        annual_income,
        purpose,
        state,
        dti,
        issue_date as application_date,
        'accepted' as application_status
    from {{ ref('int_accepted_clean') }}

),

rejected as (

    select
        loan_amount,
        cast(null as int64) as loan_term,
        cast(null as float64) as interest_rate,
        cast(null as string) as grade,
        cast(null as string) as sub_grade,
        employment_length_years,
        employment_length_bucket,
        cast(null as float64) as annual_income,
        cast(null as string) as purpose,
        state,
        dti,
        application_date,
        'rejected' as application_status
    from {{ ref('int_rejected_clean') }}

)

select * from accepted
union all
select * from rejected