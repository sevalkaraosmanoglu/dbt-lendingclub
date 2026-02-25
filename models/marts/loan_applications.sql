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
        null as loan_term,
        null as interest_rate,
        null as grade,
        null as sub_grade,
        employment_length_years,
        employment_length_bucket,
        null as annual_income,
        null as purpose,
        state,
        dti,
        application_date,
        'rejected' as application_status
    from {{ ref('int_rejected_clean') }}

)

select * from accepted
union all
select * from rejected