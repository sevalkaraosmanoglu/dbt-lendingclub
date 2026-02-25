{{ config(materialized='view') }}

with source as (

    select *
    from {{ source('lending_club', 'accepted_raw') }}

),

renamed as (

    select
        loan_amnt                         as loan_amount,
        term                              as loan_term,
        int_rate                          as interest_rate,
        grade,
        sub_grade,
        emp_length                        as employment_length,
        home_ownership,
        annual_inc                        as annual_income,
        purpose,
        addr_state                        as state,
        dti,
        loan_status,
        issue_d                           as issue_date,
        earliest_cr_line                  as earliest_credit_line
    from source

)

select *
from renamed