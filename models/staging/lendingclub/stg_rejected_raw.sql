{{ config(materialized='view') }}

with source as (

    select *
    from {{ source('lending_club', 'rejected_raw') }}

),

renamed as (

    select
        `Amount Requested`          as loan_amount,
        `Application Date`          as application_date,
        `Loan Title`                as loan_title,
        `Risk_Score`                as risk_score,
        `Debt-To-Income Ratio`      as dti,
        `Zip Code`                  as zip_code,
        State,
        `Employment Length`         as employment_length,
        `Policy Code`               as policy_code
    from source

)

select *
from renamed