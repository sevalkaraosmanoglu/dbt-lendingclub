{{ config(materialized='table') }}

with base as (
    select *,
        PARSE_DATE('%b-%Y', issue_d) as issue_date
    from {{ ref('int_accepted_loan_performance') }}
),

aggregated as (
    select
        issue_date,
        grade,
        sub_grade,
        purpose,
        addr_state as state,
        credit_history_segment,
        credit_limit_segment,
        hardship_segment,

        count(*) as loan_count,
        sum(loan_amnt) as total_loan_amount,

        sum(is_default) as defaulted_loan_count,
        sum(is_fully_paid) as fully_paid_loan_count,
        sum(is_current) as current_loan_count,

        safe_divide(sum(is_default), count(*)) as default_rate,
        sum(total_rec_prncp) as total_principal_repaid,
        sum(recoveries) as total_recoveries,
        sum(loss_amount) as total_loss_amount,
        safe_divide(sum(loss_amount), sum(loan_amnt)) as loss_rate

    from base
    group by
        issue_date,
        grade,
        sub_grade,
        purpose,
        state,
        credit_history_segment,
        credit_limit_segment,
        hardship_segment
)

select * from aggregated