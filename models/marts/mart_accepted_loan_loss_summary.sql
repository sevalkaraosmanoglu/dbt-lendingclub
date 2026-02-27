{{ config(materialized='table') }}

with base as (

    select *
    from {{ ref('int_accepted_loan_performance') }}

),

aggregated as (

    select
        -- segmentation dimensions
        grade,
        sub_grade,
        purpose,
        addr_state as state,
        credit_history_segment,
        credit_limit_segment,
        hardship_segment,

        -- volume
        count(*)                                   as loan_count,
        sum(loan_amnt)                             as total_loan_amount,

        -- loan status counts
        sum(is_default)                            as defaulted_loan_count,
        sum(is_fully_paid)                         as fully_paid_loan_count,
        sum(is_current)                            as current_loan_count,

        -- rates
        safe_divide(sum(is_default), count(*))     as default_rate,

        -- repayment & recovery
        sum(total_rec_prncp)                       as total_principal_repaid,
        sum(recoveries)                            as total_recoveries,

        -- loss
        sum(loss_amount)                           as total_loss_amount,
        safe_divide(
            sum(loss_amount),
            sum(loan_amnt)
        )                                          as loss_rate

    from base
    group by
        grade,
        sub_grade,
        purpose,
        state,
        credit_history_segment,
        credit_limit_segment,
        hardship_segment
)

select * from aggregated