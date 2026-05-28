with payment as (

    select * from {{ ref('stg__payment') }}

),

fact_revenue as (

    select
        amount
        ,staff_id
        ,rental_id
        ,payment_id
        ,customer_id
        ,payment_datetime
        ,payment_date
        ,to_char(payment_date, 'YYYYMMDD'):: int as payment_date_key
    from payment

)

select * from fact_revenue
