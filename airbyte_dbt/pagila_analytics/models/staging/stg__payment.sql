with source as (

    select * from {{ source('pagila', 'payment') }}

),

renamed as (

    select
        amount:: number(5,2) as amount,
        staff_id,
        rental_id,
        payment_id,
        customer_id,
        CONVERT_TIMEZONE('UTC', payment_date) AS payment_datetime,
        payment_date::date as payment_date

    from source

)

select * from renamed
