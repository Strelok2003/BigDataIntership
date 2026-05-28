with source as (

    select * from {{ source('pagila', 'payment') }}

),

renamed as (

    select
        amount,
        staff_id,
        rental_id,
        payment_id,
        customer_id,
        payment_date AS payment_datetime,
        payment_date::date as payment_date

    from source

)

select * from renamed
