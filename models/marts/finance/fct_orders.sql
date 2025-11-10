with payments as(

select * from {{ ref('stg_stripe__payments') }}

),

orders as(

    select * from {{ ref('stg_jaffle_shop__orders') }}


),

final as (

    select
        orders.customer_id,
        orders.order_id,
        payments.amount
       

    from orders

    left join payments using (order_id)

    WHERE payments.status = 'success'
    AND amount <> 0
)

select * from final
