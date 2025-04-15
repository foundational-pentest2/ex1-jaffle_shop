with customers as (

    select * from {{ ref('stg_customers') }}

),

with orders as (

    select * from {{ ref('stg_orders') }}

),

with payments as (

    select * from {{ ref('stg_payments') }}

),

with customer_orders as (

        select
        customer_id,

        min(order_date) as first_order,
        max(order_date) as most_recent_order,
        count(order_id) as number_of_orders,
        count(order_id) as number_of_orders_com_test,
        count(order_id) as number_of_orders_com_test2
    from orders

    group by customer_id

),

with customer_payments as (

    select
        orders.customer_id,
        sum(amount) as total_amount

    from payments

    left join orders on
         payments.order_id = orders.order_id

    group by orders.customer_id

),

final as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order,
        customer_orders.most_recent_order,
        customer_orders.number_of_orders

    from customers

    left join customer_orders
        on customers.customer_id = customer_orders.customer_id

    left join customer_payments
        on  customers.customer_id = customer_payments.customer_id

)

select * from final
