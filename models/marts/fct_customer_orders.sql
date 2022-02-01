with

customers as (

    select * from {{ ref('stg_jaffle_shop__customers') }}

),

orders as (

    select * from {{ ref('int_orders') }}

),

final as (

    select

        orders.*,
        customers.full_name,
        customers.surname,
        customers.givenname,

        -- Customer-level aggregations
        min(orders.order_date) over(
            partition by orders.customer_id
        ) as first_order_date,

        min(orders.valid_order_date) over(
            partition by orders.customer_id
        ) as first_non_returned_order_date,

        max(orders.valid_order_date) over(
            partition by orders.customer_id
        ) as most_recent_non_returned_order_date,

        count(*) over(
            partition by orders.customer_id
        ) as order_count,

        sum(case 
            when orders.valid_order_date is null then 0
            else 1 end) over(
            partition by orders.customer_id
        ) as non_returned_order_count,

        array_agg(orders.order_id) over(
            partition by orders.customer_id
        ) as order_ids,

        sum(case 
            when orders.valid_order_date is null then 0
            else orders.order_value_dollars end) over(
            partition by orders.customer_id
        ) as total_lifetime_value

    from orders
    inner join customers
        on orders.customer_id = customers.customer_id

)

select * from final order by 1