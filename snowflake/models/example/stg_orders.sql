
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/


{{ config(materialized='ephemeral') }}

with source_data as (

    select
        to_varchar(order_id) as OrderId,
        to_varchar(customer_id) as CustomerId,
        to_varchar(order_date) as OrderDate,
        to_varchar(status) as OrderStatus,
        to_varchar(product_id) as ProductId,
        to_varchar(quantity) as QuantityOrdered,
        to_numeric(price) as Price,
        to_varchar(total_amount) as TotalAmount,
        to_timestamp(substr(cdc_timestamp, 1, 19),'YYYY-MM-DD HH24:MI:SS') as ChangeTime

    from {{source('snowflake_source','orders')}}
)

select *
from source_data

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
