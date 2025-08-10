
-- Use the `ref` function to select from other models
{{
    config(
        materialized = 'incremental',
        unique_key = 'OrderId',
        alias = 'orders'
    )
}}
with incremental_data as (

    select
        *
    from 
        {{ ref('stg_orders') }}
    {% if is_incremental()%}
    where ChangeTime > (select coalesce(max(ChangeTime),'1900-01-01') from {{this}})
    {%endif%}
)
,final as (
    select distinct
        OrderId,
        CustomerId,
        OrderDate,
        OrderStatus,
        ProductId,
        QuantityOrdered,
        Price,
        TotalAmount,
        ChangeTime,
        '{{invocation_id}}' as invocation_id
    from incremental_data
)
select *
from 
final