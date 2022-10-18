
with 

source as (

    select * from {{ source('ecommerce', 'supplies') }}

),

renamed as (

    select

        ----------  ids
        {{ dbt_utils.surrogate_key(['id', 'sku']) }} as supply_uuid,
        id as supply_id,
        sku as product_id,

        ---------- properties
        name as supply_name,
        {{ cents_to_dollars('cost', 2)}} as supply_cost,
        perishable as is_perishable_supply

    from source

)

select * from renamed
