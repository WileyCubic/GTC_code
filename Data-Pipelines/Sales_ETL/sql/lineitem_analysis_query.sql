-- One row per line item from both stores, used by the line_item_analysis pipeline.
-- Shopify timestamps are stored in UTC; dates are taken in the store's local time.
select order_date::date                                       as order_date,
       item_quantity,
       concat_ws(' - ', item_name, item_variation)            as item_name,
       item_price,
       nullif(concat_ws(', ', recipient_address, recipient_city, recipient_region,
                        recipient_postal_code, recipient_country), '') as shipping_location,
       'Square'                                               as source
from square_raw

union all

select (coalesce(paid_at, created_at) at time zone 'America/Los_Angeles')::date as order_date,
       lineitem_quantity                                      as item_quantity,
       lineitem_name                                          as item_name,
       lineitem_price                                         as item_price,
       nullif(concat_ws(', ', shipping_street, shipping_city, shipping_province,
                        shipping_zip, shipping_country), '')  as shipping_location,
       'Shopify'                                              as source
from shopify_raw

order by order_date asc nulls last, source, item_name;
