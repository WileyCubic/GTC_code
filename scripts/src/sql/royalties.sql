-- Royalty report rows for one quarter, from the Postgres raw order tables.
-- :start is the first day of the quarter, :end the first day of the next quarter.
-- Square rows are limited to NCL items (case-insensitive, as MySQL LIKE was);
-- Shopify rows are all included, matching the original MySQL query.
select order_date::date                                   as "Date",
       order_id                                           as "Invoice ID",
       item_name                                          as "Item Description",
       order_name                                         as "Customer",
       recipient_country                                  as "Shipping Country",
       recipient_postal_code                              as "Shipping Postal Code",
       item_quantity                                      as "Number of Units",
       item_price                                         as "Price Per Unit",
       'Square'                                           as "Source"
from square_raw
where order_date >= :start
  and order_date <  :end
  and item_name ilike '%ncl%'

union all

select (coalesce(paid_at, created_at) at time zone 'America/Los_Angeles')::date as "Date",
       name                                               as "Invoice ID",
       lineitem_name                                      as "Item Description",
       billing_name                                       as "Customer",
       shipping_country                                   as "Shipping Country",
       shipping_zip                                       as "Shipping Postal Code",
       lineitem_quantity                                  as "Number of Units",
       lineitem_price                                     as "Price Per Unit",
       'Shopify'                                          as "Source"
from shopify_raw
where (coalesce(paid_at, created_at) at time zone 'America/Los_Angeles') >= :start
  and (coalesce(paid_at, created_at) at time zone 'America/Los_Angeles') <  :end
-- Some paid orders have no paid_at (no payment method); their creation date is used.

order by "Date" asc;
