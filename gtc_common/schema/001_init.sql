-- GTC Postgres schema, version 1.
-- Applied by `gtc-db-migrate` (gtc_common/migrate.py). Safe to re-run.
--
-- Column names are the CSV export headers converted to snake_case
-- (gtc_common.db.to_snake), e.g. "Lineitem compare at price" -> lineitem_compare_at_price.
-- The only renamed column is Square "Order" -> order_id ("order" is an SQL keyword).

-- ---------------------------------------------------------------------------
-- Square online-store order export, one row per line item.
-- Written by the Sales ETL raw_orders pipeline (upsert by order_id).
-- ---------------------------------------------------------------------------
create table if not exists square_raw (
    row_id                    bigserial primary key,
    order_id                  text not null,      -- CSV "Order", e.g. "Square Online 1196255745"
    order_name                text,               -- customer name on the order
    order_date                timestamp,
    currency                  text,
    order_subtotal            numeric(12,2),
    order_shipping_price      numeric(12,2),
    order_tax_total           numeric(12,2),
    order_total               numeric(12,2),
    order_refunded_amount     numeric(12,2),
    fulfillment_date          timestamp,
    fulfillment_type          text,
    fulfillment_status        text,
    channels                  text,
    fulfillment_location      text,
    fulfillment_notes         text,
    recipient_name            text,
    recipient_email           text,
    recipient_phone           text,
    recipient_address         text,
    recipient_address_2       text,
    recipient_postal_code     text,
    recipient_city            text,
    recipient_region          text,
    recipient_country         text,
    item_quantity             integer,
    item_name                 text,
    item_sku                  text,
    item_variation            text,
    item_modifiers            text,
    item_price                numeric(12,2),
    item_options_total_price  numeric(12,2),
    item_total_price          numeric(12,2),
    loaded_at                 timestamptz not null default now()
);
create index if not exists square_raw_order_id_idx on square_raw (order_id);
create index if not exists square_raw_order_date_idx on square_raw (order_date);

-- ---------------------------------------------------------------------------
-- Shopify order export, one row per line item (order-level fields filled on
-- every row). Cancelled orders are removed. Upsert by name (e.g. "#1621").
-- ---------------------------------------------------------------------------
create table if not exists shopify_raw (
    row_id                       bigserial primary key,
    name                         text not null,   -- Shopify order number, e.g. "#1621"
    email                        text,
    financial_status             text,
    paid_at                      timestamptz,
    fulfillment_status           text,
    fulfilled_at                 timestamptz,
    accepts_marketing            text,
    currency                     text,
    subtotal                     numeric(12,2),
    shipping                     numeric(12,2),
    taxes                        numeric(12,2),
    total                        numeric(12,2),
    discount_code                text,
    discount_amount              numeric(12,2),
    shipping_method              text,
    created_at                   timestamptz,
    lineitem_quantity            integer,
    lineitem_name                text,
    lineitem_price               numeric(12,2),
    lineitem_compare_at_price    numeric(12,2),
    lineitem_sku                 text,
    lineitem_requires_shipping   text,
    lineitem_taxable             text,
    lineitem_fulfillment_status  text,
    billing_name                 text,
    billing_street               text,
    billing_address1             text,
    billing_address2             text,
    billing_company              text,
    billing_city                 text,
    billing_zip                  text,
    billing_province             text,
    billing_country              text,
    billing_phone                text,
    shipping_name                text,
    shipping_street              text,
    shipping_address1            text,
    shipping_address2            text,
    shipping_company             text,
    shipping_city                text,
    shipping_zip                 text,
    shipping_province            text,
    shipping_country             text,
    shipping_phone               text,
    notes                        text,
    note_attributes              text,
    cancelled_at                 timestamptz,
    payment_method               text,
    payment_reference            text,
    refunded_amount              numeric(12,2),
    vendor                       text,
    outstanding_balance          numeric(12,2),
    employee                     text,
    location                     text,
    device_id                    text,
    id                           text,            -- Shopify internal order id
    tags                         text,
    risk_level                   text,
    source                       text,
    lineitem_discount            numeric(12,2),
    tax_1_name                   text,
    tax_1_value                  numeric(12,2),
    tax_2_name                   text,
    tax_2_value                  numeric(12,2),
    tax_3_name                   text,
    tax_3_value                  numeric(12,2),
    tax_4_name                   text,
    tax_4_value                  numeric(12,2),
    tax_5_name                   text,
    tax_5_value                  numeric(12,2),
    phone                        text,
    receipt_number               text,
    duties                       text,
    billing_province_name        text,
    shipping_province_name       text,
    payment_id                   text,
    payment_terms_name           text,
    next_payment_due_at          text,
    payment_references           text,
    loaded_at                    timestamptz not null default now()
);
create index if not exists shopify_raw_name_idx on shopify_raw (name);
create index if not exists shopify_raw_paid_at_idx on shopify_raw (paid_at);

-- ---------------------------------------------------------------------------
-- Line-item analysis: Square + Shopify line items with size / color /
-- organization / garment extracted from the item name. Rebuilt in full by the
-- Sales ETL line_item_analysis pipeline after every raw_orders run.
-- ---------------------------------------------------------------------------
create table if not exists lineitem_analysis (
    row_id             bigserial primary key,
    order_date         date,
    item_quantity      integer,
    item_name          text,
    item_price         numeric(12,2),
    shipping_location  text,
    source             text,     -- 'Square' or 'Shopify'
    size               text,
    color              text,
    organization_name  text,
    garment_type       text,
    money_value        numeric(12,2),   -- item_quantity * item_price
    rolling_total      numeric(14,2)    -- running sum of money_value by order_date
);

-- ---------------------------------------------------------------------------
-- AmEx card statements. Rebuilt in full by the Expenses ETL.
-- ---------------------------------------------------------------------------
create table if not exists bp_expences (     -- Business Platinum card
    row_id                        bigserial primary key,
    date                          date,
    receipt                       text,
    description                   text,
    card_member                   text,
    account                       text,
    amount                        numeric(12,2),
    extended_details              text,
    appears_on_your_statement_as  text,
    address                       text,
    city_state                    text,
    zip_code                      text,
    country                       text,
    reference                     text,
    category                      text
);

create table if not exists ab_expences (     -- Amazon Business Prime card
    row_id                        bigserial primary key,
    date                          date,
    receipt                       text,
    description                   text,
    amount                        numeric(12,2),
    extended_details              text,
    appears_on_your_statement_as  text,
    address                       text,
    city_state                    text,
    zip_code                      text,
    country                       text,
    reference                     text,
    category                      text
);
