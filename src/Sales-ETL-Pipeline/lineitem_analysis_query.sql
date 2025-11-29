select `Order Date` as OrderDate,
       `Item Quantity` as ItemQuantity,
       CONCAT_WS(' - ', `Item Name`, `Item Variation`) as ItemName,
       `Item Price` as ItemPrice,
       CONCAT_WS(', ', `Recipient Address`, `Recipient City`, `Recipient Region`, `Recipient Postal Code`, `Recipient Country`) as ShippingLocation,
       'Square' as Source
from square_raw

UNION ALL

select `Paid at` as OrderDate,
       `Lineitem quantity` as ItemQuantity,
       `Lineitem name` as ItemName,
       `Lineitem price` as ItemPrice,
       CONCAT_WS(', ', `Shipping Street`, `Shipping City`, `Shipping Province`, `Shipping Zip`, `Shipping Country`) as ShippingLocation,
       'Shopify' as Source
from shopify_raw

ORDER BY OrderDate ASC;
