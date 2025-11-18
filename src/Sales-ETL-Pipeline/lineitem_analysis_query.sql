--- Combined query to analyze sales line items ---
select `Order Date` as OrderDate,
       `Item Quantity` as ItemQuantity,
       `Item Name` as ItemName,
       `Item Variation` as ItemVariation,
       `Item Price` as ItemPrice,
       'Square' as Source
from square_raw

UNION ALL

select `Paid at` as OrderDate,
       `Lineitem quantity` as ItemQuantity,
       `Lineitem name` as ItemName,
       NULL as ItemVariation,  -- Will be parsed out using python
       `Lineitem price` as ItemPrice,
       'Shopify' as Source
from shopify_raw

ORDER BY OrderDate DESC;




