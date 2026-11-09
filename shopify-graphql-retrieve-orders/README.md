# shopify-graphql-retrieve-orders

Small Node scripts that check the Shopify **Admin GraphQL API** works with a
static admin token. They print to the console only. The Python pipelines still
use manual CSV exports; nothing here feeds them yet.

Settings come from the repo-root `.env` (via `../../load-root-env.cjs`):
`SHOPIFY_SHOP`, `SHOPIFY_ADMIN_ACCESS_TOKEN`, `SHOPIFY_API_VERSION`.

```bash
cd shopify-graphql-retrieve-orders/src
npm install
npm run check    # check_endpointANDtoken.js: validates shop domain + token, calls the API directly
npm run shop     # getShopInfo.js: shop name, domain, plan
npm run orders   # findQuery.js: first 10 order ids
```

`orders_query.gql` holds the same orders query as `findQuery.js` (not loaded by any script).
