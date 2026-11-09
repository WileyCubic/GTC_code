# gtc-graphql-api

The stock **Shopify embedded-app template** (React Router + Prisma session
storage), created with the Shopify CLI and registered as "GTC graphql api"
(`src/shopify.app.toml`, Admin API `2025-10`). It has only the template's
pages (`/app`, `/app/additional`), OAuth login and the uninstall /
scope-update webhooks. **Nothing else in this repo uses it.**

Run (needs the Shopify CLI and a Partner account):

```bash
cd gtc-graphql-api/src
npm install
npm run dev        # shopify app dev
```

It reads `SHOPIFY_API_KEY`, `SHOPIFY_API_SECRET`, `SCOPES`, `SHOPIFY_APP_URL`,
`SHOP_CUSTOM_DOMAIN`, `PORT`, `FRONTEND_PORT`, `HOST` and `NODE_ENV` from the
root `.env`.

⚠ `shopify.app.toml` requests almost every Shopify scope (read/write orders,
customers, payments, themes…). Trim it to what is actually needed, or remove
the app. See [docs/AUDIT.md](../docs/AUDIT.md#open-questions).
