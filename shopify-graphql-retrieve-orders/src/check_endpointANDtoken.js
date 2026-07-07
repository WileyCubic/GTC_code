// retrieve_order.js
require('../../load-root-env.cjs');
const Shopify = require('shopify-api-node');

// ---- Validate envs early
const SHOP = process.env.SHOPIFY_SHOP;                 // e.g., "gtcgreek.myshopify.com"
const TOKEN = process.env.SHOPIFY_ADMIN_ACCESS_TOKEN;   // must be the Admin API token (shpat_…)
const VERSION = process.env.SHOPIFY_API_VERSION || '2025-10';

function die(msg, example) {
  console.error('❌ ' + msg);
  if (example) console.error('   e.g. ' + example);
  process.exit(1);
}

// if (!SHOP)   die('Missing SHOPIFY_SHOP in .env', 'SHOPIFY_SHOP=yourstore.myshopify.com');
// if (!/\.myshopify\.com$/i.test(SHOP)) {
//   die('SHOPIFY_SHOP must be the full myshopify domain', 'SHOPIFY_SHOP=gtcgreek.myshopify.com');
// }
if (!TOKEN)  die('Missing SHOPIFY_ADMIN_ACCESS_TOKEN in .env', 'SHOPIFY_ADMIN_ACCESS_TOKEN=shpat_...');
if (!/^shpat_/i.test(TOKEN)) {
  console.warn('⚠️  SHOPIFY_ADMIN_ACCESS_TOKEN does not start with shpat_. Make sure this is the Admin API access token (Reveal token once), not API key/secret.');
}

// ---- Before using the library, hit the endpoint directly once (to catch 404s quickly)
const ENDPOINT = `https://${SHOP}/admin/api/${VERSION}/graphql.json`;
console.log('ℹ️  Testing endpoint:', ENDPOINT);

async function sanity() {
  // Node 22 has global fetch
  const res = await fetch(ENDPOINT, {
    method: 'POST',
    headers: {
      'Content-Type':'application/json',
      'X-Shopify-Access-Token': TOKEN
    },
    body: JSON.stringify({ query: 'query { shop { name myshopifyDomain } }' }),
  });

  if (res.status === 404) {
    const body = await res.text();
    throw new Error(`404 from ${ENDPOINT}. Check SHOPIFY_SHOP & VERSION.\nBody: ${body}`);
  }
  if (!res.ok) {
    const body = await res.text();
    throw new Error(`${res.status} ${res.statusText}\n${body}`);
  }
  const data = await res.json();
  console.log('✅ Sanity OK:', data.data);
}

async function main() {
  await sanity(); // prove endpoint + token are good

  const shopify = new Shopify({
    shopName: SHOP,          // full myshopify domain
    accessToken: TOKEN,      // Admin API token
    apiVersion: VERSION,     // pin version
  });

  const query = /* GraphQL */ `
    query OrdersPage($first: Int!) {
      orders(first: $first, sortKey: CREATED_AT, reverse: true) {
        nodes { id name createdAt displayFinancialStatus }
      }
    }
  `;

  try {
    const response = await shopify.graphql(query, { first: 5 });
    console.log('🧾 Orders:\n', JSON.stringify(response, null, 2));
  } catch (error) {
    // show as much detail as possible
    console.error('❌ Error retrieving orders:');
    if (error?.response?.body) {
      console.error(await streamToString(error.response.body).catch(() => String(error.response.body)));
    } else {
      console.error(error);
    }
  }
}

function streamToString(stream) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    stream.on('data', (c) => chunks.push(Buffer.from(c)));
    stream.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')));
    stream.on('error', reject);
  });
}

main().catch(e => {
  console.error('❌ Fatal:', e.message);
  process.exit(1);
});
