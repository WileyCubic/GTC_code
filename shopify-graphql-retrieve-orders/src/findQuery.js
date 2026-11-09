require('../../load-root-env.cjs')
const Shopify = require('shopify-api-node')

const shopify = new Shopify({
    shopName: process.env.SHOPIFY_SHOP,
    accessToken: process.env.SHOPIFY_ADMIN_ACCESS_TOKEN,
    apiVersion: process.env.SHOPIFY_API_VERSION || '2025-10'
});

const runquery = async () => {

    const query = `
        query {
            orders(first: 10) {
                edges {
                    cursor
                    node {
                        id
                    }
              }
                pageInfo {
                    hasNextPage
                    hasPreviousPage
                    startCursor
                    endCursor
                }
            }
      }`;


    try {
        const responce = await shopify.graphql(query)
        console.log('Orders retrieved successfully:', JSON.stringify(responce, null, 2));
    } catch (error) {
        console.error('Error retrieving orders:', error);
    }


}

runquery()
