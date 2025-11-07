require('dotenv').config()
const Shopify = require('shopify-api-node')

const shopify = new Shopify({
    shopName: process.env.shopify_shop,
    accessToken: process.env.Shopify_access_token
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
        console.log('Orders retrieved successfully:', responce, null, 2);
    } catch (error) {
        console.error('Error retrieving orders:', error);
    }


}

runquery()