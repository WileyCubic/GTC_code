require('../../load-root-env.cjs')
const Shopify = require('shopify-api-node')

const shopify = new Shopify({
    shopName: process.env.SHOPIFY_SHOP,
    accessToken: process.env.SHOPIFY_ADMIN_ACCESS_TOKEN,
    apiVersion: process.env.SHOPIFY_API_VERSION || '2025-10'
});

const getShopInfo = async () => {

    const query = `
        query shopInfo {
        shop {
            name
            url
            myshopifyDomain
            plan {
            displayName
            partnerDevelopment
            shopifyPlus
            }
        }
    }`;


    try {
        const responce = await shopify.graphql(query)
        console.log('Shop info:', JSON.stringify(responce, null, 2));
    } catch (error) {
        console.error('Error retrieving shop info:', error);
    }
}

getShopInfo()
