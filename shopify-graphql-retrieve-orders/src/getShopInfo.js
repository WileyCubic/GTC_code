require('../../load-root-env.cjs')
const Shopify = require('shopify-api-node')

const shopify = new Shopify({
    shopName: process.env.SHOPIFY_SHOP,
    accessToken: process.env.SHOPIFY_ADMIN_ACCESS_TOKEN
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
        console.log('Orders retrieved successfully:', responce);
    } catch (error) {
        console.error('Error retrieving orders:', error);
    }
}

getShopInfo()