require('dotenv').config()
const Shopify = require('shopify-api-node')

const shopify = new Shopify({
    shopName: process.env.shopify_shop,
    accessToken: process.env.Shopify_access_token
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