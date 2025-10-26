require('dotenv').config()
const Shopify = require('shopify-api-node')

const shopify = new Shopify({
    shopName: process.env.shopify_shop,
    accessToken: process.env.Shopify_access_token
});

const runquery = async () => {

    const query = `
        query suggestedRefund {
            order(id: "gid://shopify/Order/469306983") {
                suggestedRefund(refundDuties: [{dutyId: "gid://shopify/Duty/1064114503", refundType: FULL}]) {
                refundDuties {
                    amountSet {
                    shopMoney {
                        amount
                        currencyCode
                    }
                    }
                    originalDuty {
                    id
                    }
                }
                totalDutiesSet {
                    shopMoney {
                    amount
                    currencyCode
                    }
                }
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