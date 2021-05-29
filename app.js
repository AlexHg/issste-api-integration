
/**
 * Responds to any HTTP request.
 *
 * @param {!express:Request} req HTTP request context.
 * @param {!express:Response} res HTTP response context.
 * ORDERS
 * IDCODE:STRING,date:TIMESTAMP,update:TIMESTAMP,version:INTEGER,action:STRING,id:STRING,idSapOrder:STRING,createdAt:DATETIME,approvedAt:DATETIME,authorizedAt:DATETIME,createdByUserName:STRING,createdByUserEmail:STRING,approvedByUserEmail:STRING,approvedByUserName:STRING,umuCode:STRING,orderType:STRING,maxDeliveryDate:DATETIME,planDateTime:DATETIME,creatorComments:STRING
 * 
 * PRODUCTS
 * IDCODE:STRING,date:TIMESTAMP,update:TIMESTAMP,version:INTEGER,action:STRING,productCode:STRING,orderId:STRING,position:STRING,dpnPieces:NUMERIC,validatedPieces:NUMERIC,approvedPieces:NUMERIC,confirmedPieces:NUMERIC
 * 
 * DELIVERIES
 * IDCODE:STRING,date:TIMESTAMP,update:TIMESTAMP,version:INTEGER,action:STRING,orderId:STRING,certificationDateTime:DATETIME,creationDateTime:DATETIME,idRemission:STRING,transportArrivalDateTime:DATETIME,deliveredQuantity:NUMERIC
 * 
 * STOCKS
 * IDCODE:STRING,date:TIMESTAMP,update:TIMESTAMP,version:INTEGER,action:STRING,idMaterial:STRING,idUmu:STRING,stockDateTime:DATETIME,vendorBatch:STRING,totalQuantity:NUMERIC,averageCost:NUMERIC,totalAmount:NUMERIC,position:STRING
 */
 require('dotenv').config();
 const {PubSub} = require(`@google-cloud/pubsub`);
 const axios = require('axios')
 
 const pubSubClient = new PubSub();
 const apiKey = process.env.API_KEY;

 const topic = process.env.TOPIC //"issste"; 
 const dataset = process.env.DATASET //"BiIssste"
 
 function addDays(originalDate, days){
    cloneDate = new Date(originalDate.valueOf());
    cloneDate.setDate(cloneDate.getDate() + days);
    return cloneDate;
  }
  
   const insertFrom = addDays(new Date(),-1).toISOString().slice(0,10);
   const insertTo = new Date().toISOString().slice(0,10);
  

 /*
{
    "transactions": {
        "MCHB": [
            {
            "IDCODE": "M5004830001FM15PT01NECH020321",
            "MATNR": "M5004830001",
            "WERKS": "FM15",
            "LGORT": "PT01",
            "CHARG": "NECH020321",
            "EXPIRATION_DATE": "2026-03-02",
            "CLABS": "79040.000",
            "CINSM": "0.000",
            "CSPEM": "0.000",
            "STOCK_DATE": "2021-03-30",
            "STOCK_HOUR": "22:32:02",
            "ACTION": "UPSERT"
            }
        ]
    }
}
 */

    async function publisherT(messageArray){
        try{
            console.log(messageArray);
            const dataBuffer = Buffer.from(JSON.stringify({transactions: messageArray}));
            const messageId = await pubSubClient.topic(topic).publish(dataBuffer);

            console.log(`Message ${messageId} published.`);
            return messageId;
        }catch(e){
            console.log("ERROR",e)
        }
        return false;
    }

 let BiService = {

     token: null,
     refreshToken: null,
     user: null,
     clientId: null,

     authUrl: process.env.BI_AUTH_URL,
     baseUrl: process.env.BI_BASE_URL,

     credentials: {
        clientId: process.env.BI_CLIENT_ID,
        email: process.env.BI_EMAIL,
        password: process.env.BI_PASSWORD,
        user: process.env.BI_USER
     },

     authTry: 0,

     serialize: function(obj) {
        var str = [];
        for (var p in obj)
          if (obj.hasOwnProperty(p)) {
            str.push(encodeURIComponent(p) + "=" + encodeURIComponent(obj[p]));
          }
        return str.join("&");
     },

     auth: async () => {
         console.log("mytoken",!BiService.token)
        if(BiService.authTry >= 10) return false;
        if(!BiService.token){

            //console.log(BiService.authUrl, BiService.credentials)
            let {data} = await axios.post(BiService.authUrl, BiService.credentials);

            BiService.token = data.token,
            BiService.refreshToken = data.refreshToken,
            BiService.user = data.user,
            BiService.clientId = data.clientId
            BiService.authTry++;
            return data.token;
        }
        return BiService.token
     },

     // Cada 3 horas ir por dia
     // carga inicial debe ser por dia
     // Secuencial x dia 
     transferOrders: async () => {
         let countOr = 0;
         let countPr = 0;
        let arrayMessage = {
            ORDERS: [],
            PRODUCTS: [],
        };

        if(await BiService.auth() === false){  console.log("Autherror"); return; }
        
        let integrationData = []
        let todayFilter = BiService.serialize({
            "start-creation-date": insertFrom,
            "end-creation-date": insertTo
         });

        try{
            console.log(`${BiService.baseUrl}/data/api/supplies?${todayFilter}`);
            integrationData = await axios.get(`${BiService.baseUrl}/data/api/supplies?${todayFilter}`, {
                headers: { 'Authorization': 'Bearer ' + BiService.token }
            });
        }catch(e){
            console.log("ERROR:", e )
            BiService.token = null;
            return await BiService.transferOrders();
        }
        console.log(integrationData.data)
        integrationData.data.map( ({products, ...order}) => { 

            //console.log("Order", order)
            arrayMessage.ORDERS.push({
                ...order,
                IDCODE: `${order.id}`,
                ACTION: "UPSERT",
            });

            countOr++;
            
            products.map( (product, position) => {

                //console.log("Product", product)
                arrayMessage.PRODUCTS.push({
                    ...product,
                    IDCODE: `${order.id}-${product.productCode}-${position.toString().padStart(6,"0")}`,
                    orderId: order.id,
                    position: position.toString().padStart(6,"0"),
                    ACTION: "UPSERT",
                });

                countPr++;
            });
            if(arrayMessage.ORDERS.length + arrayMessage.PRODUCTS.length >= 20000){
                console.log("20000 limit exceeded, sending to pubsub....")
                console.log( publisherT( arrayMessage ) );
                arrayMessage = {
                    ORDERS: [],
                    PRODUCTS: [],
                };
            }
            console.log("CountOr", countOr, "CountPr", countPr);
        });

        //console.log(arrayMessage);
        return await publisherT(arrayMessage);

     },

     // Cada 3 horas
     // Corriendo dia anterior y actual
     // Correr carga inicial en 3 o 4 bloques de fechas
     transferDeliveries: async () => {
         let countDe = 0;
         let countPa = 0;
        let arrayMessage = {
            DELIVERIES: [],
            PACKAGES: [],
        };
        if(await BiService.auth() === false){  console.log("Autherror"); return; }
        let deliveriesData = []
        let todayFilter = BiService.serialize({
            "start-certification-date": insertFrom,
            "end-certification-date": insertTo
         });
        try{
            console.log(`${BiService.baseUrl}/data/api/deliveries-certified?${todayFilter}`);
            deliveriesData = await axios.get(`${BiService.baseUrl}/data/api/deliveries-certified?${todayFilter}`, {
                headers: { 'Authorization': 'Bearer ' + BiService.token, 'Accept': '*/*' }
            });
        }catch(e){
            console.log("ERROR:", e )
            BiService.token = null;
            return await BiService.transferDeliveries();
        }

        deliveriesData.data.map(({products, ...delivery}) => { 
            //console.log("Delivery", delivery)
            arrayMessage.DELIVERIES.push({
                ...delivery,
                IDCODE: `${delivery.idRemission}-${delivery.sapDelivery}`,

                ACTION: "UPSERT",
            })
            countDe++;
            
            products.map((product, position) => {
                //console.log("Package", product)
                arrayMessage.PACKAGES.push({
                    ...product,
                    IDCODE: `${delivery.idRemission}-${delivery.sapDelivery}-${product.materialId}-${product.vendorBatch}`,
                    deliveryId: delivery.sapDelivery,
                    idRemission: delivery.idRemission,
                    //position: position.toString().padStart(6,"0"),
                    ACTION: "UPSERT"
                })
                countPa++;
            });

            if(arrayMessage.DELIVERIES.length + arrayMessage.PACKAGES.length >= 20000){
                console.log("20000 limit exceeded, sending to pubsub....")
                publisherT( arrayMessage );
                arrayMessage = {
                    DELIVERIES: [],
                    PACKAGES: [],
                };
            }

            console.log("CountDe", countDe, "CountPa", countPa);

        });
        //console.log(arrayMessage);

        return await publisherT( arrayMessage );
     },

     transferStocks: async () => {
        let count = 0;
        
        // console.log(await BiService.auth());
        // return;
        if(await BiService.auth() === false){  console.log("Autherror"); return; }

        //let umuIdArray = Array.from(Array(10).keys());
        for(let i = 0; i < 100000; i++){
            let arrayMessage = {
                STOCKS: [],
            };
            let integrationData = [];
            let paginationFilter = BiService.serialize({
                "page": i*8000,
                "limit": 8000,
                //"umu-id":i.toString().padStart(3,"0")
            });
            try{
                console.log(`${BiService.baseUrl}/data/api/inventories?${paginationFilter}`);
                integrationData = await axios.get(`${BiService.baseUrl}/data/api/inventories?${paginationFilter}`, {
                    headers: { 'Authorization': 'Bearer ' + BiService.token }
                });
            }catch(e){
                console.log("ERROR:", e )
                BiService.token = null;
                return null;
            }
            console.log(integrationData.data)
            if(integrationData.data.length == 0) break;

            integrationData.data.map( (stock, position) => { 
                //console.log("Stock", stock)
                count++;
                arrayMessage.STOCKS.push({
                    ...stock,
                    IDCODE: `${stock.idMaterial}-${stock.idUmu}-${position.toString().padStart(6,"0")}`,
                    position: position.toString().padStart(6,"0"),
                    ACTION: "UPSERT",
                })
            });
            console.log(arrayMessage);
            await publisherT(arrayMessage);
            console.log(count);
        }
        
    }
}



// BiService.transferOrders()
// BiService.transferDeliveries()
BiService.transferStocks()