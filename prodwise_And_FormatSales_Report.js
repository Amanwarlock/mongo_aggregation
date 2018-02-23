db.omsmasters.aggregate({'$match' : {status : {'$nin':['Created','Cancelled']}}},
{'$unwind' : '$subOrders'},
{'$unwind' : '$subOrders.products'},
{'$lookup':{
    from : 'omsinvoices',
    localField : 'subOrders.invoiceNo',
    foreignField : '_id',
    as : 'invoiceData'
}},
{'$unwind' : '$invoiceData'},
{'$lookup' : {
    from : 'purchaseorders',
    localField : 'productData._id',
    foreignField : 'products.productId',
    as: 'POs'
}},
{
    '$lookup' : {
        from : 'franchises',
        localField : 'franchise.id',
        foreignField : '_id',
        as: 'franchiseData'
    }
},
{'$unwind' : '$franchiseData'},
{'$lookup' :{
    from : 'products',
    localField : 'subOrders.products.id',
    foreignField : '_id',
    as : 'productData'
}},
{'$unwind' : '$productData'},
{'$lookup' : {
    from : 'purchaseorders',
    localField : 'productData._id',
    foreignField : 'products.productId',
    as: 'POs'
}},
{'$project' : {
    data : '$$ROOT',
    'POs' : {'$arrayElemAt' : ['$POs' , 0]},
    'invDeal' : {
        '$filter' : {
            input : '$invoiceData.deals',
            cond : {'$eq' : ['$$this.id' , '$subOrders.id']}
        }
    }
}},
{'$project' : {
   'data' : 1,
   'invDeal' : {'$arrayElemAt' : ['$invDeal' , 0]},
   'poProd' : '$POs.products',
}},
{'$project' :{
    data : 1,
    'invProd': {
       '$filter' : {
           input: '$invDeal.products',
           cond:{'$eq' : ['$$this.id' , '$data.subOrders.products.id']}
       }
   },
    'COGS' : {
        '$reduce' : {
            input : '$poProd',
            initialValue : 0,
            in: {
                '$cond'  :{
                    if : {'$eq': ['$$this.productId' , '$data.subOrders.products.id' ]},
                    then : {'$add' : ['$$this.unitPrice' , '$$value']},
                    else : {'$add' : ['$$value' , 0]}
                }
            }
        }
    },
}},
{'$unwind' : '$invProd'},
{'$project' : {
    '_id' : 0,
    '_date' : {$dateToString:{format:'%Y-%m-%d',date:'$data.createdAt'}},
    '_Product Id' : '$data.subOrders.products.id',
    '_Product Name' : '$data.productData.name',
    '_Order ID' : '$data._id',
    '_Order Status' : '$data.status',
    '_Deal Id' : '$data.subOrders.id',
    '_Deal Name' : '$data.subOrders.name',
    '_Brand Id' : '$data.subOrders.brand.id',
    '_Brand' : '$data.subOrders.brand.name',
    '_Category'  : {
        '$reduce' : {
            input : '$data.productData.category',
            initialValue : '',
            in: {'$concat' : ['$$value' , {
                '$cond' : {
                    if : {'$ne' : [{ '$indexOfArray': [ '$data.productData.category', '$$this' ] } , {'$subtract' : [{ '$size': '$data.productData.category'} , 1]}] , '$ne' : ['$$value' , '']},
                    then : ', ',
                    else: ''
                    
                }
            } , '$$this'] }
        }
    },
    '_Qty Sold' : '$data.subOrders.products.quantity',
    '_MRP' : '$data.subOrders.products.mrp',
    '_Deal Price' : '$data.subOrders.b2bPrice',
    '_COGS' : "$COGS",
    '_Order Value' : {'$multiply' : ['$data.subOrders.b2bPrice'  , '$data.subOrders.quantity']},
    '_SKU' : '$data.productData.skuCode',
    '_Barcode' : '$data.productData.barcode',
    '_Imei Nos' : {
        '$reduce' : {
            input : '$invProd.serialNo',
            initialValue : '',
            in: {'$concat' : ['$$value',
                {
                    '$cond' : {
                        if : {'$ne' : [{ '$indexOfArray': [ '$invProd.serialNo', '$$this' ] } , {'$subtract' : [{ '$size': '$invProd.serialNo'} , 1]}] , '$ne' : ['$$value' , '']},
                        then : ', ',
                        else: ''
                    }
                },
                '$$this']}
        }
    },
    '_Purcharse Orders' : {
        '$reduce' : {
            input : '$data.POs',
            initialValue : '',
            in: {'$concat' : ['$$value',
                {
                    '$cond' : {
                        if : {'$ne' : [{ '$indexOfArray': [ '$data.POs', '$$this' ] } , {'$subtract' : [{ '$size': '$data.POs'} , 1]}] , '$ne' : ['$$value' , '']},
                        then : ', ',
                        else: ''
                    }
                },
                '$$this._id']}
        }
    },
    '_Franchsie Id' : '$data.franchiseData._id',
    '_Franchise Name' : '$data.franchiseData.name',
    '_State' : '$data.franchiseData.state',
    '_City' : '$data.franchiseData.city',
    '_District' : '$data.franchiseData.district',
    '_Gross Profit' : {'$subtract' : ['$data.subOrders.b2bPrice' , '$COGS']},
    '_Gross Profit %' : {'$multiply' : [{'$divide' : [{'$subtract' : ['$data.subOrders.b2bPrice' , '$COGS']} ,'$COGS' ]} , 100]},
}},
{
    '$project' : {
        'Date' : '$_date',
        'Product Id' : '$_Product Id',
        'Product Name' : '$_Product Name',
        'Order Id' : '$_Order ID',
        'Order Status' : '$_Order Status',
        'Deal Id' : '$_Deal Id',
        'Deal Name' : '$_Deal Name',
        'Brand Id' : '$_Brand Id',
        'Brand Name' : '$_Brand',
        'Category' : '$_Category',
        'Qty Sold' : '$_Qty Sold',
        'MRP' : '$_MRP',
        'Deal Price' : '$_Deal Price',
        'COGS' : '$_COGS',
        'Order Value' : '$_Order Value',
        'SKU': '$_SKU',
        'Barcode' : '$_Barcode',
        'IMEI Nos': '$_Imei Nos',
        'Purchase Orders' : '$_Purcharse Orders',
        'Franchise Id' : '$_Franchsie Id',
        'Franchise Name' : '$_Franchise Name',
        'State' : '$_State',
        'District' : '$_District',
        'City' : '$_City',
        'Gross Profit' : '$_Gross Profit',
        'Gross Profit (%)' : '$_Gross Profit %'
    }
}
).toArray();