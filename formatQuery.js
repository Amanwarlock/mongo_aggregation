db.omsmasters.aggregate({'$match':{status:{$nin:['Created','Cancelled']}}},{'$unwind':'$subOrders'},{'$unwind':'$subOrders.products'},{'$lookup':{from:'purchaseorders',localField:'productData._id',foreignField:'products.productId',as:'POs'}},{'$lookup':{from :'franchises',localField:'franchise.id',foreignField :'_id',as:'franchiseData'}},{'$unwind':'$franchiseData'},{'$lookup':{from :'products',localField : 'subOrders.products.id',foreignField : '_id',as : 'productData'}},
{'$unwind':'$productData'},{'$lookup':{from:'purchaseorders',localField:'productData._id',foreignField:'products.productId',
    as: 'POs'
}},
{'$project' : {
    data : '$$ROOT',
    'POs' : {'$arrayElemAt' : ['$POs' , 0]},
}},
{'$project' : {
   data : 1,
   poProd : '$POs.products'
}},
{'$project' :{
    data : 1,
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
{'$project' : {
    'Order ID' : '$data._id',
    'date' : {$dateToString:{format:'%Y-%m-%d',date:'$data.createdAt'}},
    'Order Status' : '$data.status',
    'SKU' : '$data.productData.skuCode',
    'Barcode' : '$data.productData.barcode',
    'Deal Id' : '$data.subOrders.id',
    'Product Id' : '$data.subOrders.products.id',
    'Deal Name' : '$data.subOrders.name',
    'Product Name' : '$data.productData.name',
    'Brand' : '$data.subOrders.brand.name',
    'Brand Id' : '$data.subOrders.brand.id',
    'Category'  : {
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
    }
,
    'MRP' : '$data.subOrders.products.mrp',
    'Deal Price' : '$data.subOrders.b2bPrice',
    'Qty Sold' : '$data.subOrders.products.quantity',
    'Order Value' : {'$multiply' : ['$data.subOrders.b2bPrice'  , '$data.subOrders.quantity']},
    'Gross Profit %' : '',
    'Franchsie Id' : '$data.franchiseData._id',
    'Franchise Name ' : '$data.franchiseData.name',
    'State' : '$data.franchiseData.state',
    'City' : '$data.franchiseData.city',
    'District' : '$data.franchiseData.district',
    'Purcharse Orders' : {
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
    'COGS' :1,
    'Gross Profit' : {'$subtract' : ['$data.subOrders.b2bPrice' , '$COGS']},
    'Gross Profit %' : {'$multiply' : [{'$divide' : [{'$subtract' : ['$data.subOrders.b2bPrice' , '$COGS']} ,'$COGS' ]} , 100]}

}}
).toArray()