[
    {
        $match:
            {
                "paymentStatus": "Paid",
                "fulfilledBy": "MPS0",
                "batchEnabled": true,
                "stockAllocation": {"$nin" : ["NotAllocated"]},
                "subOrders" : {'$elemMatch' : {invoiced : false ,status : 'Confirmed' , processed : false , batchId: "" , readyForBatching :true }}
            }
    },
    {"$unwind" : "$subOrders"},
    {
        $match:
            {
                "paymentStatus": "Paid",
                "fulfilledBy": "MPS0",
                "batchEnabled": true,
                "stockAllocation": {"$nin" : ["NotAllocated"]},
                "subOrders.invoiced": false,
                "subOrders.status": "Confirmed",
                "subOrders.processed": false,
                "subOrders.batchId": "",
                "subOrders.readyForBatching": true,
            }
    },
    {"$group" : {
        "_id": "$subOrders.category.id",
        total : {$sum : 1},
        ids : {"$addToSet": {"category" : "$subOrders.category.name" , "orderIds": "$_id" }}
    }}, 
    {
            $project: {
                "_id": 0,
                category_id: "$_id",
                orderIds: "$ids.orderIds",
                category_name:

                {
                    $arrayElemAt: ["$ids.category", 0]
                },
                TotalOrders:

                {
                    $size: "$ids.orderIds"
                }
            }
        },{
            "$lookup" : {
                from : "categories",
                localField : "category_id",
                foreignField : "_id",
                as : "cat"
            }
        },
        {
            $graphLookup:

            {
                "from": "categories",
                "startWith": "$cat.parent",
                "connectFromField": "parent",
                "connectToField": "_id",
                "as": "categoryHierarchy"
            }
        },
        {
            $project:
            {
                "category": "$category_id",
                "category_name" : 1,
                "categoryHierarchy._id": 1,
                "categoryHierarchy.name": 1,
                "categoryHierarchy.parent" :1, 
                "totalOrders": "$TotalOrders",
                "orderIds" : 1,
                "chain" : {
                "$reduce" : {
                    input : "$categoryHierarchy",
                    initialValue : "",
                    in : {"$concat" : ["$$value" , 
                    {"$cond" : {
                        if:{"$ne" : ["$$value" , ""]},
                        then: ",",
                        else: ""
                    }} ,
                    "$$this._id"]}
                }
            },
            }
        },
        {
            $project: {
                categoryID: "$category",
                category_name : 1,
                chain : {$split: ["$chain" , ","]},
                MenuId:

                {
                    $cond : {if : {$gt : [ {"$size" : "$categoryHierarchy"} , 0]} , then : {$arrayElemAt: ["$categoryHierarchy._id", 0]} , else : "$category"}    //$arrayElemAt: ["$categoryHierarchy._id", 0]
                },
                MenuName:
                {
                    $cond : {if : {$gt : [ {"$size" : "$categoryHierarchy"} , 0]} , then : {$arrayElemAt: ["$categoryHierarchy.name", 0]} , else : "$category_name"}//$arrayElemAt: ["$categoryHierarchy.name", 0]
                },
                TotalOrders: "$totalOrders",
                orderIds : 1
            }
        },
       {"$group" : {
           "_id" : "$MenuId",
           categoryID : {'$first' : '$categoryID'},
           totalOrder : {"$sum" : "$TotalOrders"},
           Menu : {$first: "$MenuName"} ,
           orderIds : {'$first' : '$orderIds'},
           chain : {"$first" : '$chain' }
       }}, 
       {
            $project:
            {
                "_id": 0,
                id: "$_id",
                name: "$Menu",
                count: "$totalOrder",
                orderIds : 1,
                chain : {$concatArrays : ['$chain' , ['$categoryID']]}
            }
        }
]