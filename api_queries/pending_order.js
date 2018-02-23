db.omsmasters.aggregate([
    {
        $match:
        {
            "paymentStatus": "Paid",
            "fulfilledBy": "MPS0",
            "batchEnabled": true,
            "stockAllocation": { "$nin": ["NotAllocated"] },
            "subOrders": { '$elemMatch': { invoiced: false, status: 'Confirmed', processed: false, batchId: "", readyForBatching: true } }
        }
    },
    { "$unwind": "$subOrders" },
    {
        $match:
        {
            "subOrders.invoiced": false,
            "subOrders.status": "Confirmed",
            "subOrders.processed": false,
            "subOrders.batchId": "",
            "subOrders.readyForBatching": true,
        }
    },
    {
        "$group": {
                "_id" : "$subOrders.category.id",
                count : {"$sum" : 1},
                orders : {"$addToSet": {"orderList" : "$_id"}},
                ids : {"$addToSet": {"category" : "$subOrders.category.name" , catId : "$subOrders.category.id", "orderIds": "$_id" }}
        }
    }
])