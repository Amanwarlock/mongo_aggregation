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
        "$project": {
            paymentStatus: 1,
            fulfilledBy: 1,
            batchEnabled: 1,
            stockAllocation: 1,
            "subOrders.invoiced": 1,
            "subOrders.status": 1,
            "subOrders.processed": 1,
            "subOrders.batchId": 1,
            "subOrders.readyForBatching": 1,
            "subOrders.category": 1
        }
    },
    {
        "$group": {
            "_id": "$subOrders.category.id",
            "name": { "$first": "$subOrders.category.name" },
            "ordersIds": { "$addToSet": { "ids": "$_id" } },
            "categories": { "$addToSet": { "ids": "$subOrders.category.id" } }
        }
    },
    { "$project": { _id: 1, name: 1, ordersIds: "$ordersIds.ids", categories: "$categories.ids" } },
    {
        "$lookup": {
            from: "categories",
            localField: "_id",
            foreignField: "_id",
            as: "category"
        }
    },
    { "$project": { _id: 1, name: 1, ordersIds: 1, categories: 1, parent: { "$arrayElemAt": ["$category.parent", 0] }, count: { "$size": "$ordersIds" } } },
    {
        $graphLookup:

            {
                "from": "categories",
                "startWith": "$parent",
                "connectFromField": "parent",
                "connectToField": "_id",
                "as": "hierarchy"
            }
    },
    {
        "$project": {
            _id: 1, name: 1, ordersIds: 1, categories: 1, parent: 1, count: 1,
            hierarchy: 1,
            menudata: { "$arrayElemAt": ["$hierarchy", 0] },
            "chain": {
                "$concatArrays": [{
                    "$split": [{
                        "$reduce": {
                            input: "$hierarchy",
                            initialValue: "",
                            in: {
                                "$concat": ["$$value",
                                    {
                                        "$cond": {
                                            if: { "$ne": ["$$value", ""] },
                                            then: ",",
                                            else: ""
                                        }
                                    },
                                    "$$this._id"]
                            }
                        }
                    }, ","]
                }, ["$_id"]]
            }
        }
    },
    {
        "$project": {
            _id: 1,
            name: 1,
            ordersIds: 1,
            categories: 1,
            parent: 1,
            count: 1,
            chain: 1,
            MenuId: {
                $cond: { if: { $gt: [{ "$size": "$hierarchy" }, 0] }, then: { $arrayElemAt: ["$hierarchy._id", 0] }, else: "$_id" }    //$arrayElemAt: ["$categoryHierarchy._id", 0]
            },
            MenuName: {
                $cond: { if: { $gt: [{ "$size": "$hierarchy" }, 0] }, then: { $arrayElemAt: ["$hierarchy.name", 0] }, else: "$name" }    //$arrayElemAt: ["$categoryHierarchy._id", 0]
            },
        }
    },
    {
        "$group": {
            "_id": "$MenuId",
            menuId: { "$first": "$MenuId" },
            name: { "$first": "$name" },
            parent: { "$first": "$parent" },
            count: { "$first": "$count" },
            MenuName: { "$first": "$MenuName" },
            chain: { "$addToSet": { list: "$chain" } },
            ordersIds: { "$addToSet": { list: "$ordersIds" } },
            categories: { "$addToSet": { list: "$categories" } },
        }
    },
    {
        "$project": {
            _id: 1,
            menuId: 1,
            name: 1,
            parent: 1,
            count: 1,
            MenuName: 1,
            chain: {
                "$reduce": {
                    input: "$chain",
                    initialValue: [],
                    in: {
                        "$concatArrays": ["$$value",
                            "$$this.list"
                        ]
                    }
                }
            },
            ordersIds: {
                "$reduce": {
                    input: "$ordersIds",
                    initialValue: [],
                    in: {
                        "$concatArrays": ["$$value",
                            "$$this.list"
                        ]
                    }
                }
            },
            categories: {
                "$reduce": {
                    input: "$categories",
                    initialValue: [],
                    in: {
                        "$concatArrays": ["$$value",
                            "$$this.list"
                        ]
                    }
                }
            }
        }
    },
    {
        "$project": {
            _id: 1,
            menuId: 1,
            MenuName: 1,
            name: 1,
            count: { "$size": "$ordersIds" },
            ordersIds: 1,
            categories: 1,
            chain: {
                $reduce: {
                    input: "$chain",
                    initialValue: [],
                    in: {
                        $let: {
                            vars: { elem: { $concatArrays: [["$$this"], "$$value"] } },
                            in: { $setUnion: "$$elem" }
                        }
                    }
                }
            }
        }
    }
])



/* -----------------------version 2 working --------------------- */

[
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
            "_id": "$subOrders.category.id",
            "name": { "$first": "$subOrders.category.name" },
            "ordersIds": { "$addToSet": { "ids": "$_id" } },
            "categories": { "$addToSet": { "ids": "$subOrders.category.id" } }
        }
    },
    { "$project": { _id: 1, name: 1, ordersIds: "$ordersIds.ids", categories: "$categories.ids" } },
    {
        "$lookup": {
            from: "categories",
            localField: "_id",
            foreignField: "_id",
            as: "category"
        }
    },
    { "$project": { _id: 1, name: 1, ordersIds: 1, categories: 1, parent: { "$arrayElemAt": ["$category.parent", 0] }, count: { "$size": "$ordersIds" } } },
    {
        $graphLookup:

            {
                "from": "categories",
                "startWith": "$parent",
                "connectFromField": "parent",
                "connectToField": "_id",
                "as": "hierarchy"
            }
    },
    {
        "$project": {
            _id: 1, name: 1, ordersIds: 1, categories: 1, parent: 1, count: 1,
            hierarchy: 1,
            menudata: { "$arrayElemAt": ["$hierarchy", 0] },
            "chain": {
                "$concatArrays": [{
                    "$split": [{
                        "$reduce": {
                            input: "$hierarchy",
                            initialValue: "",
                            in: {
                                "$concat": ["$$value",
                                    {
                                        "$cond": {
                                            if: { "$ne": ["$$value", ""] },
                                            then: ",",
                                            else: ""
                                        }
                                    },
                                    "$$this._id"]
                            }
                        }
                    }, ","]
                }, ["$_id"]]
            }
        }
    },
    {
        "$project": {
            _id: 1,
            name: 1,
            ordersIds: 1,
            categories: 1,
            parent: 1,
            count: 1,
            chain: 1,
            MenuId: {
                $cond: { if: { $gt: [{ "$size": "$hierarchy" }, 0] }, then: { $arrayElemAt: ["$hierarchy._id", 0] }, else: "$_id" }    //$arrayElemAt: ["$categoryHierarchy._id", 0]
            },
            MenuName: {
                $cond: { if: { $gt: [{ "$size": "$hierarchy" }, 0] }, then: { $arrayElemAt: ["$hierarchy.name", 0] }, else: "$name" }    //$arrayElemAt: ["$categoryHierarchy._id", 0]
            },
        }
    },
    {
        "$group": {
            "_id": "$MenuId",
            menuId: { "$first": "$MenuId" },
            name: { "$first": "$name" },
            parent: { "$first": "$parent" },
            count: { "$first": "$count" },
            MenuName: { "$first": "$MenuName" },
            chain: { "$addToSet": { list: "$chain" } },
            ordersIds: { "$addToSet": { list: "$ordersIds" } },
            categories: { "$addToSet": { list: "$categories" } },
        }
    },
    {
        "$project": {
            _id: 1,
            menuId: 1,
            name: 1,
            parent: 1,
            count: 1,
            MenuName: 1,
            chain: {
                "$reduce": {
                    input: "$chain",
                    initialValue: [],
                    in: {
                        "$concatArrays": ["$$value",
                            "$$this.list"
                        ]
                    }
                }
            },
            ordersIds: {
                "$reduce": {
                    input: "$ordersIds",
                    initialValue: [],
                    in: {
                        "$concatArrays": ["$$value",
                            "$$this.list"
                        ]
                    }
                }
            },
            categories: {
                "$reduce": {
                    input: "$categories",
                    initialValue: [],
                    in: {
                        "$concatArrays": ["$$value",
                            "$$this.list"
                        ]
                    }
                }
            }
        }
    },
    {
        "$project": {
            _id: 0,
            menuId: 1,
            MenuName: 1,
            count: { "$size": "$ordersIds" },
            ordersIds: 1,
            categories: 1,
            chain: {
                $reduce: {
                    input: "$chain",
                    initialValue: [],
                    in: {
                        $let: {
                            vars: { elem: { $concatArrays: [["$$this"], "$$value"] } },
                            in: { $setUnion: "$$elem" }
                        }
                    }
                }
            }
        }
    }
]