/**
 * METHOD -1
 */
db.blogs.aggregate(
    [
        { $group: { _id: null, uniqueTags: { $push: "$tags" } } },
        {
            $project: {
                _id: 0,
                uniqueTags: {
                    $reduce: {
                        input: "$uniqueTags",
                        initialValue: [],
                        in: {
                            $let: {
                                vars: { elem: { $concatArrays: ["$$this", "$$value"] } },
                                in: { $setUnion: "$$elem" }
                            }
                        }
                    }
                }
            }
        }
    ]
)

/**
 * METHOD - 2;
 * { _id : 1 , data : [7,4,0] }

{ _id : 2 , data : [4,5,6] }

{ _id : 3 , data : [6,7,8] }
 */
db.coll.aggregate([
    { "$match": { "_id": { "$in": [1, 2] } } },
    {
        "$group": {
            "_id": 0,
            "data": { "$push": "$data" }
        }
    },
    {
        "$project": {
            "data": {
                "$reduce": {
                    "input": "$data",
                    "initialValue": [],
                    "in": { "$setUnion": ["$$value", "$$this"] }
                }
            }
        }
    }
])

/**
 * METHOD - 3;
*/
db.omsmasters.aggregate([{"$project" : {
    _id : 0,
    id : "$menuId",
    name : "$MenuName",
    count: {"$size" : "$ordersIds"},
    chain : {
        "$reduce" : {
            input : "$chain",
            initialValue : [],
            in : {
                "$setUnion": ["$$value" , ["$$this"]]
            }
        }
    }}}]);