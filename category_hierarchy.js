
//db.categories.find({_id : {"$in" : ["C5156" , "C4190" , "C3642"]}})

db.categories.aggregate([
    {"$match" : {_id : "C5156"}},
    {
        "$graphLookup" : {
             from: "categories",
             startWith: "$parent", 
             connectFromField: "parent",
             connectToField: "_id",
             as: "categoryHierarchy"
          },
    }
]);


var parentIds = ["C5156"];


/* First element in the array is the top most parent and the subsequent elements are the immediate kin of the category */
db.categories.aggregate([
    { "$match": { "_id": { "$in": ["C5155"] } } },
    {
        "$graphLookup": {
            from: "categories",
            startWith: "$parent",
            connectFromField: "parent",
            connectToField: "_id",
            as: "hierarchy"
        },
    },
    {
        "$addFields": {
            hierarchy : {
                 $concatArrays : [ "$hierarchy" , [{"_id"  : "$_id" , name : "$name" , "parent" : "$parent"}]  ]
            },
        }
    },
    {
        "$addFields": {
             hierarchyChain : {
                "$reduce" : {
                    input : "$hierarchy",
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
        "$project" : {
            _id : "$data._id",
            name : "$data.name",
            parent : "$data.parent",
            "hierarchy._id" : 1,
            "hierarchy.name" : 1,
            "hierarchy.parent" : 1,
            hierarchyChain : {$split: ["$hierarchyChain" , ","]}
        }
    }
]);

