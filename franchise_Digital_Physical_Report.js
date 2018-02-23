//FINAL WORKING QUERY - COMBINED;

db.franchises.aggregate({"$lookup":{
    from : "omsmasters",
    localField : "_id",
    foreignField : "franchise.id",
    as: "physical"
}},
{"$lookup" : {
    from : "rechargeorders",
    localField : "_id",
    foreignField : "franchise",
    as: "digital"
}},
{"$project" : {
    physicalVolume : {
        "$reduce" : {
            input : {
                "$filter" : {
                    input : "$physical",
                    cond : {"$eq" : ["$$this.paymentStatus" , "Paid"]}
                }
            },
            initialValue : 0,
            in : {"$add" : ["$$value" , "$$this.orderAmount"]}
        }
    },
    
    digitalVolume : {
        "$reduce" : {
            input : {"$filter" : {
                input : "$digital",
                cond : {"$eq" : ["$$this.status" , "Closed"]}
            }},
            initialValue : 0,
            in : {"$add" : ["$$value" , "$$this.orderAmount"]}
        }
    },
     data : "$$ROOT"
}},
{"$project" : {
    physicalVolume : 1,
    digitalVolume : 1,
    totalVolume : {"$add" : ["$physicalVolume" , "$digitalVolume"]},
    "franchiseId" : "$_id" , 
    "name" : "$data.name" ,
    "franchise code" : "$data.sk_franchise_details.code",
    "franchise type" : "$data.sk_franchise_details.franchise_type",
    "RMF name" : "$data.sk_franchise_details.linked_rmf",
    "joining date" : "$data.createdAt",
    "mobile No" : "$data.OwnerMobileNo",
    "State" : "$data.state",
    "District" : "$data.district",
    "City" : "$data.city",
    "area" : "$data.town",
    "RCM name" : "",
    "transaction Date" : "",
    
}}

)