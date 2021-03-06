db.omsmasters.aggregate([{$match:{"franchise.id" : "F100087" ,_id : "OR201712285",'status':{$nin:['Created','Cancelled']}}},
{$unwind:{'path':'$subOrders',preserveNullAndEmptyArrays: true}},
{$unwind:{'path':'$invoices',preserveNullAndEmptyArrays: true}},
{$group:
    {_id:'$_id',total:{$sum:'$orderAmount'},
    'ids':{$push:{'id':'$franchise.id','name':'$franchise.name','state':'$franchise.state','district':'$franchise.district','city':'$franchise.city','date':'$createdAt','status':'$status','dealid':'$subOrders.id','deal':'$subOrders.name','mrp':'$subOrders.mrp','MP':'$subOrders.memberPrice','B2B':'$subOrders.b2bPrice','shipping':'$subOrders.logistics','qty':'$subOrders.quantity','payment':'$paymentStatus','partner':'$fulfilledBy','payment_mode':'$paymentMode','RMF':'$franchise.parent',category:'$subOrders.category.name',brand:'$subOrders.brand.name',paymentOn:'$paymentDate','inv':'$invoices.invoiceNo','box':'$invoices.boxId'}}}},
    {$unwind:{'path':'$ids',preserveNullAndEmptyArrays: true}},
    {$lookup:{from:'franchises',localField:'ids.RMF',foreignField:'_id',as:'frandet'}},
    {$unwind:{'path':'$frandet',preserveNullAndEmptyArrays: true}},
    {$lookup:{from:'sourcingpartners',localField:'ids.partner',foreignField:'_id',as:'sourcedet'}},
    {$unwind:{'path':'$sourcedet',preserveNullAndEmptyArrays: true}},
    {$lookup:{from:'logistics',localField:'ids.inv',foreignField:'invoiceNo',as:'logdet'}},
    {$unwind:{'path':'$logdet',preserveNullAndEmptyArrays: true}},
    {$lookup:{from:'logistics',localField:'ids.id',foreignField:'ReceivedBy',as:'logfrandet'}},
    {$unwind:{'path':'$logfrandet',preserveNullAndEmptyArrays: true}},
    {$lookup:{from:'motherboxes',localField:'logdet._id',foreignField:'logisticsId',as:'boxdet'}},
    {$unwind:{'path':'$boxdet',preserveNullAndEmptyArrays: true}},
    {$project:{
        'date':{$dateToString:{format:'%Y-%m-%d',date:'$ids.date'}},
        '_id':0,franchise_id:'$ids.id',
        name:'$ids.name',state:'$ids.state',
        city:'$ids.city',district:'$ids.district',
        Orderid:'$_id',dealid:'$ids.dealid',deal:'$ids.deal',MRP:'$ids.mrp',
        B2B:'$ids.B2B',MP:'$ids.MP',qty:'$ids.qty',shipping:'$ids.shipping',
        total:'$total',status:'$ids.status',payment_status:'$ids.payment',
        PartnerId:'$ids.partner',Payment_mode:'$ids.payment_mode',PartnerName:'$sourcedet.name',
        RMFId:'$ids.RMF',RMF:'$frandet.name',
        Category:'$ids.category',Brand:'$ids.brand',
        PaymentOn:'$ids.paymentOn',InvoiceNo:'$ids.inv',BoxId:'$ids.box',
        PackageStatus:'$logdet.status','Package_received':{$dateToString:{format:'%Y-%m-%d',date:'$logdet.arrivedAtRf'}},
        ReceivedBy:'$frandet.name',
        ReceivedByType:'$frandet.sk_franchise_details.franchise_type',
        Courier:'$boxdet.courierName',AWb:'$boxdet.awbNumber',
        boxweight:'$boxdet.volumetricWeight',boxlength:'$boxdet.boxDetails.length',
        boxbreadth:'$boxdet.boxDetails.breadth',boxheight:'$boxdet.boxDetails.height',
        ReceivedOn:'$logdet.ReceivedOn',CustomerNo:'$logdet.customer.contact',CustomerName:'$logdet.customer.name',
        pnhID:'$frandet.sk_franchise_details.code',shippedOn:'$logdet.createdAt',customerDelivered:'$logdet.lastUpdated'}}]).toArray()




        /* New Query */
        db.omsmasters.aggregate([{$match:{"franchise.id" : "F100087", _id : "OR201712285",'status':{$nin:['Created','Cancelled']}}},
        {$unwind:{'path':'$subOrders',preserveNullAndEmptyArrays: true}},
        {$unwind:{'path':'$invoices',preserveNullAndEmptyArrays: true}},
        {$group:
            {_id:'$_id',total:{$sum:'$orderAmount'},
            'ids':{$push:{'id':'$franchise.id',
                            'name':'$franchise.name','state':'$franchise.state','district':'$franchise.district','city':'$franchise.city',
                            'date':'$createdAt','status':'$status','dealid':'$subOrders.id',
                            'deal':'$subOrders.name','mrp':'$subOrders.mrp','MP':'$subOrders.memberPrice','B2B':'$subOrders.b2bPrice',
                            'shipping':'$subOrders.logistics','qty':'$subOrders.quantity',
                            'payment':'$paymentStatus','partner':'$fulfilledBy','payment_mode':'$paymentMode',
                            'RMF':'$franchise.parent',category:'$subOrders.category.name',brand:'$subOrders.brand.name',
                            paymentOn:'$paymentDate','inv':'$invoices.invoiceNo','box':'$invoices.boxId'}}}
            
        },
        {$unwind:{'path':'$ids',preserveNullAndEmptyArrays: true}},
        {$lookup:{from:'franchises',localField:'ids.RMF',foreignField:'_id',as:'frandet'}},
        {$unwind:{'path':'$frandet',preserveNullAndEmptyArrays: true}},
        {$lookup:{from:'sourcingpartners',localField:'ids.partner',foreignField:'_id',as:'sourcedet'}},
        {$unwind:{'path':'$sourcedet',preserveNullAndEmptyArrays: true}},
        {$lookup:{from:'logistics',localField:'ids.inv',foreignField:'invoiceNo',as:'logdet'}},
        {$unwind:{'path':'$logdet',preserveNullAndEmptyArrays: true}},
        {$lookup:{from:'logistics',localField:'ids.id',foreignField:'ReceivedBy',as:'logfrandet'}},
        {$lookup:{from:'motherboxes',localField:'logdet._id',foreignField:'logisticsId',as:'boxdet'}},
        {$unwind:{'path':'$boxdet',preserveNullAndEmptyArrays: true}},
        {$project:{
                'date':{$dateToString:{format:'%Y-%m-%d',date:'$ids.date'}},
                '_id':0,franchise_id:'$ids.id',
                name:'$ids.name',state:'$ids.state',
                city:'$ids.city',district:'$ids.district',
                Orderid:'$_id',dealid:'$ids.dealid',deal:'$ids.deal',MRP:'$ids.mrp',
                B2B:'$ids.B2B',MP:'$ids.MP',qty:'$ids.qty',shipping:'$ids.shipping',
                total:'$total',status:'$ids.status',payment_status:'$ids.payment',
                PartnerId:'$ids.partner',Payment_mode:'$ids.payment_mode',PartnerName:'$sourcedet.name',
                RMFId:'$ids.RMF',RMF:'$frandet.name',
                Category:'$ids.category',Brand:'$ids.brand',
                PaymentOn:'$ids.paymentOn',InvoiceNo:'$ids.inv',BoxId:'$ids.box',
                PackageStatus:'$logdet.status','Package_received':{$dateToString:{format:'%Y-%m-%d',date:'$logdet.arrivedAtRf'}},
                ReceivedBy:'$frandet.name',
                ReceivedByType:'$frandet.sk_franchise_details.franchise_type',
                Courier:'$boxdet.courierName',AWb:'$boxdet.awbNumber',
                boxweight:'$boxdet.volumetricWeight',boxlength:'$boxdet.boxDetails.length',
                boxbreadth:'$boxdet.boxDetails.breadth',boxheight:'$boxdet.boxDetails.height',
                ReceivedOn:'$logdet.ReceivedOn',CustomerNo:'$logdet.customer.contact',CustomerName:'$logdet.customer.name',
                pnhID:'$frandet.sk_franchise_details.code',shippedOn:'$logdet.createdAt',customerDelivered:'$logdet.lastUpdated'}}
            ]);
        