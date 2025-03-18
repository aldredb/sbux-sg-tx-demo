use("starbucks")
db.wallets.insertMany([{
    "_id": "WALLET001",
    "balance": 100,
}, {
    "_id": "WALLET002",
    "balance": 80,
}]);

use("starbucks")
db.orders.insertMany([{
    "_id": "1",
    "products": [{
        "sku": "SBX001",
        "name": "Caramel Machiatto",
        "price": 50,
        "quantity": 10
    }],
    "order": [{
        "status": "processed",
        "date": "2025-01-01T12:02:00Z"
    },{
        "status": "submitted",
        "date": "2025-01-01T12:00:00Z"
    }],
}, 
{
    "_id": "2",
    "products": [{
        "sku": "SBX001",
        "name": "Americano",
        "price": 50,
        "quantity": 10
    }],
    "order": [{
        "status": "processed",
        "date": "2025-01-01T12:02:00Z"
    },{
        "status": "submitted",
        "date": "2025-01-01T12:00:00Z"
    }],
}
]);


use("starbucks")
db.wallets.updateOne(
    { "_id" : "WALLET001", "balance": { $gte: 50 } },
    { $inc: { "balance": -50 } }
);

use("starbucks")
db.wallets.findAndModify({
    query: { "_id" : "WALLET001", "balance": { $gte: 50 } },
    update: { $inc: { balance: -50 } },
    new: true
})
// {
//     "_id": "WALLET001",
//     "balance": 0
//   }
// If no match, return null

use("starbucks")
db.wallets.findAndModify({
    query: { "_id" : "WALLET001" },
    update: [
        { $set: { balance: { $cond: {
            if: { $gt: ["$balance", 60] },
            then: { $subtract: ["$balance", 60] },
            else: 0
        }} } }
    ],
    new: true
})


