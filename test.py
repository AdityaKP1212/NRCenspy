import uuid
import datetime
import pymongo
from models import MerchantData, TransactionData



def test():
    biba = MerchantData()
    print(biba)
    biba.setMerchantData("d457599f-2c43-4c82-806a-e07c3410f5e9", "biba.in11", False)
    print(biba)
    print(biba.__dict__)

    transdata = TransactionData()
    print(transdata)
    transdata.setOneMerchantTransactionData(uuid.uuid4(), 100, 40)
    print(transdata.__dict__)
    #testing
    mongoClient = pymongo.MongoClient()
    db = mongoClient["test"]
    merchantData = db["test1"]
    ids = merchantData.distinct("merchantId")
    print(ids)
    print(sorted(ids))
    if uuid.UUID("6c57599f-2c43-4c82-806a-e07c3410f5e4") in ids:
        print("yes")
    else:
        print("no")
    #merchantData.insert_one(transdata.__dict__)
    merchantData.update_one({"merchantId":biba.merchantId}, {"$set" : {"siteUrl" : biba.siteUrl ,
                                        "isActive": biba.isActive, "createdDate": biba.createdDate}}, upsert=True)
    #pymongo.UpdateMany({""})
    #merchantData.bulk_write(requests= [])
    currdate = datetime.datetime.utcnow()
    nextmonth = currdate + datetime.timedelta(days=6)
    monthstart = datetime.datetime(currdate.year, currdate.month, 1)
    monthend = datetime.datetime(nextmonth.year, nextmonth.month, 1)
    data = merchantData.find({"createdDate":{"$gte":monthstart, "$lt": monthend}})
    for x in data:
        if "merchantId" in x:
            print(type(x["merchantId"]))
            print(x["merchantId"])
           #print(x["merchantId"].toCSUUID())

    data = merchantData.find({"siteUrl":"biba.in11"})
    for x in data:
        if "merchantId" in x:
            print(type(x["merchantId"]))
            print(str(x["merchantId"]))
test()