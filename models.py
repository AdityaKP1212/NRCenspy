import uuid
import datetime
import pymongo
class MerchantData:
    #if isActive = T then createdDate is createdDate if isActive = F then createdDate is lastUpdatedDate
    def __init__(self, merchantId = None, siteUrl = None, isActive = True, createdDate = datetime.datetime.utcnow()):
        self.merchantId = merchantId
        self.siteUrl = siteUrl
        self.isActive = isActive
        self.createdDate = createdDate

    def __str__(self):
        return "MerchantId: {0}, SiteUrl: {1}, IsActive: {2}, CreatedDate: {3}".format(self.merchantId, self.siteUrl, self.isActive, self.createdDate)

    def setMerchantData(self, merchantId, siteUrl, isActive = True):
        self.merchantId = uuid.UUID(merchantId)
        self.siteUrl = siteUrl
        self.isActive = isActive
        self.createdDate = datetime.datetime.utcnow()

class TransactionData:
    def __init__(self):
        self.createdDate = datetime.datetime.utcnow()
        self.transactions = {}
        self.lastUpdatedDate = datetime.datetime.now()

    def __str__(self):
        return "createdDate: {0}, transactionsCount: {1}, lastUpdatedDate: {2}".format(self.createdDate, len(self.transactions), self.lastUpdatedDate)

    def setOneMerchantTransactionData(self, merchantId, transactionCount, pageviewCount):
        self.transactions[str(merchantId)] = [transactionCount, pageviewCount]
        #self.transactions.append({"id": uuid.UUID(merchantId), "transactions": transactionCount, "pageviews": pageviewCount})
        self.lastUpdatedDate = datetime.datetime.now()

class TransactionRollupData:
    def __init__(self, rollupDate = datetime.datetime.utcnow().strftime("%m/%Y")):
        self.createdDate = datetime.datetime.utcnow()
        self.rollupDate = rollupDate
        self.transactions = {}

    def __str__(self):
        return "createdDate:{0}, transactionCount: {1}, rollupDate: {3}".format(self.createdDate, len(self.transactions), self.rollupDate)

    def setMonthlyRollupData(self, rollupDate, transactionDict):
        self.rollupDate = rollupDate
        self.transactions = transactionDict

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
    merchantData.insert_one(transdata.__dict__)
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
#test()