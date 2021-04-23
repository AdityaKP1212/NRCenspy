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