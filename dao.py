import configs
from models import MerchantData, TransactionData, TransactionRollupData
from pymongo import MongoClient
from datetime import datetime, timedelta
from uuid import UUID
from externalIO import writeRollupsToCSV
#mongo setup
mongoClient = MongoClient(configs.mongoUri)
db = mongoClient["NewRelicData"]
merchantDataCollection = db["MerchantData"]
transactionDataCollection = db["TransactionData"]
transactionRollupDataCollection = db["TransactionRollupData"]

#merchantData setup
mongoActiveMerchantIds = set(merchantDataCollection.distinct("merchantId" , {"isActive": True}))
mongoAllMerchantIds = set(merchantDataCollection.distinct("merchantId"))
mongoInActiveMerchantIds = mongoAllMerchantIds - mongoActiveMerchantIds
newrelicMerchantIds = []
newrelicMerchantIdSiteUrlDict = {}

#merchantDataCollection.update_one({"merchantId":merchantDataobj.merchantId}, {"$set" : {"siteUrl" : merchantDataobj.siteUrl ,
        #                                    "isActive": merchantDataDict.isActive, "createdDate": merchantDataobj.createdDate}}, upsert=True)
def insertDocsIntoMongo(insertDocs, collection, isMany = True):
    """Insert docs into mongo
    :param insertDocs: List of dicts to insert into mongo collection
    :param collection: Mongo collection to insert into
    :param isMany: Boolean to specify one or many docs to be inserted
    """
    if len(insertDocs) > 0:
        if isMany:
            collection.insert_many(insertDocs)
        else:
            collection.insert_one(insertDocs[0])
        

def createMerchantMetadataDocs(merchantIds):
    """Creates Mongo insert Docs of merchant Meta data
    :param merchantIds: enumerating object of merchant Ids
    :rtype: List 
    :return: List of insert Docs
    """
    insertDocs = []
    if len(merchantIds) > 0:
        for merchantId in merchantIds:
            merchantDataobj = MerchantData(merchantId, newrelicMerchantIdSiteUrlDict[merchantId])
            insertDocs.append(merchantDataobj.__dict__)
    return insertDocs

def updateMerchantMetaData(merchantIds, isActive):
    """Updates MerchantData collection Meta data like isActive Flag and updated date
    :param merchantIds: List of merchantIds
    :param isActive: Boolean flag representing status of merchant
    """
    if len(merchantIds) > 0:
        _createdDate = datetime.utcnow()
        merchantDataCollection.update_many({"merchantId" : {"$in" : merchantIds}},
                                            {"$set" : {"isActive" : isActive, "createdDate" : _createdDate}})


def processTransactionJson(transactionJsonResponses, newrelicMerchantData):
    """Create/Update Merchant Meta data and update transaction count
    :param transactionJsonResponses: List of Json object of transaction data '[sgJson, euJson]/[common]
    :param newrelicMerchantData: Dictionary of merchantTransactionData
    :rtype: Dictionary
    :return: Dictionary of merchantTransactionData
    """

    #{'name': ['6c57599f-2c43-4c82-806a-e07c3410f5d3', 'https://www.biba.in'], 'results': [{'count': 1776875}]}
    for transactionJsonResponse in transactionJsonResponses:
        for merchantData in transactionJsonResponse["facets"]:
            binmerchantId = UUID(merchantData["name"][0])
            newrelicMerchantIds.append(binmerchantId)
            newrelicMerchantIdSiteUrlDict[binmerchantId] = merchantData["name"][1]
            if binmerchantId in newrelicMerchantData:
                newrelicMerchantData[binmerchantId][0] = merchantData["results"][0]["count"]
            else:
                newrelicMerchantData[binmerchantId] = [merchantData["results"][0]["count"], 0]

    setNewrelicMids = set(newrelicMerchantIds)
    newMerchantIds =  setNewrelicMids - mongoAllMerchantIds

    depricatedMerchantIds = list(mongoActiveMerchantIds - setNewrelicMids)
    renewedMerchantIds = list((setNewrelicMids - newMerchantIds).intersection(mongoInActiveMerchantIds))

    insertDocs = createMerchantMetadataDocs(newMerchantIds)
    insertDocsIntoMongo(insertDocs, merchantDataCollection)
    #merchantDataCollection.insert_many(insertDocs)

    updateMerchantMetaData(depricatedMerchantIds, False)
    updateMerchantMetaData(renewedMerchantIds, True)

    return newrelicMerchantData

def processPageViewJson(pageViewJsonResponses, newrelicMerchantData):
    """Update Merchant Pageview count
    :param pageViewJsonResponse: List of Json object of pageView data '[sgJson, euJson]/[common]
    :param newrelicMerchantData: Dictionary of merchantTransactionData
    :rtype: Dictionary
    :return: Dictionary of merchantTransactionData
    """
    for pageViewJsonResponse in pageViewJsonResponses:
        for pageViewData in pageViewJsonResponse["facets"]:
            binmerchantId = UUID(pageViewData["name"])
            if binmerchantId in newrelicMerchantData:
                newrelicMerchantData[binmerchantId][1] = pageViewData["results"][0]["count"]
            else:
                newrelicMerchantData[binmerchantId] = [0, pageViewData["results"][0]["count"]]
    return newrelicMerchantData


def createTransactiondataDocs(newrelicMerchantData):
    """Create transaction data docs
    :param newrelicMerchantData: Dictionary of merchantTransactionData
    :rtype: Dictionary
    :return: Dictionary of TransactionData object
    """
    emptyDoc = {}
    if len(newrelicMerchantData) > 0:
        transactionDataobj = TransactionData()
        for merchantId in newrelicMerchantData:
            transactionDataobj.setOneMerchantTransactionData(merchantId, newrelicMerchantData[merchantId][0], newrelicMerchantData[merchantId][1])
        return transactionDataobj.__dict__
    return emptyDoc

def insertTransactionDataIntoMongo(newrelicMerchantData):
    """Create transaction data docs
    :param newrelicMerchantData: Dictionary of merchantTransactionData
    """
    insertDocs = []
    insertDocs.append(createTransactiondataDocs(newrelicMerchantData))
    insertDocsIntoMongo(insertDocs, transactionDataCollection, False)

def mapMonthlyDataToObjectAndInsertToMongo(monthlyData, date):
    """maps monthly data dict to TransactionRollupData object
    :param monthlyData: Dictioary of cummulative monthly data
    :param date: Datetime object to add rollup date mm/YYYY
    """
    insertDocs = []
    if len(monthlyData) > 0:
        transactionRollupDataobj = TransactionRollupData()
        transactionRollupDataobj.setMonthlyRollupData(date.strftime("%m/%Y"), monthlyData)
        insertDocs.append(transactionRollupDataobj.__dict__)
        insertDocsIntoMongo(insertDocs, transactionRollupDataCollection, False)

def writeMonthlyDataToLocalFiles(monthlyData, date):
    """maps monthly data dict to csv rollup list of lists
    :param monthlyData: Dictioary of cummulative monthly data
    :param date: Datetime object to add rollup date mm/YYYY
    :rtype: Boolean
    :return: Boolean true if file write is success, false otherwise
    """
    csvData = []
    if len(monthlyData) > 0:
        for merchantId in monthlyData:
            merchantData = [merchantId, monthlyData[merchantId][0], monthlyData[merchantId][1]]
            csvData.append(merchantData)
        saveSuccess = writeRollupsToCSV(csvData, date.strftime("%m-%Y"))
    return saveSuccess


def createTransactionRollupData():
    """creates TransactionRollupData if next day is new month
    """
    monthlyData = {} #{merchantId: [txCount, PageViewCount]}
    today = datetime.utcnow()
    nextday = today + timedelta(days=1)
    if today.month != nextday.month:
        monthStart = datetime(today.year, today.month, 1)
        monthEnd = datetime(nextday.year, nextday.month, 1)
        dailyDataDocs = transactionDataCollection.find({"createdDate":{"$gte":monthStart, "$lt": monthEnd}})
        for doc in dailyDataDocs:
            if "transactions" in doc:
                for merchantId in doc["transactions"]:
                    if merchantId not in monthlyData:
                        monthlyData[merchantId] = doc["transactions"][merchantId]
                    else:
                        monthlyData[merchantId][0] += doc["transactions"][merchantId][0]
                        monthlyData[merchantId][1] += doc["transactions"][merchantId][1]
        
        mapMonthlyDataToObjectAndInsertToMongo(monthlyData, today)
        writeMonthlyDataToLocalFiles(monthlyData, today)
