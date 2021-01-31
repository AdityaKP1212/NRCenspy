import externalIO
import configs
import dao

#get TransactionData
transactionJsonResponses = externalIO.initializeRequestAndGetJson()
transactionData = {}
transactionData = dao.processTransactionJson(transactionJsonResponses, transactionData)

if configs.isPageViewsEnabled :
    pageViewJsonResponses = externalIO.initializeRequestAndGetJson(externalIO.RequestType.PageViews)
    transactionData = dao.processPageViewJson(pageViewJsonResponses, transactionData)

dao.insertTransactionDataIntoMongo(transactionData)
dao.createTransactionRollupData()

