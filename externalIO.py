from enum import Enum
from pathlib import Path
import requests
import configs
import csv


#query preparation
periodictyDict = {
    'hourly': 'Since 1 hour ago ',
    'daily': 'Since 1 day ago ',
    'weekly': 'Since 1 week ago '
}
# baseQuery = "SELECT count(*) from Transaction "
# baseWhereClause = "where appName in ('sg-sf','eu-sf') "
# pageViewWhereClause = "where appName in ('sg-sf','eu-sf') AND name LIKE '%ASP%' AND name NOT LIKE '%ashx%' AND name NOT LIKE '%Handler%' "
# groupByClause = "FACET MerchantId "
# groupByMidSiteUrl = "FACET MerchantId, SiteURL "
# periodicityClause = periodictyDict.get(configs.periodicity.lower(), periodictyDict.get('daily'))
# limitClause = "Limit Max"

# transactionQuery = baseQuery + baseWhereClause + groupByMidSiteUrl + periodicityClause + limitClause
# pageViewQuery = baseQuery + pageViewWhereClause + groupByClause + periodicityClause + limitClause

class RequestType(Enum):
    Transactions = 1
    PageViews = 2

def prepareQuery(requestType, appName):
    baseQuery = "SELECT count(*) from Transaction "
    groupByClause = "FACET MerchantId "
    groupByMidSiteUrl = "FACET MerchantId, SiteURL "
    periodicityClause = periodictyDict.get(configs.periodicity.lower(), periodictyDict.get('daily'))
    limitClause = "Limit Max"
    if requestType == RequestType.Transactions:
        whereClause = "where appName in (" + appName +") "
        transactionQuery = baseQuery + whereClause + groupByMidSiteUrl + periodicityClause + limitClause
        print(transactionQuery)
        return transactionQuery
    if requestType == RequestType.PageViews:
        whereClause = "where appName in (" + appName + ") AND name LIKE '%ASP%' AND name NOT LIKE '%ashx%' AND name NOT LIKE '%Handler%' and name NOT LIKE '%Scripts%' and name NOT LIKE '%Suggestions%' and name NOT LIKE '%service-worker.js' and name not like '%behindpage.aspx' "
        pageViewQuery = baseQuery + whereClause + groupByClause + periodicityClause + limitClause
        print(pageViewQuery)
        return pageViewQuery


#initialize curl requests
def initializeRequestAndGetJsonAppwise(appName, requestType = RequestType.Transactions):
    """ Initializes headers, payload and url and makes an I/O requests call to fetch json response
    :param appName: represents appName which is used in where clause of nrql
    :param requestType:(optional) Type of request :Enum:`RequestType`
    :return: json object
    :rtype: json.loads
    """ 
    headers = {
        'Accept': 'application/json',
        'X-Query-Key': configs.nrql_Key,
    }

    query = prepareQuery(requestType, appName)

    payload = {
        'nrql': query
    }

    url = "https://insights-api.newrelic.com/v1/accounts/" + configs.nrql_AccId + "/query"

    jsonResponse = requests.get(url, params=payload, headers=headers).json()
    #print(jsonResponse)
    return jsonResponse


def initializeRequestAndGetJson(requestType = RequestType.Transactions):
    """ Initializes headers, payload and url and makes an I/O requests call to fetch json response
    :param requestType:(optional) Type of request :Enum:`RequestType`
    :return: List json object
    :rtype: List json.loads
    """ 
    jsonResponseList = []
    if configs.makeClusterwiseCall:
        for appName in configs.appnames.split(','):
            jsonResponseList.append(initializeRequestAndGetJsonAppwise(appName, requestType))
        return jsonResponseList
    jsonResponseList.append(initializeRequestAndGetJsonAppwise(configs.appnames, requestType))
    return jsonResponseList


def writeRollupsToCSV(data, date):
    """
    writes rollup data into csv files based on configs
    :param data: List of Lists containing all rollup data
    :param date: mm/yyyy format date used to create file name
    :rtype: Boolean
    :return: Boolean true if no exception, false if any exception
    """
    try :
        if configs.isFileWriteEnabled and len(data) > 0 :
            Path(configs.rollupPath).mkdir(parents=True, exist_ok=True)
            filePath = configs.rollupPath + "Rollup_" + date + ".csv"
            fields = ["MerchantId", "TransactionCount", "PageViewCount"]
            with open(filePath, 'w') as rollupFile:
                csvwriter = csv.writer(rollupFile)
                csvwriter.writerow(fields)
                csvwriter.writerows(data)
        return True
    except Exception as ex:
        print(ex)
        return False

