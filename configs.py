from configparser import ConfigParser

#initialize configset
config = ConfigParser()
config.read("configs.ini")

#read configs and set defaults if absent
#newrelic section
nrql_Key = config.get('NEWRELIC', 'NewRelicApiKey' ,fallback="")
nrql_AccId = config.get('NEWRELIC', 'NewRelicAccountId', fallback="")

#appsetting section
isPageViewsEnabled = config.getboolean('APPSETTINGS', 'IncludePageViews', fallback=False)
periodicity = config.get('APPSETTINGS', 'Periodicity', fallback="Daily")
makeClusterwiseCall = config.getboolean('APPSETTINGS', 'MakeClusterwiseCall', fallback=False)
appnames = config.get('APPSETTINGS', 'AppNames', fallback="'sg-sf','eu-sf'")
isFileWriteEnabled = config.getboolean('APPSETTINGS', 'WriteToLocalFiles', fallback=False)

#mongo section
mongoUri = config.get('MONGO', 'MongoUrl', fallback='mongodb://localhost:27017/')

#file section
logPath = config.get('FILESETTINGS', 'LogDirectoryPath', fallback='E:\\Logs\\')
rollupPath = config.get('FILESETTINGS', 'MonthlyRollupPath', fallback='E:\\MonthlyRollups\\')