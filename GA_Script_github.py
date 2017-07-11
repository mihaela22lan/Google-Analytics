import argparse
import httplib2
import os
import logging
import MySQLdb
import time
import sys
from apiclient.discovery import build
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client import tools
from datetime import date, datetime, timedelta


encoding = 'utf8'
reload(sys)
sys.setdefaultencoding('utf8')

# Globals
today = date.today()
error_file = 'C:\IN_ORA\Quick Solutions\GoogleAnalytics\error.txt'
profile_id = []
client_secrets = 'C:\IN_ORA\Quick Solutions\GoogleAnalytics\Script\client_secrets.json'
flow = flow_from_clientsecrets(client_secrets,scope='https://www.googleapis.com/auth/analytics.readonly',message='%s is missing' % client_secrets)
token_file_name = 'C:\IN_ORA\Quick Solutions\GoogleAnalytics\Script\credentials.dat'

# Logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler('C:\IN_ORA\Quick Solutions\GoogleAnalytics\Log_File.log')
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
fh.setFormatter(formatter)
fh.setLevel(logging.DEBUG)
logger.addHandler(fh)


db = MySQLdb.connect( host='internal-db1.colo', user='<USERNAME>', passwd='<PASS>', charset='utf8')
cursor = db.cursor()
sql = "SELECT max(`DATE_DT`) FROM `BISetup`.`GA_BD_Cliq`"

cursor.execute(sql)
result = cursor.fetchone()[0]
start_dt = result + timedelta(days=1)
end_dt = start_dt
username = '<GA_ACCOUNT>'


def create_error_file():
    with open(error_file, 'w') as err:
        err.write('')

def prepare_credentials():
    parser = argparse.ArgumentParser(parents=[tools.argparser])
    flags = parser.parse_args()
    # Retrieve existing credendials
    storage = Storage(token_file_name)
    credentials = storage.get()
    # If no credentials exist, we create new ones
    if credentials is None or credentials.invalid:
        credentials = tools.run_flow(flow, storage, flags)
    return credentials

def initialize_service():
    """
    Creates an http object and authorize it using, the function prepare_creadentials()
    """
    http = httplib2.Http()
    credentials = prepare_credentials()
    http = credentials.authorize(http)
    return build('analytics', 'v3', http=http)

def get_accounts_ids(service):
    accounts = service.management().accounts().list().execute()
    ids = []
    if accounts.get('items'):
        for account in accounts['items']:
            ids.append(account['id'])
    return ids


def get_account_name(LoopAccountId):
    accounts = service.management().accounts().list().execute()
    account_name = []
    if accounts.get('items'):
        for account in accounts['items']:
            if account['id'] == LoopAccountId:
                account_name = account['name']
    return account_name


def get_source_group2(service, profile_id, start_date, end_date):
    ids = "ga:" + str(profile_id)
    metrics = "ga:sessions,ga:users,ga:newUsers,ga:pageviews,ga:bounceRate,ga:avgSessionDuration,ga:percentNewSessions"
    dimensions = "ga:hostname,ga:country,ga:pagePath"

    try:
        data = service.data().ga().get(ids=ids, start_date=start_date, end_date=end_date, metrics=metrics, dimensions=dimensions).execute()
        return dict(data)
    except Exception as e:
        logger.info("exception %s " % e)


def write_to_mySQL(ga_params):
    try:
        cursor = db.cursor()
        sql = "INSERT INTO  `BISetup`.`<GA_ACCOUNT>` (USERNAME, DATE_DT, DOMAINBRAND, ACCOUNT_NAME, PROPERTY_NAME, PROFILE_NAME, COUNTRY, PAGEPATH, SESSIONS, USERS, NEW_USERS, PAGEVIEWS, BOUNCERATE, AVG_SESSION_DURATION, PERCENT_NEW_SESSIONS, TIMEZONE) \
                                    VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %d, %d, %d, %d, %f, %f, %f, '%s');" \
              % (ga_params["username"], ga_params["start_date"], ga_params['hostname'], ga_params["account_name"], ga_params["property_name"], ga_params["profile_name"],\
                 ga_params["country"],ga_params["pagePath"], ga_params["sessions"], ga_params["users"], ga_params["new_users"], ga_params["pageviews"],\
                 ga_params["bounceRate"], ga_params["avgSessionDuration"], ga_params["percentNewSessions"], ga_params["timezone"])
        cursor.execute(sql)
        db.commit()
    except Exception as e:
        logger.error("Error while inserting data into mySQL table: %s: " % e)
        logger.info("SQL: %s" % sql)
        create_error_file()
        db.rollback()
        db.close()


if __name__ == '__main__':

    logger.info("Starting to load GoogleAnalytics data %s" % start_dt)

    ga_params = {}
    ga_params["username"] = username

    try:
        os.remove(error_file)
    except:
        pass

    while start_dt != today:
        service = initialize_service()
        accountIds = get_accounts_ids(service)
        logger.info("Start date: %s: " % str(start_dt))

        #accountIds = ['31375850']

        for LoopAccountId in accountIds:
            start_date = str(start_dt)
            timezone=[]
            ga_params["start_date"] = start_date
            end_date = str(end_dt)
            account_name = get_account_name(LoopAccountId)
            ga_params["account_name"] = account_name
            prop_properties = service.management().webproperties().list(accountId=LoopAccountId).execute()

            if prop_properties.get('items'):

                for prop in prop_properties.get('items'):
                    property_name = prop.get('name')
                    ga_params["property_name"] = property_name
                    property_id = prop.get('id')
                    profiles = service.management().profiles().list(accountId=LoopAccountId, webPropertyId=property_id).execute()

                    if profiles.get('items'):
                        profile_name = profiles.get('items')[0].get('name')
                        ga_params["profile_name"]=profile_name
                        profile_id = profiles.get('items')[0].get('id')
                        #profile_id = '59352260'
                        timezone = profiles.get('items')[0].get('timezone')
                        data = get_source_group2(service, profile_id, start_date, end_date)
                        time.sleep(1.5)

                        if "rows" in data:

                            for item in data["rows"]:
                                try:
                                    #item = [u'm.pl.cell.com', u'United Kingdom', u'/index.cfm', u'1', u'1', u'1', u'3', u'0.0', u'1150.0', u'100.0']
                                    country = item[1]
                                    ga_params["hostname"] = item[0]

                                    if country == "(not set)":
                                        ga_params["country"] = "N/A"
                                    else:
                                        ga_params["country"] = item[1].encode("utf-8")

                                    ga_params["timezone"] = timezone
                                    ga_params["pagePath"] = item[2].encode("utf-8").replace("\\", "\\\\")
                                    ga_params["sessions"] = int(item[3].encode("utf-8"))
                                    ga_params["users"] = int(item[4].encode("utf-8"))
                                    ga_params["new_users"] = int(item[5].encode("utf-8"))
                                    ga_params["pageviews"] = int(item[6].encode("utf-8"))
                                    ga_params["bounceRate"] = float(item[7].encode("utf-8"))
                                    ga_params["avgSessionDuration"] = float(item[8].encode("utf-8"))
                                    ga_params["percentNewSessions"] = float(item[9].encode("utf-8"))
                                    write_to_mySQL(ga_params)
                                except Exception as e:
                                    logger.info("Error in for : %s " % e)
                                    pass

        start_dt = start_dt + timedelta(days=1)
        end_dt = start_dt


