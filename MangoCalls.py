import requests
import urllib
from datetime import datetime
from datetime import timedelta
import time
import string
import json
from hashlib import sha256
import pymssql
import pandas
from pandas.compat import StringIO
import r_conf_file
import re
import os
import sys
import get_arg

#global variables
r_conf_file.r_conf_file(get_arg.get_arg(sys.argv[1:]))   #Read params from file
df = pandas.DataFrame()

def get_connection():
    global con
    con = pymssql.connect(server = r_conf_file.mssql_srv_adr
                          ,user = r_conf_file.mssql_srv_login
                          ,password = r_conf_file.mssql_srv_pwd
                          ,database = 'fxcDW'
                          ,timeout = 3600) 
    global cur
    cur = con.cursor()
    
def get_config_db(ParamName):
    sql = "select ParamValue from dbo.Config where ParamName = %s"
    cur.execute(sql, ParamName)
    ParamValue = cur.fetchall()[0][0]

    return ParamValue
    
def get_variables(): 
    global params
    params = {}
    params["url"]       = get_config_db("MangoApiURL")
    params["ApiKey"]    = get_config_db("MangoApiKey")
    params["ApiSalt"]   = get_config_db("MangoApiSalt")
    params["headers"]   = {"Content-Type":"application/x-www-form-urlencoded"}
    cur.execute("SELECT CAST(COALESCE(MAX(OperDayDate), '20180907') as Datetime) - 1 FROM MangoCallsRaw")
    params["date_from"] = cur.fetchall()[0][0].date()
    params["date_to"]   = datetime.now().date()
    params["current_dir"]  = os.path.dirname(__file__)
    params["tmp_file"]  = params["current_dir"] + '\mango_tmp.csv'
    
    
def request_calls(date_from, date_to):
    json_query = '{  \
                "date_from": ' +  str(int(time.mktime(date_from.timetuple())))  + ',  \
                "date_to\": ' + str(int(time.mktime(date_to.timetuple()))) +',    \
                "fields": \"entry_id, start, finish, answer, from_extension, from_number, to_extension, to_number, line_number, disconnect_reason, location, records\"  \
            }'
    query = json_query.translate(None, string.whitespace)
    sign = sha256(params["ApiKey"] + query + params["ApiSalt"]).hexdigest()
    query = urllib.quote(query.encode('utf-8'))
    body = "vpbx_api_key="+ params["ApiKey"] + "&sign=" + sign + "&json=" + query
    r = requests.post(params["url"]+"request", headers=params["headers"], data=body)
    
    if (not re.match('{".*":".*"}',r.text)):
        raise ValueError('def request_calls. Problem with URL response, its not matched with regular expression')
    return r.text 
    
def request_callback(json_query_callback):
    
    sign = sha256(params["ApiKey"] + json_query_callback + params["ApiSalt"]).hexdigest()
    json_query_callback = urllib.quote(json_query_callback.encode('utf-8'))
    body = "vpbx_api_key=" + params["ApiKey"] +"&sign=" + sign + "&json=" + json_query_callback
    for x in range(10):
        r = requests.post(params["url"]+"result", headers=params["headers"], data=body)
        if r.text == "":
            time.sleep(5)
        else:
            break
    if len(r.text[:1000]) != 1000: 
        raise ValueError('request_callback(). Request text is not correct or empty.You can try to run script later. For period of start_date: '+ str(params["date_from"]) )
        exit(1)
        
    return r.text

def text_to_df(text):
    df = pandas.read_csv(StringIO(text)
                    ,header=0
                    ,delimiter=";"
                    ,names=["entry_id","start","finish","answer","from_extension","from_number","to_extension","to_number","line_number","disconnect_reason","location","records",])

    df['start'] = pandas.to_datetime(df['start'],unit='s')
    df['finish'] = pandas.to_datetime(df['finish'],unit='s')
    df['answer'] = pandas.to_datetime(df['answer'],unit='s')
    df['OperDayDate'] = df['start'].dt.date
    df["entry_id"] = map(lambda x: x.decode('base64','strict'), df['entry_id'])

    return df

def range_dates(date_from,date_to):
    import datetime
    range_list = []
    diff = date_to - date_from

    while date_from < date_to:
        range_list.append([date_from,date_from +  datetime.timedelta(days=10)])
        date_from = date_from +  datetime.timedelta(days=10)
    
    return range_list

def load_to_db():
    cur.execute("DELETE dbo.MangoCallsRaw WHERE OperDayDate >= %d", params["date_from"])
    sql = "BULK INSERT dbo.MangoCallsRaw FROM '"+ params["tmp_file"] + "' WITH (FIELDTERMINATOR=',',ROWTERMINATOR='\\n' );"
    cur.execute(sql)
    con.commit()
    os.remove(params["tmp_file"])
    
    
if __name__ == '__main__':
    reload(sys)  
    sys.setdefaultencoding('utf8') 
    tstart = datetime.now()
    get_connection()
    get_variables()
    for x in range_dates(params["date_from"], params["date_to"]):
        print 'Date interval from  ' + str(x[0]) + '  till  ' + str(x[1])
        json_query_callback = request_calls(x[0], x[1])
        resp = request_callback(json_query_callback)
        df = df.append(text_to_df(resp))
    if df.empty == True:
        print "The response from API retrieved empty dataset. Nothing to load into database"
        raise ValueError('No data to load to DB. Something went wrong. If problem is repiated- problem with API')
    else:
        df[["OperDayDate","entry_id","start","finish","answer","from_extension","from_number","to_extension","to_number","line_number","disconnect_reason","location","records"]].to_csv(params["tmp_file"], index=False,header=False,line_terminator='\r\n')
        load_to_db()
    tend = datetime.now()
    
    print "Loaded rows count:  " + str(df.shape[0])
    print "Dates have been loaded, count of rows :"
    print df.groupby(['OperDayDate']).size()

    print "Start:       "+str(tstart.strftime("%Y-%m-%d %H:%M:%S"))
    print "End:         "+str(tend.strftime("%Y-%m-%d %H:%M:%S"))
    print "Duration:    "+str(tend - tstart)
