import re
import datetime
import time
import os
import sys 
import getopt

def r_conf_file(fname):
    """Read config from file"""
    #Is it exist file?
    if os.path.isfile(fname) == False:
        print "ERROR: File " +  fname + " not found!"
        exit(1)
    # IP or DNS name MS SQL Server    
    global mssql_srv_adr
    mssql_srv_adr = ''
    # Login MS SQL Server    
    global mssql_srv_login
    mssql_srv_login = ''
    # Password MS SQL Server    
    global mssql_srv_pwd
    mssql_srv_pwd = ''

    
    #Read params from file
    fo = open(fname, "r")
    try:
        now = datetime.datetime.now()
        for line in fo:
            if 'MSSQL_SRV' in line.upper():    
                if line.find('=>') > 0:
                    mssql_srv_adr = line[line.find('=>')+2:].strip()                   
            elif 'MSSQL_LOGIN' in line.upper():    
                if line.find('=>') > 0:
                    mssql_srv_login = line[line.find('=>')+2:].strip()
            elif 'MSSQL_PWD' in line.upper():    
                if line.find('=>') > 0:
                    mssql_srv_pwd = line[line.find('=>')+2:].strip()                 
    except Exception as e:
        print "ERROR: Exception %s" %(e)                   
    fo.close()
    
    #Verify params  
    is_exit = 0
    is_ip = re.match("^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\.){3}([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$", mssql_srv_adr)
    is_hostname = re.match("^(([a-zA-Z]|[a-zA-Z][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)*([A-Za-z]|[A-Za-z][A-Za-z0-9\-]*[A-Za-z0-9])$", mssql_srv_adr)
    if (not is_ip and not is_hostname) or mssql_srv_adr == '':
        msg = '<Server ip address or DNS name> is not correct   ' + str(now.strftime("%Y-%m-%d %H:%M:%S"))
        print msg
        create_msg_to_es(msg,"ERR")
        is_exit = 1
    if mssql_srv_login == '':
        msg = '<Login> is not correct   ' + str(tstart.strftime("%Y-%m-%d %H:%M:%S"))
        print msg
        create_msg_to_es(msg,"ERR")
        is_exit = 1
    if mssql_srv_pwd == '':
        msg = '<Password> is not correct   ' + str(tstart.strftime("%Y-%m-%d %H:%M:%S"))
        print msg
        create_msg_to_es(msg,"ERR")
        is_exit = 1
    if is_exit == 1:
        if elast_log_link <> '':    
            write_to_es(elast_log_link, elast_log_msg)
        exit(1)
