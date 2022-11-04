# coding=utf-8
from asyncio.windows_events import NULL
import pyodbc 
import pandas as pd
import logging
import smtplib

def send_email(Count, Status):
    to = ['']
    user = '' #From
    smtpserver = smtplib.SMTP(IP_Add,Port)
    smtpserver.ehlo()
    smtpserver.ehlo
    for i in to:
        header = 'To:' + i + '\n' + 'From: ' + user + '\n' + 'Subject: Alert \n'
        print (header)
        msg = header + '\n '+Count+' Updated  \n\n Status : ' +Status+'\n'
        smtpserver.sendmail(user, i, msg)
    print ('done!')
    smtpserver.close()


Log_Format = "%(levelname)s %(asctime)s - %(message)s"

logging.basicConfig(filename = "logfile.log",
                    filemode = "a",
                    format = Log_Format, 
                    level = logging.DEBUG)

logger = logging.getLogger()

query = """Select distinct(A_party) 
from numb_list
where date >= '{0}' 
and len(A_party) = {1} 
and [A_party] like '{2}%'""".format(date,lngt,prefix)
print(query)

def updateMSISDNs(df_hadoop):
    print("Starting Update")
    logging.info("Starting Update of NumberS")
    df = df_hadoop
    try : 
        for i in range(0,len(df)):
            query3 = """Update Table Set [Column] = '{1}' where [A_party] in (
                                '{0}'
                                ) and [Column] = 'N/A' 
                            """.format("" + df.loc[i, "aparty"][1:],df.loc[i, "tnk"])
            # print(query3)
            cursor = conn.cursor()
            cursor.execute(query3)
            conn.commit()
            print(str(df.loc[i, "tnk"]) + " Trunk added to Number : " + str("92" + df.loc[i, "aparty"][1:]))
            logging.info(str(df.loc[i, "tnk"]) + " Trunk added to Number : " + str("92" + df.loc[i, "aparty"][1:]))
           

        cursor.close()    
        conn.close()
        print("All Trunks addes to MSISDNs")
        logging.info("All Trunks addes to MSISDNs")
    except:
        print(Exception)
        print("Numbers not Updated Occurred !!!!")
        logging.error("Nummbers not Updated Script Closed")
        quit()

try : 
    conn = pyodbc.connect('DRIVER={ODBC Driver 13 for SQL Server};' ### Making Connection
                      'Server=IP;'
                      'Database=DB_NAME;'
                      'PORT=PORT_NUMBER;'
                      'UID=USERNAME;'
                      'PWD=PASSWORD;')

    cursor = conn.cursor()
    cursor.execute(query) 

    list_of_A_Numbers = ["0"+item[0][2:] for item in cursor.fetchall()]
# cursor.close()
    print("Count of A Numbers : " + str(list_of_A_Numbers.__len__()))

    logging.info("Count of A Numbers : " + str(list_of_A_Numbers.__len__()))

    number_list = ''

    for i in list_of_A_Numbers:
        number_list = number_list + "'"+str(i)+"',"
except:
    print(Exception)
    print("SQL Error Occurred !!!!")
    logging.error("SQL Error Occured Script Closed")
    quit()

try:
    conn2 = pyodbc.connect("DSN=CONN", autocommit=True) 
    query2 = "Select trunk,aparty from TABLE where APARTY in ({0})".format(number_list[:-1])

    df = pd.read_sql(query2, conn2)
    
    print(str(len(df))+ " Count of DataFrame")
    logging.info(str(len(df))+ " Count of DataFrame")

    print("Hadoop task completed!!")
    logging.info("Hadoop task completed!!")

    if(len(df) == 0):
        print("Empty DataFrame")
        logging.error("Empty DataFrame")
        quit()
    conn2.close()
    updateMSISDNs(df)
except Exception as ex:
        print(ex)


