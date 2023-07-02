import requests
import json
import sys
import time
import re
import datetime
import mysql.connector
import pandas as pd
import socket
import sqlalchemy
from threading import Thread,Lock
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
threadLock=Lock()
def set_values(df,output_table_name,myd):
    threadLock.acquire()
    try:
        df.to_sql(con=myd,name=output_table_name,if_exists='append', index= False)
         
    except Exception as e:
                print(e)
    finally:
        threadLock.release()
     
def get_data(mydb):
    try:
        cursor = mydb.cursor()
        sql_select_query = "SELECT checkin,checkout,rate_shop_id,SHOPID,chain_id,ht_id FROM {}  where proc = {} order by MyOrder".format(input_table_name,proc)
        # set variable in query order by MyOrder
        cursor.execute(sql_select_query)
        # fetch result
        record = cursor.fetchall()
        return record
        

    except mysql.connector.Error as error:
        print("Failed to get record from MySQL table: {}".format(error))

    finally:
        if mydb.is_connected():
            cursor.close()

def split_n(a, n):
    k, m = divmod(len(a), n)
    return list((a[i*k+min(i, m):(i+1)*k+min(i+1, m)] for i in range(n)))

def fetch_cookies(chain_id,proxyMask):

    proxy_https={"https":"https://{}".format("premium.residential.proxyrack.net:9000")}
    headers = {
        'authority': 'live.ipms247.com',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,/;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'accept-language': 'en-US,en;q=0.9,en-GB;q=0.8',
        'cache-control': 'max-age=0',
        # 'cookie': 'res_Logo_Mobile_9642=https://d1vsci4s9o4dj5.cloudfront.net/9642_20171127003126_0104704001511742686_460_Phone.png; res_Logo_Tab_9642=https://d1vsci4s9o4dj5.cloudfront.net/9642_20171127003244_0633572001511742764_417_Tablet.png; res_Logo_9642=https://d1vsci4s9o4dj5.cloudfront.net/9642_20171127003120_0808683001511742680_146_Banner.png; res_quick_9642=false; ext_name=ojplmecpdpgccookcobabopnaifgidhf; __utmz=202751756.1677243869.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); _ga=GA1.1.1324207466.1672770596; _hjSessionUser_3346604=eyJpZCI6ImU4ODIwN2EyLWQ5N2ItNWZmZC1hYTFmLTZkYzc5YmQ2ZmYwOCIsImNyZWF0ZWQiOjE2Nzg5NDk0NTQwOTUsImV4aXN0aW5nIjp0cnVlfQ==; __utma=202751756.1324207466.1672770596.1679528752.1679560990.6; sucuri_cloudproxy_uuid_1414035b1=c9fe274394e4be3a6e0c9aba967ffcf3; __atuvc=0%7C9%2C0%7C10%2C11%7C11%2C66%7C12%2C16%7C13; _ga_YX3EF5GYX9=GS1.1.1680206370.31.1.1680206538.0.0.0; AWSALB=UMd0AfJrT35dyyeEEi6S48FrkkDfQRK2OQzbMAb8sCd+VFIFnUsvdBE0MR1Vcny2raiirp10BtSTM/z2H35HkoBiuFq5N+HUVID/OcoGWIg+5MV4ff4QqG0XWkwyplX2uqkzJ/YlwQDuqWFYjpcucKI9mSENsImPZNuctnt7mIAsl9nv3HWf499iA1Jf2A==; AWSALBCORS=UMd0AfJrT35dyyeEEi6S48FrkkDfQRK2OQzbMAb8sCd+VFIFnUsvdBE0MR1Vcny2raiirp10BtSTM/z2H35HkoBiuFq5N+HUVID/OcoGWIg+5MV4ff4QqG0XWkwyplX2uqkzJ/YlwQDuqWFYjpcucKI9mSENsImPZNuctnt7mIAsl9nv3HWf499iA1Jf2A==',
        'referer': 'https://live.ipms247.com/booking/book-rooms-santapaulainn',
        'sec-ch-ua': '"Google Chrome";v="111", "Not(A:Brand";v="8", "Chromium";v="111"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
    }
    
    url="https://live.ipms247.com/booking/"+chain_id
    proxy_https={"https":"https://{}".format(proxyMask)}
    retry = Retry(connect=5, backoff_factor=0.5,status_forcelist=[ 500, 502, 503, 504 ])
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    #proxy_https={"https":"https://{}".format("premium.residential.proxyrack.net:9000")}
    
    rep = requests.post(url,headers=headers,proxies=proxy_https,verify=False)

    tc= rep.cookies.get_dict()
    print(rep.status_code)
    print(url)
    return tc

def GetHTML(proxyMask, id,check_in,check_out,shop_id,chain_id,output_table_name,errorFull,myd,ht_id):
    #print("Called")
    df=pd.DataFrame({
    "source":[],"type":[],"hotelid":[],"checkin":[],"checkout":[],"Currency":[],"PerNight":[],"Taxes":[],"Fees":[],"StayTotalwTaxes":[],"ratecode":[],"ratename":[],
    "roomcode":[],"roomname":[],"cancelpolicy":[],"paymentpolicy":[],"package":[],"rooms":[],"guests":[],"availability":[],"idrequired":[],"shopid":[],"hc_insert":[]
})
    try:
        for i in range(len(id)):
            headers = {
                'authority': 'live.ipms247.com',
                'accept': 'text/html, /; q=0.01',
                'accept-language': 'en-US,en;q=0.9,en-GB;q=0.8',
                'content-type': 'application/x-www-form-urlencoded; charset=UTF-8',
                # 'cookie': 'res_Logo_Mobile_9642=https://d1vsci4s9o4dj5.cloudfront.net/9642_20171127003126_0104704001511742686_460_Phone.png; res_Logo_Tab_9642=https://d1vsci4s9o4dj5.cloudfront.net/9642_20171127003244_0633572001511742764_417_Tablet.png; res_Logo_9642=https://d1vsci4s9o4dj5.cloudfront.net/9642_20171127003120_0808683001511742680_146_Banner.png; res_quick_9642=false; ext_name=ojplmecpdpgccookcobabopnaifgidhf; __utmz=202751756.1677243869.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none); _ga=GA1.1.1324207466.1672770596; _hjSessionUser_3346604=eyJpZCI6ImU4ODIwN2EyLWQ5N2ItNWZmZC1hYTFmLTZkYzc5YmQ2ZmYwOCIsImNyZWF0ZWQiOjE2Nzg5NDk0NTQwOTUsImV4aXN0aW5nIjp0cnVlfQ==; __utma=202751756.1324207466.1672770596.1677246259.1678951679.3; sucuri_cloudproxy_uuid_60ea6abb6=411b19d2ea68370f007c1130d5a54eea; PHPSESSID=gir3opbck0unm0ll6qtl22nda7; _hjSession_3346604=eyJpZCI6IjNhNTVhMDI0LWY5ZGUtNDUzOC04MTczLWFmNmY5ZjNlNWIxNCIsImNyZWF0ZWQiOjE2Nzk0OTAxNjExNDAsImluU2FtcGxlIjpmYWxzZX0=; _hjAbsoluteSessionInProgress=1; _hjIncludedInSessionSample_3346604=0; _ga_YX3EF5GYX9=GS1.1.1679489035.3.1.1679491178.0.0.0; __atuvc=22%7C8%2C0%7C9%2C0%7C10%2C11%7C11%2C6%7C12; __atuvs=641afc701a6aeea4003; AWSALB=cjXX6Z1EYOxH8ssJmrzsMkBRyJg4WMDqqL0gqDJ7G3svSx57sXP20iBxb3bQ1zrBJf2TmQKHWDCvwcHaAjqEdYl6xFvMjsU8EN793pQ73SJOZ2nHq9M2XOdhA59xPFAhcD7Fno1/l7gxjNpb4pKcp/SUniS0CxgFRWZh86XawoapM4Vt9rZY1XQYBUywkQ==; AWSALBCORS=cjXX6Z1EYOxH8ssJmrzsMkBRyJg4WMDqqL0gqDJ7G3svSx57sXP20iBxb3bQ1zrBJf2TmQKHWDCvwcHaAjqEdYl6xFvMjsU8EN793pQ73SJOZ2nHq9M2XOdhA59xPFAhcD7Fno1/l7gxjNpb4pKcp/SUniS0CxgFRWZh86XawoapM4Vt9rZY1XQYBUywkQ==',
                'origin': 'https://live.ipms247.com',
                'referer': 'https://live.ipms247.com/booking/book-rooms-santapaulainn',
                'sec-ch-ua': '"Google Chrome";v="111", "Not(A:Brand";v="8", "Chromium";v="111"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
                'sec-fetch-dest': 'empty',
                'sec-fetch-mode': 'cors',
                'sec-fetch-site': 'same-origin',
                'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36',
                'x-requested-with': 'XMLHttpRequest',
            }
            cookies = {
                'res_Logo_Mobile_9642': 'https://d1vsci4s9o4dj5.cloudfront.net/9642_20171127003126_0104704001511742686_460_Phone.png',
                'res_Logo_Tab_9642': 'https://d1vsci4s9o4dj5.cloudfront.net/9642_20171127003244_0633572001511742764_417_Tablet.png',
                'res_Logo_9642': 'https://d1vsci4s9o4dj5.cloudfront.net/9642_20171127003120_0808683001511742680_146_Banner.png',
                'res_quick_9642': 'false',
                'ext_name': 'ojplmecpdpgccookcobabopnaifgidhf',
                '__utmz': '202751756.1677243869.1.1.utmcsr=(direct)|utmccn=(direct)|utmcmd=(none)',
                '_ga': 'GA1.1.1324207466.1672770596',
                '_hjSessionUser_3346604': 'eyJpZCI6ImU4ODIwN2EyLWQ5N2ItNWZmZC1hYTFmLTZkYzc5YmQ2ZmYwOCIsImNyZWF0ZWQiOjE2Nzg5NDk0NTQwOTUsImV4aXN0aW5nIjp0cnVlfQ==',
                '__utma': '202751756.1324207466.1672770596.1679528752.1679560990.6',
                'sucuri_cloudproxy_uuid_1b70b58e6': '3551af563d07a4ec292c2c5974d7bce0',
                'PHPSESSID': '2ufmrk2s3u74qk8cm773n2p971',
                '_hjIncludedInSessionSample_3346604': '0',
                '_hjSession_3346604': 'eyJpZCI6IjZjMjdjZDQ1LTFkMDQtNGQ1Yy1hMTk3LWZkMGFkZTFkNDEzOSIsImNyZWF0ZWQiOjE2Nzk4NjM4MDMzOTQsImluU2FtcGxlIjpmYWxzZX0=',
                '_hjAbsoluteSessionInProgress': '1',
                '_ga_YX3EF5GYX9': 'GS1.1.1679863803.23.1.1679863832.0.0.0',
                'AWSALB': '5Z+n0CJfMp091RDjmKBV1LwxFNAxHAhhqeTqaetMik8+4dFAm1eknN9bYd1OLibGGYS8Buwpmcb7KB4O/xMewjVfpDHk2J1+k7FYH0zBtB2F6SSv1PNaoen8NrsFyyVuBvLQmIguwtLCXU55xWzF64igMj0cKrzx6h3a0taxQjHyop2VDNDoKXg9J7UjVQ==',
                'AWSALBCORS': '5Z+n0CJfMp091RDjmKBV1LwxFNAxHAhhqeTqaetMik8+4dFAm1eknN9bYd1OLibGGYS8Buwpmcb7KB4O/xMewjVfpDHk2J1+k7FYH0zBtB2F6SSv1PNaoen8NrsFyyVuBvLQmIguwtLCXU55xWzF64igMj0cKrzx6h3a0taxQjHyop2VDNDoKXg9J7UjVQ==',
                '__atuvc': '0%7C9%2C0%7C10%2C11%7C11%2C66%7C12%2C4%7C13',
                '__atuvs': '6420aff9f11bfd82002',
            }
            time_duration=check_out[i]-check_in[i]
            data = {
                'checkin': check_in[i].strftime("%m-%d-%y"),
                'gridcolumn': '1',
                'adults': '1',
                'child': '0',
                'nonights': str(int(time_duration.total_seconds()/86400)),
                'ShowSelectedNights': 'true',
                'DefaultSelectedNights': '1',
                'calendarDateFormat': 'mm-dd-yy',
                'rooms': '1',
                'ArrvalDt':check_in[i].strftime("%y-%m-%d") ,
                'HotelId': id[i],
                'isLogin': 'lf',
                'selectedLang': '',
                'modifysearch': 'false',
                'layoutView': '2',
                'ShowMinNightsMatchedRatePlan': 'false',
                'LayoutTheme': '2',
                'w_showadult': 'false',
                'w_showchild_bb': 'false',
                'ShowMoreLessOpt': '',
                'w_showchild': 'true',
                
            }
            proceed=True
            try:
                cookieContainer=fetch_cookies(chain_id[i],proxyMask)
                cookies.update(cookieContainer)
            #print(cookies)
            except IOError as e:
                print("IO Error")
                proceed=False
            except Exception as e:
                print("e")
                proceed=False
                

            #proxy = {"http":"http://{}".format("premium.residential.proxyrack.net:9000")} 
            #proxy_https={"https":"https://{}".format("unmetered.residential.proxyrack.net:222")}
            if proceed:
                proxy_https={"https":"https://{}".format(proxyMask)}
                retry = Retry(connect=3, backoff_factor=0.5)
                adapter = HTTPAdapter(max_retries=retry)
                session = requests.Session()
                session.mount('http://', adapter)
                session.mount('https://', adapter)
                #requests.Session.mount(r'https://', adapter)
                lastCheck=True
                #proxies = {"http": proxy, "https": proxy_https} 
                try:
                    response = session.post('https://live.ipms247.com/booking/rmdetails', cookies=cookies, headers=headers, data=data,proxies=proxy_https,timeout=5,verify=False)
                except IOError as e:
                    print("IO Error from server")
                    lastCheck=False
                    brtag=chain_id[i]
                    source="1"
                    shopId=shop_id[i]
                    errorMsg="IO Error from server"
                    type="NA"
                    retries=3
                    hcIn=datetime.datetime.now()
                    mycursor = Errormydb.cursor()
                    sql="INSERT IGNORE INTO {} (brtag,source,shopid,errormsg,type,retries,hc_insert) VALUE(%s,%s,%s,%s,%s,%s,%s)".format(errorFull)
                    val=(brtag,source,shopId,errorMsg,type,retries,hcIn)
                    mycursor.execute(sql,val)
                    finalcheck=False
                except socket.timeout:
                    print("Socket Timeout")
                    lastCheck=False
                except socket.error:
                    print("socket error")
                    lastCheck=False
                except Exception as e:
                    print("e")
                    lastCheck=False
                
                if "Please check your URL" in response.text:
                    lastCheck=False

                if lastCheck: 
                    print(response.status_code)
                    soup = response.text
                
                    #print(soup)

                    ma = re.findall(r'resgrid=([\s\S]*?)}]]',str(soup))

                    # script = str(ma)[4:-2]
                    finalcheck=True
                    script = str(ma).replace("['[","")
                    script = script.replace(r'\\"','')
                    script = script.replace("']","}]")
                    try:
                        data=json.loads(script.encode('unicode_escape'))
        
                    except Exception as e:
                        print("JSON error from server")
                        brtag=chain_id[i]
                        source="1"
                        shopId=shop_id[i]
                        errorMsg="JSON error from server"
                        type="NA"
                        retries=3
                        hcIn=datetime.datetime.now()
                        mycursor = Errormydb.cursor()
                        sql="INSERT IGNORE INTO {} (brtag,source,shopid,errormsg,type,retries,hc_insert) VALUE(%s,%s,%s,%s,%s,%s,%s)".format(errorFull)
                        val=(brtag,source,shopId,errorMsg,type,retries,hcIn)
                        mycursor.execute(sql,val)
                        print(chain_id[i])
                        # print(script)
                        # time.sleep(1000)
                        finalcheck=False
                    if finalcheck:
                        currency=[]
                       
                        stayTotal=[]
                        rateCode=[]
                        rateName=[]
                        roomCode=[]
                        roomName=[]
                        rooms=[]
                        guests=[]
                        
                        currency = [data[v].get('curr_code') for v in range(len(data))]
                        stayTotal = [data[v].get('MinAvgPerNightDiscount') for v in range(len(data)) ]
                        rateCode = [data[v].get('ratetypeunkid') for v in range(len(data))]
                        rateName = [data[v].get('display_name') for v in range(len(data)) ]
                        roomCode = [data[v].get('roomrateunkid') for v in range(len(data))]
                        roomName = [data[v].get('roomtype') for v in range(len(data)) ]
                        rooms = [data[v].get('requestedrooms') for v in range(len(data))]
                        guests = [data[v].get('totalguest') for v in range(len(data)) ]
                            #soup = response.text  

                        val= [
                                (
                                "1",
                                typeVar,
                                ht_id[i],
                                check_in[i].strftime('%Y-%m-%d'),
                                check_out[i].strftime('%Y-%m-%d'),
                                currency[a],
                                float(stayTotal[a])*int(time_duration.total_seconds()/86400),
                                0,
                                0,
                                float(stayTotal[a])*int(time_duration.total_seconds()/86400),
                                rateCode[a],
                                rateName[a],
                                roomCode[a],
                                roomName[a],
                                "",
                                "",
                                "",
                                int(rooms[a]),
                                int(guests[a]),
                                "",
                                "",
                                shop_id[i],
                                datetime.datetime.now()
                                ) for a in range(0,len(rooms))]
                        j=[]
                        j.append(val)
                        resultList = [element for nestedlist in j for element in nestedlist]


                        
                        for product in resultList:
                            df.loc[len(df)]=product
                       
                       # if len(df.index)>=0:
                        while True:
                            if threadLock.locked():
                                time.sleep(0.5)
                                print("locked")
                            else:
                                set_values(df,output_table_name,myd)
                                df.drop(df.index, inplace=True)
                                #driver.refresh()
                                #print("Successfull entries ",x)
                                break
                        # else:
                        #      continue
    finally:
         if len(df.index)>0:
              df=set_values(df,output_table_name)
                                                 
                    
        
if __name__ == "_main_":
        args = sys.argv[1:]

        print(args)

        number_of_threads = int(args[0])

        databse_url = args[1]

        database_user = args[2]

        database_password = args[3]

        input_database = args[4]

        #global input_table_name
        input_table_name = args[5]

        output_database = args[6]

        output_table_name = args[7]

        #global proc
        proc = args[8]

        error_database = args[9]

        error_table_name = args[10]

        proxy_provider = args[11]

        global typeVar
        typeVar = args[12]
        
        # number_of_threads = 2

        # #global databse_url
        # databse_url = "localhost"

        # #global database_user
        # database_user = "root"

        # #global database_password
        # database_password = ""

        # input_database = "scrape_inputs"

        # #global input_table_name
        # input_table_name = "ipms_dayl"

        # output_database = "scrape_data"

        # output_table_name = "ipms_rates_std_test"

        # #global proc
        # proc = "0"

        # error_database = "scrape_data"

        # error_table_name = "ipms_scrapeerror_master"

        # proxy_provider = "unmetered.residential.proxyrack.net:222"

        # global typeVar
        # typeVar = "mppp"
        
        mydb = mysql.connector.connect(     
                                            host=databse_url,
                                            database=input_database,
                                            user=database_user,
                                            password=database_password
                                                    # host='localhost',
                                                    # database='scrape_inputs',
                                                    # user='root',
                                                    # password=''
                                                    )
        #Taking input data and splitting it according to the number of threads
        start_time = time.time()
        check_in=[]
        check_out=[]

        #global df


        rate_shop_id=[]
        shop_id=[]
        chain_id=[]
        ht_id=[]
        input_data=get_data(mydb)
        mydb.close()
        for i in input_data:
            check_in.append(i[0])
            check_out.append(i[1])
            rate_shop_id.append(i[2])
            shop_id.append(i[3])
            chain_id.append(i[4])
            ht_id.append(i[5])
      

        bifChkin=split_n(check_in,number_of_threads)
        bifChkout=split_n(check_out,number_of_threads)
        bifrateSid=split_n(rate_shop_id,number_of_threads)
        bifShopid=split_n(shop_id,number_of_threads)
        bifChainid=split_n(chain_id,number_of_threads)
        bifHtid=split_n(ht_id,number_of_threads)
        errorFull=error_database + "." + error_table_name
        threads = []
        Errormydb = mysql.connector.connect(     
                                            host=databse_url,
                                            database=error_database,
                                            user=database_user,
                                            password=database_password
                                                    # host='localhost',
                                                    # database='scrape_inputs',
                                                    # user='root',
                                                    # password=''
                                                    )
     
        mydb = sqlalchemy.create_engine('mysql+mysqlconnector://{}:{}@{}/{}'.format(database_user,database_password,databse_url,output_database))
        # session_factory = sessionmaker(bind=engine)
        # Session = scoped_session(session_factory)
        #Program starts requests
        for n in range(number_of_threads):
            t = Thread(target=GetHTML, args=(proxy_provider,bifrateSid[n],bifChkin[n],bifChkout[n],bifShopid[n],bifChainid[n],output_table_name,error_table_name,mydb,bifHtid[n])) # get number for place in list `buttons`
            t.start()
            threads.append(t)

        for t in threads:
            t.join()
        
        end_time = time.time()
        print("\nTotal time taken for program execution in seconds = ",end_time-start_time)