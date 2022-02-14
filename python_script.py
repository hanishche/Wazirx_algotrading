
import requests 
import pandas as pd
import schedule
import time
from pytz import timezone
from datetime import datetime, timedelta
import matplotlib.pyplot as plt
import os
from matplotlib.backends.backend_pdf import PdfFile,PdfPages
import seaborn as sns
import pywhatkit as pywt
import threading
import keyboard
import os


from indicators import rsi_tradingview,MACD






def email(Buyorsell,body: str=None):
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.mime.base import MIMEBase
    from email import encoders
    import getpass
    import glob

    body = body
    # put your email here
    sender = email_id*
    password = pwd*
    # put the email of the receiver here
    receiver = email_id_reciever*

    #Setup the MIME
    message = MIMEMultipart()
    message['From'] = sender
    message['To'] = receiver
    message['Subject'] = Buyorsell
    

#     message.attach(MIMEText(body, 'plain'))
    message.attach(MIMEText(body, 'html', 'utf-8'))
    
    session = smtplib.SMTP('smtp.gmail.com', 587)

    #enable security
    session.starttls()

    #login with mail_id and password
    session.login(sender, password)

    text = message.as_string()
    session.sendmail(sender, receiver, text)
    session.quit()
    print('Mail Sent')


# If you need historical data


def EMA_live(crypto,api_key):
    try:
        url = 'https://www.alphavantage.co/query?function=CRYPTO_INTRADAY&symbol={}&market=INR&interval=1min&outputsize=full&apikey=api_key'.format(crypto)
        r = requests.get(url)
        data = r.json()
        data1=pd.DataFrame(data['Time Series Crypto (1min)']).T.reset_index()
        data1['index']=pd.to_datetime(data1['index']).apply(lambda x: (x+timedelta(minutes = 330)))
        data1=data1.sort_values(by='index',ascending=False)
        data1.columns=['date_time','open','high','low','close','volume']
        data1['EMA_13']=data1['close'].ewm(span=13, adjust=False).mean()
        data1['EMA_10']=data1['close'].ewm(span=10, adjust=False).mean()
        data1['diff']=data1['EMA_10'].astype(float)-data1['EMA_13'].astype(float)
        data=data1
    except Exception as e:
        email(Buyorsell=e)
        url ='https://min-api.cryptocompare.com/data/v2/histominute?fsym={}&tsym=INR'.format(crypto) #&aggregate=1
        response = requests.get(url)
        data = response.json()
        data=pd.DataFrame(data['Data']['Data'])
        data['date_time']=pd.to_datetime(data['time'], unit='s').apply(lambda x: (x+timedelta(minutes = 330)))
        data=data.sort_values(by='date_time',ascending=False).reset_index().drop(['index','time','conversionType','conversionSymbol'],axis=1)
        data['EMA_13']=data['close'].ewm(span=13, adjust=False).mean()
        data['EMA_10']=data['close'].ewm(span=10, adjust=False).mean()
        data['diff']=data['EMA_10'].astype(float)-data['EMA_13'].astype(float)
        data=data.drop(0,axis=0).reset_index().drop('index',axis=1)
        data

    data_10_13=data
    
    return data_10_13


# In[410]:


def dump_own(crypto,rows):
    time.sleep(1)
    
    try:
        from datetime import datetime,timedelta
        response=client.send("trades",{"limit": 1000, "symbol": {crypto}, "recvWindow": 10000, "timestamp": int(time.time() * 1000),"Signature":signature})
        data=(pd.DataFrame(response[1]))
        data['date_time']=pd.to_datetime(data['time'], unit='ms').apply(lambda x: (x+timedelta(minutes = 330)))

        data['date']=data['date_time'].apply(lambda x: x.strftime("%Y-%m-%d %H:%M"))
        main_data=pd.pivot_table(data, values=['price'], index=['date'],aggfunc={'price': [min, max]})['price'].reset_index()

        main_data.columns=['date_time','high','low']

        df=pd.pivot_table(data, values=['price','date_time'], index=['date'],aggfunc={'date_time': [min, max]})['date_time'].reset_index()

        df1=pd.merge(df,data[['date_time','price']],left_on='min',right_on='date_time',how='left').merge(data[['date_time','price']],left_on='max',right_on='date_time',how='left')
        df1=df1.drop(['max','min','date_time_x','date_time_y'],axis=1)
        df1.columns=['date_time','open','close']
        df1=df1[~df1.duplicated('date_time', keep='first')]
        df1=df1.reset_index().drop('index',axis=1)

        final_data=pd.concat([main_data,df1.drop('date_time',axis=1)],axis=1)
        final_data
        idx=pd.date_range(final_data['date_time'].min(), final_data['date_time'].max(), freq = "60s")
        final1=final_data.set_index(['date_time'])
        final1.index=pd.DatetimeIndex(final1.index)
        final1=final1.reindex(idx,method='ffill')
        final1=final1.reset_index()
        final1.columns=['date_time','high','low','open','close']
        final1['mins']=1
        final1=final1.sort_values(by='date_time',ascending=False).reset_index().drop('index',axis=1)
        final1['close']=final1['close'].astype(float)
        data_tags=final1.iloc[:rows,:]
        data_tags['EMA_13']=data_tags['close'].ewm(span=13, adjust=False).mean()
        data_tags['EMA_10']=data_tags['close'].ewm(span=10, adjust=False).mean()
        data_tags['diff']=data_tags['EMA_10'].astype(float)-data_tags['EMA_13'].astype(float)
        data_tags['manual_RSI']=rsi_tradingview(data_tags,6,round_rsi=True)
        data_tags['macd_line']=MACD(data_tags,12,26,9)[0]
        data_tags['macd_signal_line']=MACD(data_tags,12,26,9)[1]
        data_tags['macd_cross']=MACD(data_tags,12,26,9)[2]
        data_tags=data_tags.reset_index().drop('index',axis=1)
        data_tags=data_tags.reset_index().drop('index',axis=1)
        data_tags['tags_RSI']=np.where(data_tags['manual_RSI']>70,'SELL',np.where(data_tags['manual_RSI']<20,'BUY','Neutral'))
        list_ema=[]
        for i in range(0,len(data_tags)):
                if (data_tags['diff'][i]<0):
                    list_ema.append('BUY')
                elif (data_tags['diff'][i]>0):
                    list_ema.append('SELL')
                else:
                    list_ema.append('Neutral')

        list_MACD=[]
        for i in range(0,len(data_tags)):

                if (data_tags['macd_cross'][i]<0):
                    list_MACD.append('BUY')
                elif (data_tags['macd_cross'][i]>0):
                    list_MACD.append('SELL')
                else:
                    list_MACD.append('Neutral')

        data_tags['tags_ema']=list_ema
        data_tags['tags_macd']=list_MACD

        if pd.qcut(data_tags['close'], int(round((data_tags['close'].max()-data_tags['close'].min())/2,0)),duplicates='drop').isna().all():
            data_tags['buckets']=pd.qcut(data_tags['close'],3)
        else:
            data_tags['buckets']=pd.qcut(data_tags['close'], int(round((data_tags['close'].max()-data_tags['close'].min())/2,0)),duplicates='drop')
        #     data_tags['buckets']=pd.qcut(data_tags['close'],3)
        pivot_ema=pd.DataFrame(data_tags.groupby(['buckets','tags_ema'])['tags_ema'].count()).rename(columns={'tags_ema': 'tags_cnt'}).reset_index()
        ema=pd.pivot_table(pivot_ema, values='tags_cnt', index=['buckets'],columns='tags_ema', aggfunc=max, fill_value=0).reset_index()

        pivot_rsi=pd.DataFrame(data_tags.groupby(['buckets','tags_RSI'])['tags_RSI'].count()).rename(columns={'tags_RSI': 'tags_cnt'}).reset_index()
        rsi=pd.pivot_table(pivot_rsi, values='tags_cnt', index=['buckets'],columns='tags_RSI', aggfunc=max, fill_value=0).reset_index()

        bckt1=pd.merge(ema,rsi,on='buckets',how='inner')
        bckt1.columns=[col.replace('_x','') if '_x' in col else col.replace('_y','') if '_y' in col else col for col in bckt1.columns]
        bckt1.columns=[col.replace('_z','') if '_z' in col else col for col in bckt1.columns]
        cols=bckt1.columns.to_list()
        bckt1['BUY']= bckt1['BUY'].sum(axis=1) if cols.count('BUY')>1 else 0 if cols.count('BUY')==0 else bckt1['BUY']
        bckt1['SELL']= bckt1['SELL'].sum(axis=1) if cols.count('SELL')>1 else 0 if cols.count('SELL')==0 else bckt1['SELL']
        bckt1['Neutral']= bckt1['Neutral'].sum(axis=1) if cols.count('Neutral')>1 else 0 if cols.count('Neutral')==0 else bckt1['Neutral']    

        bckt1=bckt1.loc[:,~bckt1.columns.duplicated()]
        bckt1['action']=bckt1.iloc[:,1:].idxmax(axis=1)
    except Exception as e:
        email(Buyorsell=str(e))
        final1=None
        data_tags=None
        bckt1=None
        response[0]=e
    return final1,response[0],bckt1,data_tags  


# In[411]:


def dump(crypto):
    time.sleep(1)
    
    try:
        from datetime import datetime,timedelta
        response=client.send("trades",{"limit": 1000, "symbol": {crypto}, "recvWindow": 10000, "timestamp": int(time.time() * 1000),"Signature":signature})
        data=(pd.DataFrame(response[1]))
        data['date_time']=pd.to_datetime(data['time'], unit='ms').apply(lambda x: (x+timedelta(minutes = 330)))

        data['date']=data['date_time'].apply(lambda x: x.strftime("%Y-%m-%d %H:%M"))
        main_data=pd.pivot_table(data, values=['price'], index=['date'],aggfunc={'price': [min, max]})['price'].reset_index()

        main_data.columns=['date_time','high','low']

        df=pd.pivot_table(data, values=['price','date_time'], index=['date'],aggfunc={'date_time': [min, max]})['date_time'].reset_index()

        df1=pd.merge(df,data[['date_time','price']],left_on='min',right_on='date_time',how='left').merge(data[['date_time','price']],left_on='max',right_on='date_time',how='left')
        df1=df1.drop(['max','min','date_time_x','date_time_y'],axis=1)
        df1.columns=['date_time','open','close']
        df1=df1[~df1.duplicated('date_time', keep='first')]
        df1=df1.reset_index().drop('index',axis=1)

        final_data=pd.concat([main_data,df1.drop('date_time',axis=1)],axis=1)
        final_data
        idx=pd.date_range(final_data['date_time'].min(), final_data['date_time'].max(), freq = "60s")
        final1=final_data.set_index(['date_time'])
        final1.index=pd.DatetimeIndex(final1.index)
        final1=final1.reindex(idx,method='ffill')
        final1=final1.reset_index()
        final1.columns=['date_time','high','low','open','close']
        final1['mins']=1
        final1=final1.sort_values(by='date_time',ascending=False).reset_index().drop('index',axis=1)
        final1['close']=final1['close'].astype(float)
        final1['EMA_13']=final1['close'].ewm(span=13, adjust=False).mean()
        final1['EMA_10']=final1['close'].ewm(span=10, adjust=False).mean()
        final1['diff']=final1['EMA_10'].astype(float)-final1['EMA_13'].astype(float)
        final1=final1.sort_values(by='date_time')
        final1['manual_RSI']=rsi_tradingview(final1,6,round_rsi=True)
        final1['macd_line']=MACD(final1,12,26,9)[0]
        final1['macd_signal_line']=MACD(final1,12,26,9)[1]
        final1['macd_cross']=MACD(final1,12,26,9)[2]
        final1

    except Exception as e:
        email(Buyorsell=str(e))
        final1=None
        response[0]=e
    return final1,response[0]


# In[412]:


def login():
    import os
    os.chdir(r"C:\Users\hanis\Downloads\wazirx-connector-python-master")

    from wazirx_sapi_client.rest import Client
    # private
    api_key = **add your api_key**
    secret_key = **add your secret_key**

    client = Client(api_key=api_key, secret_key=secret_key)
    
    return client,'connection_successful' if client.send("ping")[0]==200 else email(Buyorsell='connection_failed') 
 


# In[413]:


import hashlib
import hmac


api_key = "mqYT4am7T7Gt8BUWfuHY7aZfv4nQuGB7XkfjiU6OxdHAuYZy2GhXIe6H3EjBbwPb"
secret_key = "cxblmVt4rWxwOHqyUBzPhGdHL0fcfIerglfKUqax"

def get_signature(key): #signature function
    # encoding
    ts = int(round(time.time() * 1000))
    order = f"recvWindow=6000&timestamp={ts}" # Request body - funds
    byte_key = bytes(key, 'UTF-8')  # key.encode() would also work in this case
    message = order.encode()
    # now use the hmac.new function and the hexdigest method
    sign = hmac.new(byte_key, message, hashlib.sha256).hexdigest()
    return sign

#drivercode
# def funds():
#     secret_key = "cxblmVt4rWxwOHqyUBzPhGdHL0fcfIerglfKUqax"
#     ts = int(round(time.time() * 1000))
#     order = f"recvWindow=6000&timestamp={ts}" # Request body - funds
#     signature = get_signature(order,secret_key)
#     response = requests.get(f'https://api.wazirx.com/sapi/v1/funds?{order}&signature={signature}', headers = {'X-Api-Key': api_key})
#     funds=pd.DataFrame(response.json())
#     wallet=float(funds[funds['asset']=='inr']['free'][0])
#     return wallet


def funds():
    funds=pd.DataFrame(client.send('funds_info',{"recvWindow": 10000, "timestamp": int(time.time() * 1000)})[1])
    wallet=float(funds[funds['asset']=='inr']['free'][0])
    return wallet

def error(iter_error):
    import sys
    import traceback
    iter_error +=1
    ex_type, ex_value, ex_traceback = sys.exc_info()
    trace_back = traceback.extract_tb(ex_traceback)
    stack_trace = list()
    for trace in trace_back:
                stack_trace.append("File : %s , Line : %d, Func.Name : %s, Message : %s" % (trace[0], trace[1], trace[2], trace[3]))

    body="""<p><br>\n {0} <br><br>\n Value: {1} <br><br>\n Key_error: {2} </p>""".format(stack_trace[0],str(ex_value),str(ex_type).split("'")[1])
    email(Buyorsell=str(ex_type).split("'")[1]+' for {} & Iter_error:{}'.format(i,iter_error),body=body)
    time.sleep(30)
    return iter_error


# In[428]:


def buy(data1,crypto,client,bought_coin,buy_price,qty,wallet):
    try:
        live_response,live_price=client.send("ticker",{"symbol":crypto})

        response=client.send('create_order',{"symbol": crypto, 
                                             "side": "buy",
                                             "type": "limit",
                                             "price": float(live_price['askPrice']),
                                             "quantity": wallet/float(live_price['askPrice']),
                                             "recvWindow": 10000,
                                             "timestamp": int(time.time() * 1000)
                                            }
                                            )


        if response[0]==201:
            msg = "{} BOUGHT SUCCESSFULLY @{} INR".format(crypto,float(live_price['askPrice']))
            email(Buyorsell=msg)
            buy_price.update({crypto:float(live_price['askPrice'])})                        
            quantity=wallet/buy_price[crypto]
            orders=pd.DataFrame()
            orders['order_id']=round(time.time()*1000)
            orders['crypto']=crypto
            orders['status']='BUY'
            orders['price']=float(live_price['askPrice'])
            orders['qty']=quantity
            orders['wallet']=wallet-(quantity*buy_price[crypto])
            orders['last_updated']=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            orders['error']=None                        
            wallet=funds()
            bought_coin.append(crypto)
            qty.update({crypto:quantity})
            orders.to_sql('orders', if_exists='append', con=engine)
        else:
            email(Buyorsell='Purchase Unsucessful {} due to {}'.format(crypto,response[1]['message']))
            buy_price={}
            orders=pd.DataFrame()
            orders['order_id']=round(time.time()*1000)
            orders['crypto']=crypto
            orders['status']='BUY_FAILED'
            orders['price']=float(live_price['askPrice'])
            orders['qty']=wallet/float(live_price['askPrice'])
            orders['wallet']=wallet
            orders['last_updated']=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            orders['error']=response[1]['message']
            wallet=wallet
            orders.to_sql('orders', if_exists='append', con=engine)

    except:        
        error(iter_error)
        bought_coin=bought_coin
        qty=qty
        buy_price=buy_price
        return bought_coin,buy_price,qty


# In[429]:


def sell(client,data1,bp,sl,crypto,qty,bought_coin,buy_price):
                     # & ((float(live_price['bidPrice'])>=bp+(bp*0.002)))
    try:
        
        live_response,live_price=client.send("ticker",{"symbol":crypto})
        response=client.send('create_order',{"symbol": crypto, 
                                             "side": "sell", 
                                             "type": "limit", 
                                             "price": float(live_price['bidPrice']), 
                                             "quantity": qty[crypto], 
                                             "recvWindow": 10000,
                                             "timestamp": int(time.time() * 1000)
                                            }
                                            )

        open_orders=pd.DataFrame(client.send('open_orders',{"recvWindow": 10000,"timestamp": int(time.time() * 1000)})[1])
        if open_orders.empty:
            open_orders=[]             
        else:
            open_orders=open_orders['symbol'].to_list()

        if (response[0]==201) & (crypto not in open_orders):
            msg = "{} SOLD SUCCESSFULLY @{} INR".format(crypto,float(live_price['bidPrice'])) 
            
            email(Buyorsell=msg)
            sold_price=float(live_price['bidPrice'])
            wallet=funds()
            orders=pd.DataFrame()
            orders['order_id']=round(time.time()*1000)
            orders['crypto']=crypto
            orders['status']='SELL'
            orders['price']=float(live_price['bidPrice'])
            orders['qty']=qty[crypto]
            orders['wallet']=wallet
            orders['last_updated']=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            orders['error']=None
            orders.to_sql('orders', if_exists='append', con=engine)
            bought_coin.remove(crypto)
            qty.pop(crypto)
            buy_price.pop(crypto)

        elif crypto in open_orders:
            msg = "{} Order is in Open Status @ {} INR".format(crypto,open_orders[open_orders['symbol']==crypto]['price'][0]) 
            email(Buyorsell=msg)
            wallet=funds()
            orders=pd.DataFrame()
            orders['order_id']=round(time.time()*1000)
            orders['crypto']=crypto
            orders['status']='SELL_OPEN'
            orders['price']= float(open_orders[open_orders['symbol']==crypto]['price'][0])
            orders['qty']= float(open_orders[open_orders['symbol']==crypto]['origQty'][0])
            orders['wallet']=wallet
            orders['last_updated']=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            orders['error']=None
            orders.to_sql('orders', if_exists='append', con=engine)
            bought_coin=bought_coin
            qty=qty
            buy_price=buy_price

        else:
            email(Buyorsell='Sold Unsucessful {} due to {}'.format(crypto,'message'))#response[1]['message']
            wallet=funds()
            orders=pd.DataFrame()
            orders['order_id']=round(time.time()*1000)
            orders['crypto']=crypto
            orders['status']='SELL_FAILED'
            orders['price']=float(live_price['bidPrice'])
            orders['qty']=qty[crypto]
            orders['wallet']=wallet
            orders['last_updated']=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            orders['error']=response[1]['message']
            orders.to_sql('orders', if_exists='append', con=engine)
            bought_coin=bought_coin
            qty=qty
            buy_price=buy_price

    except:
        error(iter_error)
        wallet=funds()
        bought_coin=bought_coin
        qty=qty
        buy_price=buy_price
        return wallet,bought_coin,qty,buy_price


# In[459]:


import requests
import MySQLdb
import sqlalchemy
import pandas
con = MySQLdb.connect('127.0.0.1','root','Hanju@1993', 'wazirx')
con_str = 'mysql+mysqldb://root:Hanju@1993@127.0.0.1/wazirx'
engine = sqlalchemy.create_engine(con_str) #because I am using mysql
cur = con.cursor()

client=login()[0]
wallet=funds()



# crypto=['maticinr','manainr','xrpinr','dogeinr','adainr','batinr','chrinr','phainr','enjinr','cotiinr','ctsiinr','dockinr']
crypto=['enjinr']
crypto1=crypto.copy()
sl=0.02
bought_coin=[]
qty={}
buy_price={}
iter_error=0

while iter_error<=3:        
        import os
        os.chdir(r"C:\Users\hanis\Downloads\wazirx-connector-python-master")
        from wazirx_sapi_client.rest import Client
        
        api_key = 'wazirx_api_key'***
        secret_key = 'wazirx_secret_key'***
        client = Client(api_key=api_key, secret_key=secret_key)
        signature=get_signature(secret_key)
        
        import time
        from wazirx_sapi_client.rest import Client
        if datetime.now().second==0:
            for i in crypto:
                try:
                    dump_response=[]
                    data1,response_dump,bucket1,data_tags=dump_own(i,200)
                    dump_response.append((response_dump,i,datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
                    time.sleep(1)
                    live_response,live_price=client.send("ticker",{"symbol":i})
                    
                    for j in range(0,len(bucket1)):
                        if round(float(live_price['askPrice']),3) in bucket1['buckets'][j]:
                                action_buy=bucket1['action'][j]
                        if round(float(live_price['bidPrice']),3) in bucket1['buckets'][j]:
                                action_sell=bucket1['action'][j]
                        elif round(float(live_price['askPrice']),3)<=data_tags['close'].min():
                                action_buy='BUY'
                        elif round(float(live_price['bidPrice']),3)>=data_tags['close'].max():
                                action_sell='SELL'
   
                    data_tags['last_updated']=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    data_tags.to_sql('{}'.format(i+'_tags'), if_exists='append', con=engine)
                    pd.DataFrame(dump_response,columns=['status','coin','last_updated']).to_sql('response', if_exists='append', con=engine)
#                     time.sleep(1)
                    
                    wallet=funds()
                    if (action_buy=='BUY') & (wallet>=float(live_price['askPrice'])):
                        bucket1['date_time']=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        bucket1.to_sql('{}'.format('buy_bckt'), if_exists='append', con=engine)
                        bought_coin,buy_price,qty=buy(data1,i,client,bought_coin,buy_price,qty,wallet) 
                        
                        
                    bp=(0 if not buy_price else buy_price[i] if i in bought_coin else 0 )
                    
                    if (i in bought_coin) & (action_sell=='SELL') & (bp<float(live_price['bidPrice'])):
                        bucket1['date_time']=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        bucket1.to_sql('{}'.format('sell_bckt'), if_exists='append', con=engine)
                        wallet,bought_coin,qty,buy_price=sell(client,data1,bp,sl,i,qty,bought_coin,buy_price)  
                    
                    time.sleep(1)


                except:
                    iter_error=error(iter_error)

