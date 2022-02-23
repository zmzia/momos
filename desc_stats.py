#import the libraries
import numpy as np
import pandas as pd

from dateutil import parser
import datetime
from datetime import datetime, timedelta

import itertools

# to get the past 12 hours order and sales occured for both promo/non-promo
###########################################################################
def get_past_12hrs(data, txn_dt, brand, outlet, channel, txn_hr, debug=False):

    txn_prv_dt = ""; txn_prv_hr = ""; qry_str1 = ""; qry_str5 = ""; qry_str_fin = "";
    qry_str_br = " (brand_name =='" + str(brand) + "')"
    qry_str_out = " and (outlet_name =='" + str(outlet) + "')"
    qry_str_chn = " and (channel =='" + str(channel) + "')"

    if txn_hr >=12 : #same day
        txn_prv_hr = txn_hr - 12
        qry_str1 = " and ((transaction_date =='" + str(txn_dt) + "' and transaction_hour >=" +str(txn_prv_hr) + ")"
        qry_str5 = " and (transaction_date =='" + str(txn_dt) + "' and transaction_hour <" +str(txn_hr) +"))"
        
    if txn_hr < 12 : # diff day
        mydate = datetime.strptime(txn_dt, "%Y-%m-%d")
        txn_prv_dt = mydate + timedelta(days=-1)
        txn_prv_dt = datetime.strftime(txn_prv_dt, "%Y-%m-%d")
        #print(type(txn_prv_dt),txn_prv_dt)
        hr_format = '%H'
        txn_prv_hr = datetime.strptime(str(int(txn_hr)), hr_format) - datetime.strptime(str(12), hr_format)
        txn_prv_hr = str(txn_prv_hr).split(",")[1].split(":")[0]
        #print(type(txn_prv_hr),txn_prv_hr)
        qry_str1 = " and ((transaction_date =='" + str(txn_prv_dt) + "' and transaction_hour >=" +str(txn_prv_hr) + ")"
        qry_str5 = " or (transaction_date =='" + str(txn_dt) + "' and transaction_hour <" +str(txn_hr) +"))"
        
    qry_str_fin = qry_str_br + qry_str_out + qry_str_chn + qry_str1 + qry_str5
    if debug==True : print('12h:',qry_str_fin)
    fltr_data = data.query(qry_str_fin)
    #display(fltr_data)
    return (fltr_data['total_promo_orders'].sum(),fltr_data['total_non_promo_orders'].sum()\
         ,fltr_data['total_promo_sales'].sum(),fltr_data['total_non_promo_sales'].sum())


# to get the past 15 days order and sales occured at the same hour-time for both promo/non-promo
################################################################################################
def get_past_15days(data,txn_dt, brand, outlet, channel, txn_hr, debug=False):

    qry_str_br = " (brand_name =='" + str(brand) + "')"
    qry_str_out = " and (outlet_name =='" + str(outlet) + "')"
    qry_str_chn = " and (channel =='" + str(channel) + "')"

    mydate = datetime.strptime(txn_dt, "%Y-%m-%d")
    txn_prv_dt = mydate + timedelta(days=-15)
    txn_prv_dt = datetime.strftime(txn_prv_dt, "%Y-%m-%d")
    qry_str1 = " and ((transaction_date >='" + str(txn_prv_dt) + "' and transaction_date <'" +str(txn_dt) + "')"
    qry_str5 = " and (transaction_hour ==" +str(txn_hr) +"))"

    qry_str_fin = qry_str_br + qry_str_out + qry_str_chn + qry_str1 + qry_str5
    if debug==True: print('15d:',qry_str_fin)
    fltr_data = data.query(qry_str_fin)
    #display(fltr_data)
    return (fltr_data['total_promo_orders'].sum(),fltr_data['total_non_promo_orders'].sum()\
         ,fltr_data['total_promo_sales'].sum(),fltr_data['total_non_promo_sales'].sum())


# to get the past quarter's order and sales occured at the same hour-time for both promo/non-promo
################################################################################################
def get_past_quarter(data,txn_dt, brand, outlet, channel, txn_hr, debug=False):

    qry_str_br = " (brand_name =='" + str(brand) + "')"
    qry_str_out = " and (outlet_name =='" + str(outlet) + "')"
    qry_str_chn = " and (channel =='" + str(channel) + "')"

    mydate = datetime.strptime(txn_dt, "%Y-%m-%d")
    txn_prv_dt = mydate + timedelta(days=-90)
    txn_prv_dt = datetime.strftime(txn_prv_dt, "%Y-%m-%d")
    qry_str1 = " and ((transaction_date >='" + str(txn_prv_dt) + "' and transaction_date <'" +str(txn_dt) + "')"
    qry_str5 = " and (transaction_hour ==" +str(txn_hr) +"))"

    qry_str_fin = qry_str_br + qry_str_out + qry_str_chn + qry_str1 + qry_str5
    if debug==True: print('Qtr:',qry_str_fin)
    fltr_data = data.query(qry_str_fin)
    #display(fltr_data)
    return (fltr_data['total_promo_orders'].sum(),fltr_data['total_non_promo_orders'].sum()\
         ,fltr_data['total_promo_sales'].sum(),fltr_data['total_non_promo_sales'].sum())


# to get brand master info
###########################
def get_brand_master(data,brands):
    data = data[data['brand_name'].isin(brands)]
    #brnd_cnt = data.groupby(['brand_name']). .values[['outlet_name','channel','order_type']]
    brnd_cnt_ls = data.groupby(['brand_name'])[['outlet_name','channel','order_type']].aggregate(lambda tdf:tdf.dropna().unique().tolist())
    brnd_sum = data.groupby(['brand_name']).sum()[['total_orders','total_sales','total_promo_orders','total_promo_sales',\
    'total_non_promo_orders','total_non_promo_sales', 'platform_promo_spend','ad_spend','clicks','impressions','reach']]
    brnd_mast = pd.concat([brnd_cnt_ls,brnd_sum],axis=1)
    return(brnd_mast)

# to get outlet master info
###########################
def get_outlet_master(data,outlet):
    data = data[data['outlet_name'].isin(outlet)]
    #brnd_cnt = data.groupby(['brand_name']). .values[['outlet_name','channel','order_type']]
    brnd_cnt_ls = data.groupby(['outlet_name'])[['channel','order_type']].aggregate(lambda tdf: tdf.dropna().unique().tolist())
    brnd_sum = data.groupby(['outlet_name']).sum()[['total_orders','total_sales','total_promo_orders','total_promo_sales',\
    'total_non_promo_orders','total_non_promo_sales', 'platform_promo_spend','ad_spend','clicks','impressions','reach']]
    brnd_mast = pd.concat([brnd_cnt_ls,brnd_sum],axis=1)
    return(brnd_mast)


# to get channel master info
###########################
def get_channel_master(data,channel):
    data = data[data['channel'].isin(channel)]
    #brnd_cnt = data.groupby(['brand_name']). .values[['outlet_name','channel','order_type']]
    brnd_cnt_ls = data.groupby(['channel'])[['order_type']].aggregate(lambda tdf: tdf.dropna().unique().tolist())
    brnd_sum = data.groupby(['channel']).sum()[['total_orders','total_sales','total_promo_orders','total_promo_sales',\
    'total_non_promo_orders','total_non_promo_sales', 'platform_promo_spend','ad_spend','clicks','impressions','reach']]
    brnd_mast = pd.concat([brnd_cnt_ls,brnd_sum],axis=1)
    return(brnd_mast)


######################################
# avg
#####################################

# to get the past 12 hours order and sales occured for both promo/non-promo
###########################################################################
def get_avg_past_12hrs(data, txn_dt, brand, outlet, channel, txn_hr, debug=False):

    txn_prv_dt = ""; txn_prv_hr = ""; qry_str1 = ""; qry_str5 = ""; qry_str_fin = "";
    qry_str_br = " (brand_name =='" + str(brand) + "')"
    qry_str_out = " and (outlet_name =='" + str(outlet) + "')"
    qry_str_chn = " and (channel =='" + str(channel) + "')"

    if txn_hr >=12 : #same day
        txn_prv_hr = txn_hr - 12
        qry_str1 = " and ((transaction_date =='" + str(txn_dt) + "' and transaction_hour >=" +str(txn_prv_hr) + ")"
        qry_str5 = " and (transaction_date =='" + str(txn_dt) + "' and transaction_hour <" +str(txn_hr) +"))"
        
    if txn_hr < 12 : # diff day
        mydate = datetime.strptime(txn_dt, "%Y-%m-%d")
        txn_prv_dt = mydate + timedelta(days=-1)
        txn_prv_dt = datetime.strftime(txn_prv_dt, "%Y-%m-%d")
        #print(type(txn_prv_dt),txn_prv_dt)
        hr_format = '%H'
        txn_prv_hr = datetime.strptime(str(int(txn_hr)), hr_format) - datetime.strptime(str(12), hr_format)
        txn_prv_hr = str(txn_prv_hr).split(",")[1].split(":")[0]
        #print(type(txn_prv_hr),txn_prv_hr)
        qry_str1 = " and ((transaction_date =='" + str(txn_prv_dt) + "' and transaction_hour >=" +str(txn_prv_hr) + ")"
        qry_str5 = " or (transaction_date =='" + str(txn_dt) + "' and transaction_hour <" +str(txn_hr) +"))"
        
    qry_str_fin = qry_str_br + qry_str_out + qry_str_chn + qry_str1 + qry_str5
    if debug==True : print('12h:',qry_str_fin)
    fltr_data = data.query(qry_str_fin)
    #display(fltr_data)
    return (fltr_data['total_promo_orders'].sum(),fltr_data['total_non_promo_orders'].sum()\
         ,fltr_data['total_promo_sales'].replace(0,np.nan).mean(),fltr_data['total_non_promo_sales'].replace(0,np.nan).mean())

# to get the past 15 days order and sales occured at the same hour-time for both promo/non-promo
################################################################################################
def get_avg_past_15days(data,txn_dt, brand, outlet, channel, txn_hr, debug=False):

    qry_str_br = " (brand_name =='" + str(brand) + "')"
    qry_str_out = " and (outlet_name =='" + str(outlet) + "')"
    qry_str_chn = " and (channel =='" + str(channel) + "')"

    mydate = datetime.strptime(txn_dt, "%Y-%m-%d")
    txn_prv_dt = mydate + timedelta(days=-15)
    txn_prv_dt = datetime.strftime(txn_prv_dt, "%Y-%m-%d")
    qry_str1 = " and ((transaction_date >='" + str(txn_prv_dt) + "' and transaction_date <'" +str(txn_dt) + "')"
    qry_str5 = " and (transaction_hour ==" +str(txn_hr) +"))"

    qry_str_fin = qry_str_br + qry_str_out + qry_str_chn + qry_str1 + qry_str5
    if debug==True: print('15d:',qry_str_fin)
    fltr_data = data.query(qry_str_fin)
    #display(fltr_data)
    return (fltr_data['total_promo_orders'].sum(),fltr_data['total_non_promo_orders'].sum()\
         ,fltr_data['total_promo_sales'].replace(0,np.nan).mean(),fltr_data['total_non_promo_sales'].replace(0,np.nan).mean())

# to get the past quarter's order and sales occured at the same hour-time for both promo/non-promo
################################################################################################
def get_avg_past_quarter(data,txn_dt, brand, outlet, channel, txn_hr, debug=False):

    qry_str_br = " (brand_name =='" + str(brand) + "')"
    qry_str_out = " and (outlet_name =='" + str(outlet) + "')"
    qry_str_chn = " and (channel =='" + str(channel) + "')"

    mydate = datetime.strptime(txn_dt, "%Y-%m-%d")
    txn_prv_dt = mydate + timedelta(days=-90)
    txn_prv_dt = datetime.strftime(txn_prv_dt, "%Y-%m-%d")
    qry_str1 = " and ((transaction_date >='" + str(txn_prv_dt) + "' and transaction_date <'" +str(txn_dt) + "')"
    qry_str5 = " and (transaction_hour ==" +str(txn_hr) +"))"

    qry_str_fin = qry_str_br + qry_str_out + qry_str_chn + qry_str1 + qry_str5
    if debug==True: print('Qtr:',qry_str_fin)
    fltr_data = data.query(qry_str_fin)
    #display(fltr_data)
    return (fltr_data['total_promo_orders'].sum(),fltr_data['total_non_promo_orders'].sum()\
         ,fltr_data['total_promo_sales'].replace(0,np.nan).mean(),fltr_data['total_non_promo_sales'].replace(0,np.nan).mean())



