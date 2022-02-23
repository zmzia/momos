import pandas as pd
from flask import Flask, render_template, request
import joblib
from desc_stats import *
from pycaret.classification import *

app = Flask(__name__)

model = load_model('sm_momos_linear_model')

@app.route('/')
def hi():
    return render_template('index.html')

@app.route('/predict', methods=['POST'])
def predict():
    print(request.form.values())
    inp = [i for i in request.form.values()]
    print('====x====')
    for j in inp: print(j)    
    txdt = inp[2] + "-" + inp[1] + "-" + inp[0] 
    brnd = inp[3]
    outlt = inp[4]
    chn = inp[5]
    txhr = int(inp[6])
    print(txdt,brnd,outlt,chn,txhr)
    actual = ''
    result = []
    ip_data = pd.read_csv('Momos Data Case Study - Data Set (1).csv')
    result.extend([brnd,outlt,chn,txhr])
    result.extend(get_past_12hrs(ip_data,txdt,brnd,outlt,chn,txhr))
    result.extend(get_past_15days(ip_data,txdt,brnd,outlt,chn,txhr))
    result.extend(get_past_quarter(ip_data,txdt,brnd,outlt,chn,txhr))

    #piv_data = pd.DataFrame([result])
    #display(piv_data)
    #print('...1...')
    #print(type(result),result)
    test_df = pd.DataFrame([result])

    X = ['brand_name', 'outlet_name', 'channel', 'transaction_hour', \
       'past_12h_tot_ord_promo', 'past_12h_tot_ord_nonpromo',\
       'past_12h_tot_sales_promo', 'past_12h_tot_sales_nonpromo',\
       'past_15d_tot_ord_promo', 'past_15d_tot_ord_nonpromo',\
       'past_15d_tot_sales_promo', 'past_15d_tot_sales_nonpromo',\
       'past_qtr_tot_ord_promo', 'past_qtr_tot_ord_nonpromo',\
       'past_qtr_tot_sales_promo', 'past_qtr_tot_sales_nonpromo' ]

    test_df.columns = X
    #print('...2...')
    #print(test_df.info())
    #print(model)
    res = model.predict(test_df)
    print(res[0])
    add_info=""
    if res[0]==0:
        add_info = "Promotion is not recommended"
    else:
        add_info = "Promotion is recommended"

    add_info = add_info + "<br><br>Recommendations were based upon the below Descriptive Stats<br>" + "-"*75 + " \n" + \
        "<br>Total Promo Orders for past 12 hours :" + str(test_df.loc[0,['past_12h_tot_ord_promo']].values) + \
        "<br>Total Promo Sales for past 12 hours :" + str(test_df.loc[0,['past_12h_tot_sales_promo']].values)  + \
        "<br>Total Non-Promo Orders for past 12 hours :" + str(test_df.loc[0,['past_12h_tot_ord_nonpromo']].values) + \
        "<br>Total Non-Promo Sales for past 15 days :" + str(test_df.loc[0,['past_12h_tot_sales_nonpromo']].values)  + \
        "<br><br>Total Promo Orders for past 15 days :" + str(test_df.loc[0,['past_15d_tot_ord_promo']].values) + \
        "<br>Total Promo Sales for past 15 days :" + str(test_df.loc[0,['past_15d_tot_sales_promo']].values)  + \
        "<br>Total Non-Promo Orders for past 15 days :" + str(test_df.loc[0,['past_15d_tot_ord_nonpromo']].values) + \
        "<br>Total Non-Promo Sales for past 15 days :" + str(test_df.loc[0,['past_15d_tot_sales_nonpromo']].values)  + \
        "<br><br>Total Promo Orders for past quarter :" + str(test_df.loc[0,['past_qtr_tot_ord_promo']].values) + \
        "<br>Total Promo Sales for past quarter :" + str(test_df.loc[0,['past_qtr_tot_sales_promo']].values)  + \
        "<br>Total Non-Promo Orders for past quarter :" + str(test_df.loc[0,['past_qtr_tot_ord_nonpromo']].values) + \
        "<br>Total Non-Promo Sales for past quarter :" + str(test_df.loc[0,['past_qtr_tot_sales_nonpromo']].values)  + "<br>"+"-"*75

    mess = add_info
    print(mess)
    print('@@@@@@-3-@@@@@@')
    #return str(res[0])
    #brnd_list = ['Pizzeria','The Brunch Place']
    brnd_list = [inp[3]]
    brnd_df = get_brand_master(ip_data,brnd_list)

    mess2 ='bbbbb'
    #outlet_list = ['Pizzeria - Central','Pizzeria - East']
    outlet_list = [inp[4]]
    outlet_df = get_outlet_master(ip_data,outlet_list)

    #channel_list = ['Platform A', 'Facebook', 'Platform B', 'Platform C']
    channel_list = [inp[5]]
    channel_df = get_channel_master(ip_data,channel_list)

    #print(mess2.values)
    '''
    if isinstance(res[0], float):
        mess = "Recommendation is {} ".format(str(res[0]))
    else:
        mess = "Please enter valid input"
    '''
    return render_template('index.html', prediction = mess, prediction_remarks = mess2, \
        brandtable=[brnd_df.T.to_html(classes='data', header="true")], \
            outlettable=[outlet_df.T.to_html(classes='data', header="true")],\
                channeltable=[channel_df.T.to_html(classes='data', header="true")]
        )

if __name__ == "__main__":
    app.run(debug = True)