import pandas as pd
import numpy as np
import time 

def get_stock_data(ticker,
                   dict_subperiods):
    
    api_private_key = '916YV9L00OXB1Z39'
    print ('reading data from api, ticker:',ticker)
    df_return_subperiod = pd.DataFrame(index=[ticker], columns=dict_subperiods.keys(), data=np.nan)
    url = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={}&apikey={}'.format(ticker, api_private_key)
    max_try = 5
    while True:
        df_ticker_raw = pd.read_json(url, typ='series').apply(pd.Series).T
        df_ticker_raw.reset_index(inplace=True)
        print ('data retreived', df_ticker_raw.shape)
        if df_ticker_raw.shape[0]>=100:
            print ('data retreived successfully')
            break
        if max_try ==0:
            print ('max try reached')
        if df_ticker_raw.shape[0]==1:
            print ('data failed to retreive, try again after 5 seconds')
            max_try = max_try - 1
            time.sleep(5)
        else:
            print ('unexpected error, try again')
            time.sleep(5)
            max_try = max_try - 1
    # clean raw data
    df_ticker_clean = df_ticker_raw.iloc[5:].set_index(['index'])['Time Series (Daily)'].apply(pd.Series)[['4. close', '5. volume']].reset_index()
    df_ticker_clean.columns = ['date','close','volume']
    df_ticker_clean['ticker'] = ticker
    df_ticker_clean['date'] = pd.to_datetime(df_ticker_clean['date'])
    df_ticker_clean['close'] = pd.to_numeric(df_ticker_clean['close'])
    df_ticker_clean.sort_values(by=['date'],ascending=False,inplace=True)
    # calculate subperiod returns
    for subperiod in dict_subperiods.keys():
        _return = df_ticker_clean['close'].iloc[0]/df_ticker_clean['close'].iloc[dict_subperiods[subperiod]+1]-1
        df_return_subperiod.loc[ticker][subperiod] = _return
        
    # delete unused dataframe
    del df_ticker_raw, url
    df_return_subperiod.index.names = ['ticker']
    df_return_subperiod.reset_index(inplace=True)
    
    return df_ticker_clean, df_return_subperiod

def get_test_data (path_test):
    
    df_test = pd.read_csv(path_test)
    
    return df_test
    
if __name__ == "__main__":
    
    list_tickers = ['SPY','QQQ','DIA']
    dict_subperiods = {'1-Day':1, '5-Day':5,'1-Month': 20, '3-Month':60}                   

    df_all = pd.DataFrame()
    df_return_subperiod_all = pd.DataFrame()
    for ticker in list_tickers:
        df_data, df_return_subperiod = get_stock_data(ticker = ticker,
                                                      dict_subperiods = dict_subperiods)
        
        df_all = df_all.append(df_data)
        df_return_subperiod_all = df_return_subperiod_all.append(df_return_subperiod)
    df_all.to_csv(r"C:\Projects\powerBI_course\market_dashboard\price.csv")     
    df_return_subperiod_all.to_csv(r"C:\Projects\powerBI_course\market_dashboard\return.csv")     


    
    
   
