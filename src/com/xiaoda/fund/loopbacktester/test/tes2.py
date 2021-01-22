'''
Created on 2021年1月3日

@author: xiaoda
'''
import tushare
import pandas as pd

if __name__ == '__main__':
    
    #显示所有列
    pd.set_option('display.max_columns', None)

    #使用TuShare pro版本    
    tushare.set_token('221f96cece132551e42922af6004a622404ae812e41a3fe175391df8')
    
    sdDataAPI=tushare.pro_api()

    df = sdDataAPI.fund_portfolio(ts_code='007280.OF')
    
    print()