'''
Created on 2020年1月19日

@author: xiaoda
'''
import tushare

if __name__ == '__main__':
    #使用TuShare pro版本    
    tushare.set_token('221f96cece132551e42922af6004a622404ae812e41a3fe175391df8')
    sdDataAPI=tushare.pro_api()
    
    df=sdDataAPI.fund_nav(ts_code='000218.OF',start_date='20200101',end_date='20200110')
        #end_date='20200102')
    print()