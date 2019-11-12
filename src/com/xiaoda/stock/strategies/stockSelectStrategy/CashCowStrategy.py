'''
Created on 2019年11月12日

@author: picc
'''
from com.xiaoda.stock.strategies.stockSelectStrategy.StrategyParent import StrategyParent
from datetime import datetime as dt
import time

class CashCowStrategy(StrategyParent):
    '''
    classdocs
    '''
    @staticmethod
    def getlastquarterfirstday():
        today=dt.now()
        quarter = (today.month-1)/3+1
        if quarter == 1:
            return dt(today.year-1,10,1)
        elif quarter == 2:
            return dt(today.year,1,1)
        elif quarter == 3:
            return dt(today.year,4,1)
        else:
            return dt(today.year,7,1)
    
    #决定对哪些股票进行投资
    def getStockList(self,sdDataAPI):
        startday=CashCowStrategy.getlastquarterfirstday().strftime('%Y%m%d')

        sdf = sdDataAPI.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')

        cfRatioDict={}

        for idx in sdf.index:

            #获取资产负债表，总资产
            bs = sdDataAPI.balancesheet(ts_code=sdf.at[idx,'ts_code'],start_date=startday,end_date=dt.now().strftime('%Y%m%d'), fields='ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,cap_rese,total_assets')
            bs.at[0,'total_assets']
            time.sleep(0.75)
    
            #获取现金流量表中，现金等价物总数
            cf = sdDataAPI.cashflow(ts_code=sdf.at[idx,'ts_code'],start_date=startday,end_date=dt.now().strftime('%Y%m%d'))#, period='20190930')
            cf.at[0,'c_cash_equ_end_period']
        
            ratio=cf.at[0,'c_cash_equ_end_period']/bs.at[0,'total_assets']
    
            cfRatioDict[sdf.at[idx,'ts_code']]=ratio

        sortedCFRatioList=sorted(cfRatioDict.items(),key=lambda x:x[1],reverse=True)

        returnStockList=[]

        for tscode, ratio in sortedCFRatioList[:100]:    
            returnStockList.append(tscode)
        
        return returnStockList
