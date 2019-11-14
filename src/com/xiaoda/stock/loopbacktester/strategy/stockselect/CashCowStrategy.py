'''
Created on 2019年11月12日

@author: picc
'''
from com.xiaoda.stock.loopbacktester.strategy.stockselect.StrategyParent import StrategyParent
from datetime import datetime as dt
import time
from com.xiaoda.stock.loopbacktester.utils.MysqlUtils import MysqlProcessor

class CashCowStrategy(StrategyParent):
    '''
    classdocs
    '''

    
    #决定对哪些股票进行投资
    def getSelectedStockList(self,dateStr):
        #startday=CashCowStrategy.getlastquarterfirstday().strftime('%Y%m%d')


        #sdf = sdDataAPI.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
        sdf=MysqlProcessor.getStockList()
        
        cfRatioDict={}

        for idx in sdf.index:


            bs=MysqlProcessor.getLatestStockBalanceSheet(sdf.at[idx,'ts_code'],dateStr)
            #获取资产负债表，总资产
            #bs = sdDataAPI.balancesheet(ts_code=sdf.at[idx,'ts_code'],start_date=startday,end_date=dt.now().strftime('%Y%m%d'), fields='ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,cap_rese,total_assets')
            #bs.at[0,'total_assets']
            #time.sleep(0.8)
    
            #获取现金流量表中，现金等价物总数
            cf=MysqlProcessor.getLatestStockCashFlow(sdf.at[idx,'ts_code'],dateStr)
            #cf = sdDataAPI.cashflow(ts_code=sdf.at[idx,'ts_code'],start_date=startday,end_date=dt.now().strftime('%Y%m%d'))#, period='20190930')
            #cf.at[0,'c_cash_equ_end_period']
        
            ratio=cf.at[0,'c_cash_equ_end_period']/bs.at[0,'total_assets']
    
            cfRatioDict[sdf.at[idx,'ts_code']]=ratio
            
            print('CashCowStrategy:'+sdf.at[idx,'ts_code']+','+str(ratio))

        sortedCFRatioList=sorted(cfRatioDict.items(),key=lambda x:x[1],reverse=True)

        returnStockList=[]

        for tscode, ratio in sortedCFRatioList[:100]:    
            returnStockList.append(tscode)
        
        return returnStockList
