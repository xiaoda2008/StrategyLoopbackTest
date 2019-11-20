'''
Created on 2019年11月12日

@author: picc
'''
import os
from com.xiaoda.stock.loopbacktester.strategy.stockselect.StrategyParent import StrategyParent
from com.xiaoda.stock.loopbacktester.utils.StockDataUtils import StockDataProcessor
from com.xiaoda.stock.loopbacktester.utils.FinanceDataUtils import FinanceDataProcessor
from com.xiaoda.stock.loopbacktester.utils.LoggingUtils import Logger


class CashCowStrategy(StrategyParent):
    '''
    classdocs
    '''
    
    
    log = Logger(os.path.split(__file__)[-1].split(".")[0]+'.log',level='info')
    
    def __init__(self):
        '''
        Constructor
        '''
        self.name="CashCowStrategy"
    
    #决定对哪些股票进行投资
    def getSelectedStockList(self,dateStr):
        #startday=CashCowStrategy.getlastquarterfirstday().strftime('%Y%m%d')


        #sdf = sdDataAPI.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
        stockList=StockDataProcessor.getAllStockList()
        
        cfRatioDict={}

        for stockCode in stockList:

            #if stockCode=='300796.SZ':
            #   print()

            bs=FinanceDataProcessor.getLatestStockBalanceSheet(stockCode,dateStr)
            #获取资产负债表，总资产
            #bs = sdDataAPI.balancesheet(ts_code=sdf.at[idx,'ts_code'],start_date=startday,end_date=dt.now().strftime('%Y%m%d'), fields='ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,cap_rese,total_assets')
            #bs.at[0,'total_assets']
            #time.sleep(0.8)
    
            #获取现金流量表中，现金等价物总数
            cf=FinanceDataProcessor.getLatestStockCashFlow(stockCode,dateStr)
            #cf = sdDataAPI.cashflow(ts_code=sdf.at[idx,'ts_code'],start_date=startday,end_date=dt.now().strftime('%Y%m%d'))#, period='20190930')
            #cf.at[0,'c_cash_equ_end_period']
        
            if bs.empty or cf.empty:
                ratio=0
            else:
                ratio=cf.at[0,'c_cash_equ_end_period']/bs.at[0,'total_assets']
    
            cfRatioDict[stockCode]=ratio
            
            self.log.logger.info('CashCowStrategy:'+stockCode+','+str(ratio))

        sortedCFRatioList=sorted(cfRatioDict.items(),key=lambda x:x[1],reverse=True)

        returnStockList=[]

        for tscode, ratio in sortedCFRatioList[:100]:    
            returnStockList.append(tscode)
        
        return returnStockList
