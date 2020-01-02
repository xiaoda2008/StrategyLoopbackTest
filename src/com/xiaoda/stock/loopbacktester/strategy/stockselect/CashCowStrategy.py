'''
Created on 2019年11月12日

@author: picc
'''
import os
from com.xiaoda.stock.loopbacktester.strategy.stockselect.StrategyParent import StrategyParent
from com.xiaoda.stock.loopbacktester.utils.FinanceDataUtils import FinanceDataProcessor
from com.xiaoda.stock.loopbacktester.utils.LoggingUtils import Logger
from numpy.distutils.log import good


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
        self.finProcessor=FinanceDataProcessor()
    
    #决定对哪些股票进行投资
    def getSelectedStockList(self,sdProcessor,startdateStr):
        #startday=CashCowStrategy.getlastquarterfirstday().strftime('%Y%m%d')


        #sdf = sdDataAPI.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
        sdict=sdProcessor.getHS300Dict()
        
        cfRatioDict={}

        #可以考虑多进程？
        for (stockCode,scdict) in sdict.items():
            
            listdate=scdict['list_date']
            
            if listdate>startdateStr:
                continue
            #if stockCode=='300796.SZ':
            #   print()

            bs=self.finProcessor.getLatestBalanceSheetReport(stockCode,startdateStr,False)
            #bs为所有之前发布的所有资产负债表数据
                
            #获取现金流量表中，现金等价物总数
            cf=self.finProcessor.getLatestCashFlowReport(stockCode,startdateStr,False)
            #cf为之前发布的所有现金流量表数据
            
            #有可能数据不全，直接跳过
            if bs.empty or cf.empty:
                continue
            
            
            try:
                #商誉
                goodwill=bs[bs['goodwill'].notnull()].reset_index(drop=True).at[0,'goodwill']      
            except:
                goodwill=0
                pass
            
            #需要到里面找到最后一个不是空的总资产
            #if stockCode>'000768.SZ':
            #    print()
            try:
                #总资产
                totalAsset=bs[bs['total_assets'].notnull()].reset_index(drop=True).at[0,'total_assets']

                #总负债
                #totalLiab=bs[bs['total_liab'].notnull()].reset_index(drop=True).at[0,'total_liab']       
                
                #netAssets=totalAsset-totalLiab
                
                #现金等价物
                cashequ=cf[cf['c_cash_equ_end_period'].notnull()].reset_index(drop=True).at[0,'c_cash_equ_end_period']

            except KeyError:
                continue
            
            #cf = sdDataAPI.cashflow(ts_code=sdf.at[idx,'ts_code'],start_date=startday,end_date=dt.now().strftime('%Y%m%d'))#, period='20190930')
            #cf.at[0,'c_cash_equ_end_period']
        
            if cashequ<100000000 or totalAsset<1000000000:
                #对于现金等价物小于1亿或者总资产小于10亿的直接排除
                continue  
            elif goodwill/totalAsset>0.5:
                continue
            else:
                ratio=cashequ/totalAsset
    
            cfRatioDict[stockCode]=ratio
            
            self.log.logger.info('CashCowStrategy:%s的CCRate:%f,商誉:%f'%(stockCode,ratio,goodwill/totalAsset))
            
        sortedCFRatioList=sorted(cfRatioDict.items(),key=lambda x:x[1],reverse=True)

        returnStockList=[]

        for tscode, ratio in sortedCFRatioList[:10]:
            returnStockList.append(tscode)
        
        return returnStockList
