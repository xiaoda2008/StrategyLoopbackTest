'''
Created on 2019年11月12日

@author: picc
'''
import os
from com.xiaoda.stock.loopbacktester.strategy.stockselect.StrategyParent import StrategyParent
from com.xiaoda.stock.loopbacktester.utils.FinanceDataUtils import FinanceDataProcessor
from com.xiaoda.stock.loopbacktester.utils.LoggingUtils import Logger
from numpy.distutils.log import good
from com.xiaoda.stock.loopbacktester.strategy.utils.RiskAvoidUtil import RiskAvoidProcessor
from com.xiaoda.stock.loopbacktester.strategy.utils.StockListFilterUtil import StockListFilterProcessor



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
        #是否可以考虑放宽范围？会是什么效果？
        sdict=sdProcessor.getHS300Dict()
        #sdict=sdProcessor.getZZ500Dict()
        
        
        cfRatioDict={}

        #可以考虑多进程？
        for (stockCode,scdict) in sdict.items():
            if stockCode=="600519.SH":
                continue

                        
            listdate=scdict['list_date']
            
            if listdate>startdateStr:
                continue


            bs=self.finProcessor.getLatestBalanceSheetReport(stockCode,startdateStr,False)
            #bs为所有之前发布的所有资产负债表数据
                
            #获取现金流量表中，现金等价物总数
            cf=self.finProcessor.getLatestCashFlowReport(stockCode,startdateStr,False)
            #cf为之前发布的所有现金流量表数据
            
            ic=self.finProcessor.getLatestIncomeReport(stockCode,startdateStr,False)
            #ic为之前发布的所有利润表数据
             
            #获取最近的每日信息
            db=self.finProcessor.getLatestDailyBasic(stockCode, startdateStr)
            
            
            #有可能数据不全，直接跳过
            if bs.empty or cf.empty:
                continue
            
            
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

                #if db.empty:
                #    percentInLst5Years=0
                #else:
                #    percentInLst5Years=db.at[0,'Percentage_PE_Lst_5Years']
                
            except KeyError:
                continue
            
            #cf = sdDataAPI.cashflow(ts_code=sdf.at[idx,'ts_code'],start_date=startday,end_date=dt.now().strftime('%Y%m%d'))#, period='20190930')
            #cf.at[0,'c_cash_equ_end_period']


            #self.log.logger.info("debug:%s"%(stockCode))

            #if stockCode=='002773.SZ':
            #    print()
                
            #防暴雷、防财务造假逻辑
            if RiskAvoidProcessor.getRiskAvoidFlg(stockCode, ic, bs, cf, sdProcessor)==True:
                continue
            
            if cashequ<100000000 or totalAsset<1000000000:
                #对于现金等价物小于1亿或者总资产小于10亿的直接排除
                continue  
            #elif percentInLst5Years>0.9:
            #    continue
            else:
                ratio=cashequ/totalAsset
    
            cfRatioDict[stockCode]=ratio
            
            self.log.logger.info('CashCowStrategy:%s的CCRate:%f'%(stockCode,ratio))
            
        sortedCFRatioList=sorted(cfRatioDict.items(),key=lambda x:x[1],reverse=True)

        tmpStockList=[]
        for tscode, ratio in sortedCFRatioList[:30]:
            tmpStockList.append(tscode)

        #这里应当保留每个股票的Ratio，以便后续进行人工筛选时，对同一行业或者类似行业的股票进行人工筛选
        
        
        #删选以避免某一行业占比过高
        returnStockList=StockListFilterProcessor.filterStockList(tmpStockList, sdProcessor)        
        
        #选出的股票不要少于10支
        if len(returnStockList)<10:
            tmpStockList=[]
            for tscode, ratio in sortedCFRatioList[:50]:
                tmpStockList.append(tscode)
            
            #删选以避免某一行业占比过高
            returnStockList=StockListFilterProcessor.filterStockList(tmpStockList, sdProcessor)        
        
        
        return returnStockList
