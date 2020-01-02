'''
Created on 2019年12月22日

@author: xiaoda
'''

import os
from com.xiaoda.stock.loopbacktester.strategy.stockselect.StrategyParent import StrategyParent
from com.xiaoda.stock.loopbacktester.utils.FinanceDataUtils import FinanceDataProcessor
from com.xiaoda.stock.loopbacktester.utils.LoggingUtils import Logger


class CCPlusNPRStrategy(StrategyParent):
    '''
    classdocs
    '''
    
    
    log = Logger(os.path.split(__file__)[-1].split(".")[0]+'.log',level='info')
    
    def __init__(self):
        '''
        Constructor
        '''
        self.name="CCPlusNPRStrategy"
        self.finProcessor=FinanceDataProcessor()
    
    #决定对哪些股票进行投资
    def getSelectedStockList(self,sdProcessor,startdateStr):
        
        sdict=sdProcessor.getAllStockDataDict()
        
        npRatioDict={}
        cfRatioDict={}
        
        #可以考虑多进程？
        for (stockCode,scdict) in sdict.items():
            
            listdate=scdict['list_date']
            
            if listdate>startdateStr:
                continue
            
            bs=self.finProcessor.getLatestBalanceSheetReport(stockCode,startdateStr,False)
            #bs为所有之前发布的所有资产负债表数据
            
            #获取现金流量表中，现金等价物总数
            cf=self.finProcessor.getLatestCashFlowReport(stockCode,startdateStr,False)
            #cf为之前发布的所有现金流量表数据
            
            ic=self.finProcessor.getLatestIncomeReport(stockCode,startdateStr,True)
            #ic为之前发布的所有利润表数据
            
            db=self.finProcessor.getLatestDailyBasic(stockCode,startdateStr)
            #db为之前最近一个交易日的每日数据
            
            #有可能数据不全，直接跳过
            if bs.empty or cf.empty or ic.empty or db.empty:
                continue
            
            try:
                #商誉
                goodwill=bs[bs['goodwill'].notnull()].reset_index(drop=True).at[0,'goodwill']      
            except:
                goodwill=0
                pass
            
            
            #需要到里面找到最后一个不是空的总资产
            try:
                #净利润
                netIncome1=ic[ic['n_income'].notnull()].reset_index(drop=True).at[0,'n_income']
                netIncome2=ic[ic['n_income'].notnull()].reset_index(drop=True).at[1,'n_income']           
                netIncome3=ic[ic['n_income'].notnull()].reset_index(drop=True).at[2,'n_income']
                netIncome4=ic[ic['n_income'].notnull()].reset_index(drop=True).at[3,'n_income']                
                
                #总收入
                totalAvenue1=ic[ic['total_revenue'].notnull()].reset_index(drop=True).at[0,'total_revenue']
                totalAvenue2=ic[ic['total_revenue'].notnull()].reset_index(drop=True).at[1,'total_revenue']
                totalAvenue3=ic[ic['total_revenue'].notnull()].reset_index(drop=True).at[2,'total_revenue']
                totalAvenue4=ic[ic['total_revenue'].notnull()].reset_index(drop=True).at[3,'total_revenue']
                
                #总资产
                totalAsset=bs[bs['total_assets'].notnull()].reset_index(drop=True).at[0,'total_assets']

                #现金等价物
                cashequ=cf[cf['c_cash_equ_end_period'].notnull()].reset_index(drop=True).at[0,'c_cash_equ_end_period']

                #总市值
                total_mv=db.at[0,'total_mv']
                
                #流通市值
                circ_mv=db.at[0,'circ_mv']
                
            except KeyError:
                
                continue

 
            #if total_mv<2000000:
                #剔除市值在200亿以下的
            #    continue
            #if total_mv>2000000 or total_mv<800000:
            #    continue
            #if circ_mv/total_mv>0.6:
                #剔除流通市值占比过大的
            #    continue
            if netIncome4>netIncome3 or netIncome3>netIncome2 or netIncome2>netIncome1:
                continue
            elif goodwill/totalAsset>0.5:
                continue
            elif totalAvenue4>totalAvenue3 or totalAvenue3>totalAvenue2 or totalAvenue2>totalAvenue1:
                continue
            elif netIncome4<100000000:
                #对于净利润小于1亿直接排除
                continue
            elif cashequ<100000000 or totalAsset<1000000000:
                #对于现金等价物小于1亿或者总资产小于10亿的直接排除
                continue 
            else:
                cashequRatio=cashequ/totalAsset              
                profitRatio=(netIncome1+netIncome2+netIncome3+netIncome4)/(totalAvenue1+totalAvenue2+totalAvenue3+totalAvenue4)
    
            npRatioDict[stockCode]=profitRatio
            cfRatioDict[stockCode]=cashequRatio
            
            self.log.logger.info('CCPlusNPRStrategy,profitRatio'+stockCode+','+str(profitRatio))            
            self.log.logger.info('CCPlusNPRStrategy,cashequRatio:'+stockCode+','+str(cashequRatio))

        sortedNPRatioTuples=sorted(npRatioDict.items(),key=lambda x:x[1],reverse=True)
        sortedCFRatioTuples=sorted(cfRatioDict.items(),key=lambda x:x[1],reverse=True)
        
        
        sortedNPRatioList=[]
        sortedCFRatioList=[]
        
        for tscode,ratio in sortedNPRatioTuples:
            sortedNPRatioList.append(tscode)   

        for tscode,ratio in sortedCFRatioTuples:
            sortedCFRatioList.append(tscode)           
        
        
        
        returnStockList=list(set(sortedNPRatioList[:100]).intersection(set(sortedCFRatioList[:100])))

        
        return returnStockList
