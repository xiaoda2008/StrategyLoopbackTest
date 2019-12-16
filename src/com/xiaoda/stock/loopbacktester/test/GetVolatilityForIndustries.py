'''
Created on 2019年12月16日

@author: xiaoda
'''

from com.xiaoda.stock.loopbacktester.utils.MysqlUtils import MysqlProcessor
from com.xiaoda.stock.loopbacktester.utils.StockDataUtils import StockDataProcessor

class GetVolatilityForIndustries(object):
    '''
    classdocs
    '''


    def __init__(self):
        '''
        Constructor
        '''
        self.mysqlProcessor=MysqlProcessor()
        self.sdProcessor=StockDataProcessor()

    def getVolatilityDFForIndustries(self):


        #用于记录每个行业的波动率的字典
        industryVolDict={}
        
        allStockDF=self.sdProcessor.getAllStockDataDict()
                
        for (stockCode,scdict) in allStockDF.items():
            industry=scdict['industry']
            if not (industry in industryVolDict):
                #还未处理过该行业类别的股票
                valForStock=self.calVolatilityForStock(stockCode)
                industryVolDict[industry]={'num':0,'val':valForStock}
            else:
                #已经处理过，则取出原有数据
                #与当前股票数据取平均值后再次赋值
                valForStock=self.calVolatilityForStock(stockCode)
                
                num=industryVolDict[industry]['num']
                valForInd=industryVolDict[industry]['val']
                
                avgVal=round((num*valForInd+valForStock)/(num+1),4)
                
                industryVolDict[industry]={'num':num+1,'val':avgVal}
        
        #对每个股票进行遍历，
        #计算其最近一年内的波动幅度，并将同一行业的股票波动幅度求平均值
      
    def calVolatilityForStock(self,stockCode):
        #对于股票stockCode，计算过去一年的波动率
        #波动率用什么表示？
        #涨跌幅绝对值的平均值？
        #最大跌幅、最大涨幅？-》可能用最大涨跌幅更合理
        maxRetRate=self.getMaxRetRateForStockInLastYear(stockCode)
        maxIncRate=self.getMaxIncRateForStockInLastYear(stockCode)
        
        return (maxRetRate,maxIncRate)
    
    
    def getMaxRetRateForStockInLastYear(self,stockCode):
        '''计算股票在最近一年内的最大回撤率'''
        maxRetRate=0
        
        return maxRetRate
    
    def getMaxIncRateForStockInLastYear(self,stockCode):
        '''计算股票在最近一年内的最大上涨率'''
        maxIncRate=0
        
        return maxIncRate
    
    
    def getTopDataInLastYear(self,stockCode):
        '''计算股票在最近一年内最高点的信息：价格，日期'''
        
        return 0,20190101
        
    def getBottomDataInLastYear(self,stockCode):
        '''计算股票在最近一年内最低点的信息：价格，日期'''
        
        return 0,20190101
    
    
gfi=GetVolatilityForIndustries()
gfi.getVolatilityDFForIndustries()     