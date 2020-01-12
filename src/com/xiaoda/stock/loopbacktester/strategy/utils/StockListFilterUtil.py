'''
Created on 2020年1月11日

@author: xiaoda
'''

from com.xiaoda.stock.loopbacktester.utils.ParamUtils import industryProprtion

class StockListFilterProcessor(object):
    '''
    防暴雷逻辑
    '''
    
    def __init__(self, params):
        '''
        Constructor
        '''   
    @staticmethod
    def filterStockList(stockCodeList,sdProcessor):

        #对于同一行业出现的股票过多，需要进行剔除
        #需要确定系数，允许同一行业出现的股票数量占比多大
        
        #统计各行业的数量，如果某个行业的股票数量已经超过阈值，则remove

        threshHold=len(stockCodeList)*industryProprtion
                
        industryNumDict={}

        stockToRemList=[]

        for stock in stockCodeList:
            
            stInfoDF=sdProcessor.getStockInfo(stock)
            
            industry=stInfoDF.at[0,'industry']
            
            if not industry in industryNumDict:
                industryNumDict[industry]=1
            elif industryNumDict[industry]+1>threshHold:
                stockToRemList.append(stock)
            else:
                industryNumDict[industry]=industryNumDict[industry]+1


        for stock in stockToRemList:
            stockCodeList.remove(stock)
        
        #对于超出限制比例的行业，循环股票列表，剔除超出比例的股票

                    
        return stockCodeList