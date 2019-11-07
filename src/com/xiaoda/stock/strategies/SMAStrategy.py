'''
Created on 2019年10月29日

@author: picc
'''
import pandas
import math
from com.xiaoda.stock.strategies.StrategyParent import StrategyParent
from com.xiaoda.stock.loopbacktester.utils.ParamUtils import nShare


class SMAStrategy(StrategyParent):
    '''
    SMA（简单移动平均）策略
            根据MA的数值变化情况，决定是否买入或者卖出
    '''

 
#可参考的文章：https://www.jianshu.com/p/642ad8a0366e


    #决定买入或卖出的数量
    #正数代表买入，负数代表卖出
    #continuousRiseOrFallCnt，正数代表连续上涨，负数代表连续下跌
    #MA策略不考虑是否第一天，都按照同样的逻辑进行判断
    def getShareToBuyOrSell(self,priceNow,latestDealPrice, 
                     latestDealType,holdShares,
                     holdAvgPrice,continuousRiseOrFallCnt,
                     stock_k_data,todayDate):
        
        stock_k_data = stock_k_data.set_index('trade_date')
        
        #lastMarketDay=MysqlUtils.getLastMarketDay(todayDate)
        
        todayMA20 = stock_k_data.at[todayDate,'today_MA20']
        yesterdayDayMA20 = stock_k_data.at[todayDate,'yesterday_MA20']
        
        #如果没有MA20数据
        if pandas.isna(yesterdayDayMA20):
            return 0
        
        #需要调整，当天，只可能知道当天开盘价，无法知道当天平均价，不能采用上帝模式

        #应该用当天开盘价与前一天的MA20进行比较
        if stock_k_data.at[todayDate,'pre_close']<yesterdayDayMA20 and priceNow>yesterdayDayMA20:
            #前一天收盘价格低于20日均线，且当天开盘价格高于20日均线-》上穿20日均线，可以买入
            return math.floor(nShare/2)
        elif stock_k_data.at[todayDate,'pre_close']>yesterdayDayMA20 and priceNow<yesterdayDayMA20:
            #前一天收盘价格高于20日均线，且当天开盘价格低于20日均线-》下穿20日均线，可以卖出
            return -1*math.floor(nShare/2)
        else:
            return 0
        
