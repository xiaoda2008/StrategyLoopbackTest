'''
Created on 2020年1月15日

@author: xiaoda
'''
import pandas as pd
##利用最小二乘法进行线性回归，拟合CAPM模型
import statsmodels.api as sm
import matplotlib.pyplot as plt
from com.xiaoda.stock.loopbacktester.utils.StockDataUtils import StockDataProcessor
from com.xiaoda.stock.loopbacktester.utils.MysqlUtils import MysqlProcessor
   
if __name__ == '__main__':


    startday='20150101'
    endday='20200101'
    
    sdProcessor=StockDataProcessor()       
    mysqlProcessor=MysqlProcessor()
    mysqlSession=mysqlProcessor.getMysqlSession()
    
    #sdict=sdProcessor.getAllStockDataDict()    
    
    sdict=sdProcessor.getHS300Dict()
    
    
    for (stockCode,scInfo) in sdict.items():
        
        stockDF=sdProcessor.getStockKData(stockCode,startday,endday,'qfq')
        stockDF.set_index('trade_date',drop=True,inplace=True)
    
        idxDF=sdProcessor.getidxData('HS300',startday,endday)
    
        '''
        #mydf_sz=ts.get_hist_data('sz',start='2017-01-01',end='2018-5-7')
        mydf_sh=ts.get_hist_data('sh',start='2017-01-01',end='2018-5-7')
        mydf_sh_md=ts.get_hist_data('000333',start='2017-01-01',end='2018-5-7')
        mydf_sh_md.p_change
        mydf_sh.p_change
        '''
        
        sh_md_merge=pd.merge(pd.DataFrame(idxDF.pct_chg),pd.DataFrame(stockDF.pct_chg),\
                             left_index=True,right_index=True,how='inner')
         
        #计算日无风险利率
        Rf_annual=0.0334#以一年期的国债利率为无风险利率
        Rf_daily=(1+Rf_annual)**(1/365)-1##年利率转化为日利率
         
        #计算风险溢价:Ri-Rf
        risk_premium=sh_md_merge-Rf_daily
        #risk_premium.head()
        
        #画出两个风险溢价的散点图，查看相关性
        #plt.scatter(risk_premium.values[:,0],risk_premium.values[:,1])
        #plt.xlabel("MD Daily Return")
        #plt.xlabel("SH Index Daily Return")   
        
        md_capm=sm.OLS(risk_premium.pct_chg_y[1:],sm.add_constant(risk_premium.pct_chg_x[1:]))
        result=md_capm.fit()
        #print(result.summary())
        #print(result.params)
        

        
        sqlStr="update u_stock_list set beta_In_5_years='%.4f' where ts_code='%s'"%(result.params[1],stockCode)
        
        mysqlProcessor.execSql(mysqlSession,sqlStr,True)
        #if result.params[1]>1.5:
        #    print("股票%s的Beta值:%.4f"%(stockCode,result.params[1]))
        #拟合结果：Rp-Rf=0.2185+1.1539(Rm-Rf)+ε
        #参数检验通过了，但是R^2不是很理想