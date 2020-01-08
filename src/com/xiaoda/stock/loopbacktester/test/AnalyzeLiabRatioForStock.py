'''
Created on 2020年1月7日

@author: xiaoda
'''

from com.xiaoda.stock.loopbacktester.utils.MysqlUtils import MysqlProcessor
from com.xiaoda.stock.loopbacktester.utils.StockDataUtils import StockDataProcessor
import matplotlib
from sqlalchemy.util.langhelpers import NoneType
from com.xiaoda.stock.loopbacktester.utils.FinanceDataUtils import FinanceDataProcessor
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import math
import multiprocessing


def totalUpdate(mysqlSession,lastDealDay):   
    sql="update u_data_desc set content='%s' where content_name='marketval_date_update_to';"%(lastDealDay) 
    MysqlProcessor.execSql(mysqlSession,sql,True)
'''
def partialUpdate(mysqlSession,stockCode):   
    sql="update u_data_desc set content='%s' where content_name='marketval_stock_update_to';"%(stockCode) 
    MysqlProcessor.execSql(mysqlSession,sql,True)
''' 

def get8slListFromStockList(stockList):
    
    retSLList=[]
    slLen=len(stockList)
    #对所有股票进行遍历
    i=0
    tmpList1=[]
    tmpList2=[]
    tmpList3=[]
    tmpList4=[]
    tmpList5=[]
    tmpList6=[]
    tmpList7=[]
    tmpList8=[]
    
    for stockCode in stockList:
        #log.logger.info(stockCode)
        if i<slLen*1/8:
            tmpList1.append(stockCode)
        elif i>=slLen*1/8 and i<slLen*2/8:
            tmpList2.append(stockCode)
        elif i>=slLen*2/8 and i<slLen*3/8:
            tmpList3.append(stockCode)
        elif i>=slLen*3/8 and i<slLen*4/8:
            tmpList4.append(stockCode)
        elif i>=slLen*4/8 and i<slLen*5/8:
            tmpList5.append(stockCode)
        elif i>=slLen*5/8 and i<slLen*6/8:
            tmpList6.append(stockCode)
        elif i>=slLen*6/8 and i<slLen*7/8:
            tmpList7.append(stockCode)
        else:
            tmpList8.append(stockCode)            
        i=i+1
    retSLList.append(tmpList1)
    retSLList.append(tmpList2)
    retSLList.append(tmpList3)
    retSLList.append(tmpList4)
    retSLList.append(tmpList5)
    retSLList.append(tmpList6)
    retSLList.append(tmpList7)
    retSLList.append(tmpList8)
                
    return retSLList 

def processStockLiabRatio(scList,sd):
    
    mysqlProcessor=MysqlProcessor()
    #mysqlEngine=mysqlProcessor.getMysqlEngine()
    mysqlSession=mysqlProcessor.getMysqlSession()
    sdProcessor=StockDataProcessor()       
    finProcessor=FinanceDataProcessor()
     
    for stockCode in scList:
       
        #print("开始处理股票%s的负债率数据"%(stockCode))

        sData=sdProcessor.getStockInfo(stockCode)
        
        ind=sData.at[0,'industry']
        if ind in ['证券','银行']:
            continue
        
        bs=finProcessor.getLatestBalanceSheetReport(stockCode,sd)
        #bs为所有之前发布的所有资产负债表数据             
        #ic=finProcessor.getLatestIncomeReport(stockCode,sd)
        #ic为之前发布的所有利润表数据
        #获取现金流量表中，现金等价物总数
        #cf=finProcessor.getLatestCashFlowReport(stockCode,sd)
        #cf为之前发布的所有现金流量表数据
        
        #货币资金
        moneyCap=bs[bs['money_cap'].notnull()].reset_index(drop=True).at[0,'money_cap']
        
        
        #短期借款
        if bs[bs['st_borr'].notnull()].empty:
            stBorr=0
        else:
            stBorr=bs[bs['st_borr'].notnull()].reset_index(drop=True).at[0,'st_borr']
        
        #长期借款
        if bs[bs['lt_borr'].notnull()].empty:
            ltBorr=0
        else:
            ltBorr=bs[bs['lt_borr'].notnull()].reset_index(drop=True).at[0,'lt_borr']
        
        #应付债券
        if bs[bs['bond_payable'].notnull()].empty:
            bondPayable=0
        else:
            bondPayable=bs[bs['bond_payable'].notnull()].reset_index(drop=True).at[0,'bond_payable']
        
        #总资产
        totalAsset=bs[bs['total_assets'].notnull()].reset_index(drop=True).at[0,'total_assets']
        
        cashRatio=moneyCap/totalAsset
        
        liabRatio=(stBorr+ltBorr+bondPayable)/moneyCap
        
        if liabRatio>0.5 and cashRatio>0.25:
            print("股票%s的现金高，且负债高,现金占比:%.4f,负债占比:%.4f"%(stockCode,cashRatio,liabRatio))
        
        #print("处理完股票%s的历史PE数据"%(sc))


if __name__ == '__main__':

    sdProcessor=StockDataProcessor()    
    mysqlProcessor=MysqlProcessor()
    mysqlSession=mysqlProcessor.getMysqlSession()
    

    #sql="select content from u_data_desc where content_name='data_end_dealday'"
    #df=mysqlProcessor.querySql(sql)    
    #lstDealDayInDB=df.at[0,'content']

    #sql="select content from u_data_desc where content_name='marketval_stock_update_to'"
    #df=mysqlProcessor.querySql(sql)    
    #lstUpdateStockCode=df.at[0,'content']
    
    hs300Dict=sdProcessor.getHS300Dict()      
    sz100Dict=sdProcessor.getSZ100Dict()
    sh50Dict=sdProcessor.getSH50Dict()
    #cfRatioDict={}

    #sd=sdProcessor.getCalDayByOffset(lstDealDayInDB, -365*5)
    
    #ed=lstDealDayInDB
    
    sd='20191231'
    
    hs300List=[]
    for (stockCode,scdict) in hs300Dict.items():
        hs300List.append(stockCode)
    
    sh50List=[]
    for (stockCode,scdict) in sh50Dict.items():
        sh50List.append(stockCode)
            
    sz100List=[]
    for (stockCode,scdict) in sz100Dict.items():
        sz100List.append(stockCode)
    
        
    #stockCodeList=list(set(sz100List).difference(set(hs300List))) # b中有而a中没有的
 
    stockCodeList=list(set(set(sz100List).union(set(hs300List))).union(set(sh50List)))


    processStockLiabRatio(stockCodeList,sd)
    
    ''' 
    slList=get8slListFromStockList(stockCodeList)
    

    #分8个进程，分别计算8段股票历史估值情况
    process=[]

    for subStockList in slList:
        
        p=multiprocessing.Process(target=processStockLiabRatio,args=(subStockList,sd))
        p.start()
        process.append(p)
    
    for p in process:
        p.join() 

        #partialUpdate(mysqlSession,stockCode)
    '''
    
    #partialUpdate(mysqlSession,'')


'''
df.drop(columns=['vol'],inplace=True)

df.set_index('trade_date',drop=True,inplace=True)

df.plot()
plt.grid(True)  
plt.savefig(r'd:/outputdir/PEAnalyze/%s.png'%(stockCode[:6]))
plt.clf()
plt.cla()
plt.close("all")
'''
