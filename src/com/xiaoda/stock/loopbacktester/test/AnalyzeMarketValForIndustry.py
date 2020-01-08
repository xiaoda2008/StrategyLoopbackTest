'''
Created on 2020年1月3日

@author: xiaoda
'''

from com.xiaoda.stock.loopbacktester.utils.MysqlUtils import MysqlProcessor
from com.xiaoda.stock.loopbacktester.utils.StockDataUtils import StockDataProcessor
import matplotlib
from sqlalchemy.util.langhelpers import NoneType
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

def processStockMarketValPE(scList,sd,ed):
    
    mysqlProcessor=MysqlProcessor()
    #mysqlEngine=mysqlProcessor.getMysqlEngine()
    mysqlSession=mysqlProcessor.getMysqlSession()
    sdProcessor=StockDataProcessor()       
    
    for sc in scList:
        
        #已经处理过的不再进行处理
        #错误处理
        #if stockCode<=lstUpdateStockCode:
        #    continue
        
        startday=sd
        endday=ed
        #sd=sdProcessor.getCalDayByOffset(lstDealDayInDB, -365*5)
        #ed=lstDealDayInDB
        
        sql='alter table s_dailybasic_%s add avg_PE_Lst_5Years float default 0;'%(sc[:6]) 
        #MysqlProcessor.execSql(mysqlSession,sql,True)

        sql+='alter table s_dailybasic_%s add mAvg_PE_Lst_5Years float default 0;'%(sc[:6]) 
        #MysqlProcessor.execSql(mysqlSession,sql,True)

        sql+='alter table s_dailybasic_%s add Percentage_PE_Lst_5Years float default 0;'%(sc[:6]) 
        MysqlProcessor.execSql(mysqlSession,sql,True)
       
        print("开始处理股票%s的历史PE数据"%(sc))
                  
        while True:
            
            sql="SELECT\
            db.trade_date trade_date,\
            db.pe pe,\
            db.pb pb,\
            db.ps ps,\
            kd.vol vol\
            FROM\
            s_dailybasic_%s db,\
            s_kdata_%s kd \
            WHERE\
            db.trade_date = kd.trade_date \
            AND db.trade_date >= '%s' \
            and db.trade_date <= '%s' \
            ORDER BY\
            trade_date ASC;"%(sc[:6],sc[:6],startday,endday)
            
            df=mysqlProcessor.querySql(sql)
            
            #已经取不到数据了
            #说明已经到了股票最早上市的交易日
            if df.empty:
                break
        
            df.set_index('trade_date',drop=True,inplace=True)
        
            try:
                lastPE=df.at[endday,'pe']
            except:
                print("股票%s在最%s交易日停牌"%(sc,endday))
                endday=sdProcessor.getLastDealDay(endday,False)
                startday=sdProcessor.getCalDayByOffset(endday,-365*5)            
                continue
            
            df.reset_index(inplace=True)
            
            avgPE=0
            sumPE=0
            biggerCnt=0
            
            
            sumVol=0
            wSumPE=0 
            wAvgPE=0
            
            if endday=='19940425':
                pass
            idx=0
            while idx<len(df):
                if type(df.at[idx,'pe'])==NoneType or math.isnan(df.at[idx,'pe']):
                    idx+=1
                    continue
                sumPE+=df.at[idx,'pe']
                if df.at[idx,'pe']>=lastPE:
                    biggerCnt+=1
                 
                
                sumVol+=df.at[idx,'vol']
                wSumPE+=df.at[idx,'vol']*df.at[idx,'pe']
                
                idx+=1
        
            avgPE=sumPE/len(df)
            if sumVol>0:
                wAvgPE=wSumPE/sumVol
            else:
                wAvgPE=0
            
            if math.isnan(avgPE):
                avgPE=0
            if math.isnan(wAvgPE):
                wAvgPE=0
            
            
            
            #将信息计入到daily_basic中
            #增加3列：
            #wAvg_PE_Lst_5Years：过去五年交易量加权PE
            #Percentage_PE_Lst_5Years：过去五年的PE分位数（多少交易日低于当前)
            #avg_PE_Lst_5Years：过去5年平均PE
            
            sql='update s_dailybasic_%s set avg_PE_Lst_5Years=%.4f where trade_date=%s;'%(sc[:6],avgPE,endday) 
            #MysqlProcessor.execSql(mysqlSession,sql,True)
    
            sql+='update s_dailybasic_%s set mAvg_PE_Lst_5Years=%.4f where trade_date=%s;'%(sc[:6],wAvgPE,endday) 
            #MysqlProcessor.execSql(mysqlSession,sql,True)
    
            sql+='update s_dailybasic_%s set Percentage_PE_Lst_5Years=%.4f where trade_date=%s;'%(sc[:6],1-biggerCnt/len(df),endday)
            MysqlProcessor.execSql(mysqlSession,sql,True)            
            
            #print("股票%s当前PE值为：%.2f过去5年，有%.2f%%的交易日PE值低于当前"%(stockCode,lastPE,(1-biggerCnt/len(df))*100),end=',')
            #print("过去5年平均PE:%.2f，按交易量加权平均PE:%.2f"%(avgPE,wAvgPE))
            
            #print("处理完股票%s在%s的PE数据"%(sc,endday))
        
            endday=sdProcessor.getLastDealDay(endday,False)
            startday=sdProcessor.getCalDayByOffset(endday,-365*5)
            
        print("处理完股票%s的历史PE数据"%(sc))


   
if __name__ == '__main__':
    
    sdProcessor=StockDataProcessor()    
    mysqlProcessor=MysqlProcessor()
    mysqlSession=mysqlProcessor.getMysqlSession()
    

    sql="select content from u_data_desc where content_name='data_end_dealday'"
    df=mysqlProcessor.querySql(sql)    
    lstDealDayInDB=df.at[0,'content']

    #sql="select content from u_data_desc where content_name='marketval_stock_update_to'"
    #df=mysqlProcessor.querySql(sql)    
    #lstUpdateStockCode=df.at[0,'content']
    
    hs300Dict=sdProcessor.getHS300Dict()      
    sz100Dict=sdProcessor.getSZ100Dict()
    sh50Dict=sdProcessor.getSH50Dict()
    #cfRatioDict={}

    sd=sdProcessor.getCalDayByOffset(lstDealDayInDB, -365*5)
    ed=lstDealDayInDB    
    
    
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
 
    slList=get8slListFromStockList(stockCodeList)
    

    #分8个进程，分别计算8段股票历史估值情况
    process=[]

    for subStockList in slList:
        
        p=multiprocessing.Process(target=processStockMarketValPE,args=(subStockList,sd,ed))
        p.start()
        process.append(p)
    
    for p in process:
        p.join() 

        #partialUpdate(mysqlSession,stockCode)
    
    #partialUpdate(mysqlSession,'')
    
    mysqlProcessor=MysqlProcessor()
    mysqlSession=mysqlProcessor.getMysqlSession()
        
    totalUpdate(mysqlSession,lstDealDayInDB)

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
