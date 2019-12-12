'''
Created on 2019年11月18日

@author: xiaoda
'''
import pandas
import sqlalchemy
from com.xiaoda.stock.loopbacktester.utils.MysqlUtils import MysqlProcessor

class FinanceDataProcessor(object):
    '''
    classdocs
    '''
    
    '''
    def getlasthalffirstday():
        today=dt.now()
        quarter = (today.month-1)/3+1
        if quarter == 1:
            return dt(today.year-1,10,1)
        elif quarter == 2:
            return dt(today.year,1,1)
        elif quarter == 3:
            return dt(today.year,4,1)
        else:
            return dt(today.year,7,1)
    '''
    
    def __init__(self):
        '''
        Constructor
        ''' 
        self.mysqlProcessor=MysqlProcessor()  


    def getLatestStockBalanceSheetReport(self,stockCode,dateStr):
        '''
        获取指定日期前最近一次的资产负债表
        '''
        #查询语句
        sql = 'select * from s_balancesheet_%s where ann_date<=%s order by ann_date desc;'%(stockCode[:6],dateStr)
        return self.mysqlProcessor.querySql(sql)
        #查询结果
        #try:
        #    df=pandas.read_sql_query(sqltxt,engine)
        #except sqlalchemy.exc.ProgrammingError:
            #如果压根就没有这个表
            #在kdata与股票列表数据不一致的情况下会出现
        #    df=pandas.DataFrame()
        #finally:
        #    return df
        
    def getLatestStockCashFlowReport(self,stockCode,dateStr):
        '''
        获取指定日期前最近一次的现金流量表
        '''
        #查询语句
        sql = 'select * from s_cashflow_%s where ann_date<=%s order by ann_date desc;'%(stockCode[:6],dateStr)
        return self.mysqlProcessor.querySql(sql)
        #查询结果
        #try:
        #    df=pandas.read_sql_query(sqltxt,engine)
        #except sqlalchemy.exc.ProgrammingError:
            #如果压根就没有这个表
            #在kdata与股票列表数据不一致的情况下会出现
        #    df=pandas.DataFrame()
        #finally:
        #    return df

    def getLatestIncomeReport(self,stockCode,dateStr):
        '''
        获取指定日期前最近一次的利润表
        '''
        #查询语句
        sql='select * from s_income_%s where ann_date<=%s order by ann_date desc;'%(stockCode[:6],dateStr)
        return self.mysqlProcessor.querySql(sql)


    def getLatestAnnualOrSemiReportEBIT(self):
        '''
        获取最近一次的半年度或年度报表的EBIT
        '''
        #查询语句
        sql="select * from s_income_000001 where end_date like '%0630' or end_date like '%1231' order by end_date desc;"
        aosr=self.mysqlProcessor.querySql(sql)
#（营业总收入-营业税金及附加）-（营业成本+利息支出+手续费及佣金支出+销售费用+管理费用+研发费用+坏账损失+存货跌价损失）+其他收益

#营业总收入

#营业税金及附加

#营业成本

#利息支出

#手续费及佣金支出

#销售费用

#管理费用

#研发费用

#坏账损失

#存货跌价损失

#其他收益（投资收益)

    

        '''
                today=dt.now()
                quarter = (today.month-1)/3+1
                if quarter == 1:
                    return dt(today.year-1,10,1)
                elif quarter == 2:
                    return dt(today.year,1,1)
                elif quarter == 3:
                    return dt(today.year,4,1)
                else:
                    return dt(today.year,7,1)
        '''
 
        '''
        # 创建对象的基类:
        Base = declarative_base()
        
        # 定义StockKData对象:
        class StockKData(Base):
                
            # 表的名字:
            __tablename__ = 's_kdata_'+stockCode

            # 表的结构:
            ts_code=Column(String(20))
            trade_date=Column(String(20),primary_key=True)
            open=Column(Float)
            high=Column(Float)
            low=Column(Float)
            close=Column(Float)
            pre_close=Column(Float)
            change=Column(Float)
            pct_chg=Column(Float)
            vol=Column(Integer)
            amount=Column(Integer)
        
        
        # 创建DBSession类型:
        DBSession = sessionmaker(bind=engine)
        
        # 创建session对象:
        session = DBSession()
        
        stockKData = session.query(StockKData).filter(and_(StockKData.trade_date>=startDate,StockKData.trade_date<=endDate)).all()

        # 关闭Session:
        session.close()
        
        #stockKData为List，需要转换为DataFram返回，以适应数据处理逻辑
        if len(stockKData)==0:
            return None
        else:
            sData=DataFrame(stockKData)
            return sData
    ''' 
   
    '''
    @staticmethod
    def getlastquarterfirstday(dateStr):
        #today=dt.now()
        quarter = (dateStr[4:6]-1)/3+1
        if quarter == 1:
            return dt(dateStr[0:4]-1,10,1)
        elif quarter == 2:
            return dt(dateStr[0:4],1,1)
        elif quarter == 3:
            return dt(dateStr[0:4],4,1)
        else:
            return dt(dateStr[0:4],7,1)
    '''