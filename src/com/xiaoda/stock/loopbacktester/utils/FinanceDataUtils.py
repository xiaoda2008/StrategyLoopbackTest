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


    def __init__(self, params):
        '''
        Constructor
        '''   

    @staticmethod
    def getLatestStockBalanceSheet(stockCode,dateStr):
        '''
        获取指定日期前最近一次的资产负债表
        '''
        #startday=MysqlProcessor.getlastquarterfirstday().strftime('%Y%m%d')
        
        #engine = MysqlProcessor.getMysqlEngine()
        #查询语句
        sql = 'select * from s_balancesheet_%s where ann_date<=%s order by ann_date desc;'%(stockCode[:6],dateStr)
        sqltxt = sqlalchemy.text(sql)
        
        return MysqlProcessor.querySql(sqltxt)
        #查询结果
        #try:
        #    df=pandas.read_sql_query(sqltxt,engine)
        #except sqlalchemy.exc.ProgrammingError:
            #如果压根就没有这个表
            #在kdata与股票列表数据不一致的情况下会出现
        #    df=pandas.DataFrame()
        #finally:
        #    return df
        
    @staticmethod
    def getLatestStockCashFlow(stockCode,dateStr):
        '''
        获取指定日期前最近一次的现金流量表
        '''
        #startday=MysqlProcessor.getlastquarterfirstday().strftime('%Y%m%d')
        
        #engine = MysqlProcessor.getMysqlEngine()
        #查询语句
        sql = 'select * from s_cashflow_%s where ann_date<=%s order by ann_date desc;'%(stockCode[:6],dateStr)
        #查询结果
        sqltxt = sqlalchemy.text(sql)
        
        return MysqlProcessor.querySql(sqltxt)
        #查询结果
        #try:
        #    df=pandas.read_sql_query(sqltxt,engine)
        #except sqlalchemy.exc.ProgrammingError:
            #如果压根就没有这个表
            #在kdata与股票列表数据不一致的情况下会出现
        #    df=pandas.DataFrame()
        #finally:
        #    return df


    @staticmethod
    def getLatestIncome(stockCode,dateStr):
        '''
        获取指定日期前最近一次的利润表
        '''
        pass
        '''
        engine = MysqlProcessor.getMysqlEngine()
        #查询语句
        sql = 'select * from s_cashflow_%s where c_cash_equ_end_period is not null and ann_date<=%s order by ann_date desc limit 1;'%(stockCode[:6],dateStr)
        #查询结果
        sqltxt = sqlalchemy.text(sql)
        #查询结果
        try:
            df=pandas.read_sql_query(sqltxt,engine)
        except sqlalchemy.exc.ProgrammingError:
            #如果压根就没有这个表
            #在kdata与股票列表数据不一致的情况下会出现
            df=pandas.DataFrame()
        finally:
            return df
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