'''
Created on 2019年11月2日

@author: xiaoda

一次性的获取所有需要的数据到本地数据库中
并在过程中进行处理情况的记录，以便kettle进行错误处理
'''

import sys
import os
curPath = os.path.abspath(os.path.dirname(__file__))
rootPath = os.path.split(curPath)[0]

#print(sys.path)
#sys.path.clear()
#print(sys.path)
sys.path.append(r'E:\eclipse-workspace\StrategyLoopbackTest\src')
sys.path.append(r'D:\Program Files\Python38\Lib\site-packages')
sys.path.append(r'D:\Program Files\Python38')
sys.path.append(r'D:\Program Files\Python38\DLLs')
sys.path.append(r'D:\Program Files\Python38\libs')
sys.path.append(r'C:\Users\picc\eclipse-workspace\StrategyLoopbackTester\src')
sys.path.append(r'C:\Users\picc\AppData\Local\Programs\Python\Python36-32\Lib\site-packages')
sys.path.append(r'C:\Users\picc\AppData\Local\Programs\Python\Python36-32')



from com.xiaoda.stock.loopbacktester.utils.StockDataUtils import StockDataProcessor
import tushare
import sqlalchemy
from com.xiaoda.stock.loopbacktester.utils.LoggingUtils import Logger
from com.xiaoda.stock.loopbacktester.utils.MysqlUtils import MysqlProcessor
#from com.xiaoda.stock.loopbacktester.utils.ParamUtils import LOGGINGDIR
from datetime import datetime as dt


mysqlProcessor=MysqlProcessor()

import argparse

DAYONE='19900101'
log = Logger(os.path.split(__file__)[-1].split(".")[0]+'.log',level='info')


'''
def execSqlFile(file,cmtFlg=True):
    with open(file,'r+',encoding='UTF-8') as f:
        sql_list=f.read().split('\n')[:-1]  # sql文件最后一行加上;
        #sql_list=[x.replace('\n', '') if '\n' in x else x for x in sql_list]  # 将每段sql里的换行符改成空格
    ##执行sql语句，使用循环执行sql语句
    for sql_item in sql_list:
        if len(sql_item)==0:
            continue
        MysqlProcessor.execSql(mysqlSession,sql_item,cmtFlg)
'''

def getlastquarterfirstday():
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
    

def partialUpdate(mysqlSession):    
    #部分更新语句
    pupdatesql="update u_data_desc set content='%s' where content_name='last_update_time';"%(dt.now().strftime('%Y%m%d %H:%M:%S'))
    MysqlProcessor.execSql(mysqlSession,pupdatesql,True)

def totalUpdate(mysqlSession,sdProcessor):
    #全局更新语句
    tupdatesql="update u_data_desc set content='%s' where content_name='last_total_update_time';"%(dt.now().strftime('%Y%m%d %H:%M:%S'))
    MysqlProcessor.execSql(mysqlSession,tupdatesql,False)
    #从sd往后找到第一个交易日，含sd
    tupdatesql="update u_data_desc set content='%s' where content_name='data_start_dealday';"%(sdProcessor.getNextDealDay(sd, True))
    MysqlProcessor.execSql(mysqlSession,tupdatesql,False)
    #从ed往前找到最后一个交易日，是否含ed需要根据当前时间是否已经完成该日交易
    #最好是每天取前一个交易日的数据，这样就不会存在当天日期是否已经可用的问题
    tupdatesql="update u_data_desc set content='%s' where content_name='data_end_dealday';"%(sdProcessor.getLastDealDay(ed,True))
    MysqlProcessor.execSql(mysqlSession,tupdatesql,False)
    mysqlSession.commit()
    

def lastDataUpdate(mysqlSession,stockCode,dataType):
    if dataType=='FR_StockCode':
        #更新财务报表最新股票代码
        sql="update u_data_desc set content='%s' where content_name='finance_report_stockcode_update_to';"%(stockCode)
    elif dataType=='FR_Date':
        sql="update u_data_desc set content='%s' where content_name='finance_report_date_update_to';"%(dt.now().strftime('%Y%m%d'))
    elif dataType=="KD":
        #更新K线最新股票代码
        sql="update u_data_desc set content='%s' where content_name='kdata_update_to';"%(stockCode)
    elif dataType=='DB':
        #更新股票每日基本数据
        sql="update u_data_desc set content='%s' where content_name='dailybasic_update_to';"%(stockCode)
    elif dataType=="ADJ":
        #更新复权因子最新股票代码
        sql="update u_data_desc set content='%s' where content_name='adjdata_update_to';"%(stockCode)
    
    else:
        raise Exception("dataType不正确")
    
    MysqlProcessor.execSql(mysqlSession,sql,True)



if __name__ == '__main__':
    
    #使用TuShare pro版本
    
    mysqlProcessor=MysqlProcessor()
    
    #写入数据库的引擎
    mysqlEngine = mysqlProcessor.getMysqlEngine()
    mysqlSession=mysqlProcessor.getMysqlSession()
    
    
    tushare.set_token('221f96cece132551e42922af6004a622404ae812e41a3fe175391df8')

    sdDataAPI=tushare.pro_api()
    
    
    #1、获取交易日信息，并存入数据库
    startday=DAYONE
    trade_cal_data=sdDataAPI.trade_cal(exchange='',start_date=startday,end_date=dt.now().strftime('%Y%m%d'),fields='exchange,cal_date,is_open,pretrade_date')
    #将交易日列表存入数据库表中
    trade_cal_data.to_sql(name='u_trade_cal',con=mysqlEngine,chunksize=1000,if_exists='replace',index=None)
    #完成部分信息更新
    partialUpdate(mysqlSession)
    
    
    #2、获取股票列表并存入数据库
    sdf=sdDataAPI.stock_basic(exchange='',list_status='L',fields='ts_code,symbol,name,area,industry,list_date')
 
    for sidx in sdf.index:
        valStr=sdf[sidx:sidx+1].to_csv(index=False,header=False,sep=",",na_rep='NULL').replace('\n','').replace('\r','')
        valList=valStr.split(',')
        paramStr=''
        for val in valList:
            if val=='NULL':
                paramStr=paramStr+'NULL,'
            else:       
                paramStr=paramStr+"'"+val+"'"+','
        paramStr=paramStr[:-1]
        sql='insert ignore into u_stock_list values (%s)'%(paramStr)
        MysqlProcessor.execSql(mysqlSession,sql,False)
    
    stockCodeList=sdf['ts_code']
    #完成部分信息更新
    #会在partialUpdate时一并commit，不用单独调用commit
    partialUpdate(mysqlSession)
    
    
    
    sdProcessor=StockDataProcessor()
    
    # 创建命令行解析器句柄，并自定义描述信息
    parser=argparse.ArgumentParser(description="enter the params")

    #开始日期为19900101，即取全量数据
    parser.add_argument("--startdate","-sd",type=str, default='19911219',help="Enter the start date")
    # 定义可选参数module1
    #结束日期默认为当前日期的前一个交易日（不含当天，以便解决当天可能还未完成交易的问题）
    parser.add_argument("--enddate","-ed",type=str, default=sdProcessor.getLastDealDay(dt.now().strftime('%Y%m%d'),False),help="Enter the end date")
    args=parser.parse_args()  # 返回一个命名空间
    params=vars(args)  # 返回 args 的属性和属性值的字典
    v1=[]
    
    sd=''
    ed=''
    
    for k, v in params.items():
        if k=='startdate':
            sd=v
        elif k=='enddate':
            if v>=dt.now().strftime('%Y%m%d'):
                #调整一下日期，如果输入的结束日期等于或晚于当天
                #则取当天之前的一个交易日
                v=StockDataProcessor.getLastDealDay(dt.now().strftime('%Y%m%d'),False)
            ed=v
    
    startday=sd
    endday=ed
    
    
    
    #3、获取财务报表数据，存入数据库
    #startday=getlastquarterfirstday().strftime('%Y%m%d')
    
    #找到之前处理的最后一个股票的代码
    sql="select content from u_data_desc where content_name='finance_report_stockcode_update_to'"
    res=mysqlProcessor.querySql(sql)
    try:
        finance_report_update_to=res.at[0,'content']
    except:
        finance_report_update_to=''
    
    for index,stockCode in stockCodeList.items():
    
        if stockCode<=finance_report_update_to:
            continue
        
        #获取资产负债表
        bs=sdDataAPI.balancesheet(ts_code=stockCode,start_date=startday,end_date=endday,fields=
            'ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,total_share,cap_rese,\
            undistr_porfit,surplus_rese,special_rese,money_cap,trad_asset,notes_receiv,accounts_receiv,\
            oth_receiv,prepayment,div_receiv,int_receiv,inventories,amor_exp,nca_within_1y,sett_rsrv,\
            loanto_oth_bank_fi,premium_receiv,reinsur_receiv,reinsur_res_receiv,pur_resale_fa,oth_cur_assets,\
            total_cur_assets,fa_avail_for_sale,htm_invest,lt_eqt_invest,invest_real_estate,time_deposits,\
            oth_assets,lt_rec,fix_assets,cip,const_materials,fixed_assets_disp,produc_bio_assets,\
            oil_and_gas_assets,intan_assets,r_and_d,goodwill,lt_amor_exp,defer_tax_assets,\
            decr_in_disbur,oth_nca,total_nca,cash_reser_cb,depos_in_oth_bfi,prec_metals,\
            deriv_assets,rr_reins_une_prem,rr_reins_outstd_cla,rr_reins_lins_liab,rr_reins_lthins_liab,\
            refund_depos,ph_pledge_loans,refund_cap_depos,indep_acct_assets,client_depos,client_prov,\
            transac_seat_fee,invest_as_receiv,total_assets,lt_borr,st_borr,cb_borr,depos_ib_deposits,\
            loan_oth_bank,trading_fl,notes_payable,acct_payable,adv_receipts,sold_for_repur_fa,\
            comm_payable,payroll_payable,taxes_payable,int_payable,div_payable,oth_payable,\
            acc_exp,deferred_inc,st_bonds_payable,payable_to_reinsurer,rsrv_insur_cont,\
            acting_trading_sec,acting_uw_sec,non_cur_liab_due_1y,oth_cur_liab,total_cur_liab,\
            bond_payable,lt_payable,specific_payables,estimated_liab,defer_tax_liab,\
            defer_inc_non_cur_liab,oth_ncl,total_ncl,depos_oth_bfi,deriv_liab,depos,\
            agency_bus_liab,oth_liab,prem_receiv_adva,depos_received,ph_invest,reser_une_prem,\
            reser_outstd_claims,reser_lins_liab,reser_lthins_liab,indept_acc_liab,pledge_borr,\
            indem_payable,policy_div_payable,total_liab,treasury_share,ordin_risk_reser,\
            forex_differ,invest_loss_unconf,minority_int,total_hldr_eqy_exc_min_int,\
            total_hldr_eqy_inc_min_int,total_liab_hldr_eqy,lt_payroll_payable,oth_comp_income,\
            oth_eqt_tools,oth_eqt_tools_p_shr,lending_funds,acc_receivable,st_fin_payable,\
            payables,hfs_assets,hfs_sales,update_flag')
        
        try:
            bs.to_sql(name='s_fin_report_balancesheet',con=mysqlEngine,chunksize=1000,if_exists='append',index=None)
        except sqlalchemy.exc.IntegrityError:
            log.logger.warning("此前处理过该股票%s的balanceSheet"%(stockCode))
            
        #获取现金流量表
        cf = sdDataAPI.cashflow(ts_code=stockCode,start_date=startday,end_date=endday,fields=
            'ts_code,ann_date,f_ann_date,end_date,comp_type,report_type,net_profit,finan_exp,\
            c_fr_sale_sg,recp_tax_rends,n_depos_incr_fi,n_incr_loans_cb,n_inc_borr_oth_fi,\
            prem_fr_orig_contr,n_incr_insured_dep,n_reinsur_prem,n_incr_disp_tfa,ifc_cash_incr,\
            n_incr_disp_faas,n_incr_loans_oth_bank,n_cap_incr_repur,c_fr_oth_operate_a,\
            c_inf_fr_operate_a,c_paid_goods_s,c_paid_to_for_empl,c_paid_for_taxes,\
            n_incr_clt_loan_adv,n_incr_dep_cbob,c_pay_claims_orig_inco,pay_handling_chrg,\
            pay_comm_insur_plcy,oth_cash_pay_oper_act,st_cash_out_act,n_cashflow_act,\
            oth_recp_ral_inv_act,c_disp_withdrwl_invest,c_recp_return_invest,\
            n_recp_disp_fiolta, n_recp_disp_sobu,stot_inflows_inv_act,c_pay_acq_const_fiolta,\
            c_paid_invest,n_disp_subs_oth_biz,oth_pay_ral_inv_act,n_incr_pledge_loan,\
            stot_out_inv_act,n_cashflow_inv_act,c_recp_borrow,proc_issue_bonds,\
            oth_cash_recp_ral_fnc_act,stot_cash_in_fnc_act,free_cashflow,c_prepay_amt_borr,\
            c_pay_dist_dpcp_int_exp,incl_dvd_profit_paid_sc_ms,oth_cashpay_ral_fnc_act,\
            stot_cashout_fnc_act,n_cash_flows_fnc_act,eff_fx_flu_cash,n_incr_cash_cash_equ,\
            c_cash_equ_beg_period,c_cash_equ_end_period,c_recp_cap_contrib,incl_cash_rec_saims,\
            uncon_invest_loss,prov_depr_assets,depr_fa_coga_dpba,amort_intang_assets,\
            lt_amort_deferred_exp,decr_deferred_exp,incr_acc_exp,loss_disp_fiolta,\
            loss_scr_fa,loss_fv_chg,invest_loss,decr_def_inc_tax_assets,incr_def_inc_tax_liab,\
            decr_inventories,decr_oper_payable,incr_oper_payable,others,im_net_cashflow_oper_act,\
            conv_debt_into_cap,conv_copbonds_due_within_1y,fa_fnc_leases,end_bal_cash,\
            beg_bal_cash,end_bal_cash_equ,beg_bal_cash_equ,im_n_incr_cash_equ,update_flag')
        
        try:
            cf.to_sql(name='s_fin_report_cashflow',con=mysqlEngine,chunksize=1000,if_exists='append',index=None)
        except sqlalchemy.exc.IntegrityError:
            log.logger.warning("此前处理过该股票%s的cashflow"%(stockCode))
            
        #获取利润表
        ic = sdDataAPI.income(ts_code=stockCode,start_date=startday,end_date=endday,fields=
            'ts_code,ann_date,f_ann_date,end_date,report_type,comp_type,basic_eps,diluted_eps,\
            total_revenue,revenue,int_income,prem_earned,comm_income,n_commis_income,n_oth_income,\
            n_oth_b_income,prem_income,out_prem,une_prem_reser,reins_income,n_sec_tb_income,\
            n_sec_uw_income,n_asset_mg_income,oth_b_income,fv_value_chg_gain,invest_income,\
            ass_invest_income,forex_gain,total_cogs,oper_cost,int_exp,comm_exp,biz_tax_surchg,\
            sell_exp,admin_exp,fin_exp,assets_impair_loss,prem_refund,compens_payout,\
            reser_insur_liab,div_payt,reins_exp,oper_exp,compens_payout_refu,insur_reser_refu,\
            reins_cost_refund,other_bus_cost,operate_profit,non_oper_income,non_oper_exp,\
            nca_disploss,total_profit,income_tax,n_income,n_income_attr_p,minority_gain,\
            oth_compr_income,t_compr_income,compr_inc_attr_p,compr_inc_attr_m_s,\
            ebit,ebitda,insurance_exp,undist_profit,distable_profit,update_flag')
        
        try:
            ic.to_sql(name='s_fin_report_income',con=mysqlEngine,chunksize=1000,if_exists='append',index=None)
        except sqlalchemy.exc.IntegrityError:
            log.logger.warning("此前处理过该股票%s的income"%(stockCode))
        #
        #mysqlSession.commit()
        log.logger.info('处理完%s的财务报表数据'%(stockCode))
        #会在partialUpdate时一并commit，可以不用单独调用commit
        partialUpdate(mysqlSession)
        lastDataUpdate(mysqlSession,stockCode, "FR_StockCode")
        
        #time.sleep(0.9)
    
    lastDataUpdate(mysqlSession,stockCode, "FR_Date")    

    
    #4、获取股票不复权日K线数据，并存入数据库
    
    #找到之前处理的最后一个股票的代码
    sql="select content from u_data_desc where content_name='kdata_update_to'"
    res=mysqlProcessor.querySql(sql)
    kdata_update_to=res.at[0,'content']
    
    for index,stockCode in stockCodeList.items():
        
        if stockCode<=kdata_update_to:
            continue
        
        #将1990-01-01到19999-12-31该股票数据导入数据库
        if startday>'19991231' or endday<'19900101':
            pass
        else:
            if startday<'19900101':
                tmpSTARTDATE='19900101'
            else:
                tmpSTARTDATE=startday
            if endday>'19991231':
                tmpENDDATE='19991231'
            else:
                tmpENDDATE=endday
                
            #获取该股票数据并写入数据库
            stock_k_data=tushare.pro_bar(ts_code=stockCode,start_date=tmpSTARTDATE,end_date=tmpENDDATE)
            
            if stock_k_data.empty:
                #如果没有任何返回值，说明该时间段内没有上市交易过该股票
                log.logger.warning('%s在%s到%s时段内无交易'%(stockCode,tmpSTARTDATE,tmpENDDATE))
            else:
                stock_k_data.sort_index(inplace=True,ascending=False)
                #存入数据库
                try:
                    stock_k_data.to_sql(name='s_k_data', con=mysqlEngine, chunksize=1000, if_exists='append', index=None)
                except sqlalchemy.exc.IntegrityError:
                    log.logger.warning("此前处理过该股票%s在%s到%s区间的kdata"%(stockCode,tmpSTARTDATE,tmpENDDATE))


        #将2000-01-01到2009-12-31该股票数据导入数据库
        if startday>'20091231' or endday<'20000101':
            pass
        else:
            if startday<'20000101':
                tmpSTARTDATE='20000101'
            else:
                tmpSTARTDATE=startday
            if endday>'20091231':
                tmpENDDATE='20091231'
            else:
                tmpENDDATE=endday
                
            #获取该股票数据并写入数据库
            stock_k_data = tushare.pro_bar(ts_code=stockCode, start_date=tmpSTARTDATE, end_date=tmpENDDATE)
    
            if stock_k_data.empty:
                #如果没有任何返回值，说明该时间段内没有上市交易过该股票
                log.logger.warning('%s在%s到%s时段内无交易'%(stockCode,tmpSTARTDATE,tmpENDDATE))
            else:
                
                stock_k_data.sort_index(inplace=True,ascending=False)
                #存入数据库
                try:  
                    stock_k_data.to_sql(name='s_k_data', con=mysqlEngine, chunksize=1000, if_exists='append', index=None)
                except sqlalchemy.exc.IntegrityError:
                    log.logger.warning("此前处理过该股票%s在%s到%s区间的kdata"%(stockCode,tmpSTARTDATE,tmpENDDATE))
                    
        #将2010-01-01到2018-12-31该股票数据导入数据库
        if startday>'20181231' or endday<'20100101':
            pass
        else:
            if startday<'20100101':
                tmpSTARTDATE='20100101'
            else:
                tmpSTARTDATE=startday
            if endday>'20181231':
                tmpENDDATE='20181231'
            else:
                tmpENDDATE=endday
        
            #获取该股票数据并写入数据库
            stock_k_data=tushare.pro_bar(ts_code=stockCode,start_date=tmpSTARTDATE,end_date=tmpENDDATE)
              
            if stock_k_data.empty:
                #如果没有任何返回值，说明该时间段内没有上市交易过该股票
                log.logger.warning('%s在%s到%s时段内无交易'%(stockCode,tmpSTARTDATE,tmpENDDATE))
            else:
                stock_k_data.sort_index(inplace=True,ascending=False)
                #存入数据库
                try:
                    stock_k_data.to_sql(name='s_k_data', con=mysqlEngine, chunksize=1000, if_exists='append', index=None)
                except sqlalchemy.exc.IntegrityError:
                    log.logger.warning("此前处理过该股票%s在%s到%s区间的kdata"%(stockCode,tmpSTARTDATE,tmpENDDATE))

                
        #将2019-01-01到当前该股票数据导入数据库
        if startday>dt.now().strftime('%Y%m%d') or endday<'20190101':
            pass
        else:
            if startday<'20190101':
                tmpSTARTDATE='20190101'
            else:
                tmpSTARTDATE=startday
            if endday>dt.now().strftime('%Y%m%d'):
                tmpENDDATE=dt.now().strftime('%Y%m%d')
            else:
                tmpENDDATE=endday
                
            #获取该股票数据并写入数据库
            stock_k_data=tushare.pro_bar(ts_code=stockCode,start_date=tmpSTARTDATE,end_date=tmpENDDATE)
       
            if stock_k_data.empty:
                #如果没有任何返回值，说明该时间段内没有上市交易过该股票
                log.logger.warning('%s在%s到%s时段内无交易'%(stockCode,tmpSTARTDATE,tmpENDDATE))
            else:
                stock_k_data.sort_index(inplace=True,ascending=False)
                #存入数据库

                try:
                    stock_k_data.to_sql(name='s_k_data', con=mysqlEngine, chunksize=1000, if_exists='append', index=None)
                except sqlalchemy.exc.IntegrityError:
                    log.logger.warning("此前处理过该股票%s在%s到%s区间的kdata"%(stockCode,tmpSTARTDATE,tmpENDDATE))
                
        partialUpdate(mysqlSession)
        lastDataUpdate(mysqlSession,stockCode,"KD")               
        log.logger.info('处理完股票%s的k线数据'%(stockCode))

            
        #time.sleep(0.9)


    #5、获取股票每日指标数据，并存入数据库
    
    #找到之前处理的最后一个股票的代码
    sql="select content from u_data_desc where content_name='dailybasic_update_to'"
    res=mysqlProcessor.querySql(sql)
    dailybasic_update_to=res.at[0,'content']
    
    for index,stockCode in stockCodeList.items():
        
        if stockCode<=dailybasic_update_to:
            continue

        
        #将1990-01-01到1999-12-31该股票数据导入数据库
        if startday>'19991231' or endday<'19900101':
            pass
        else:
            if startday<'19900101':
                tmpSTARTDATE='19900101'
            else:
                tmpSTARTDATE=startday
            if endday>'19991231':
                tmpENDDATE='19991231'
            else:
                tmpENDDATE=endday
            
            #获取该股票数据并写入数据库
            daily_basic_data=sdDataAPI.daily_basic(ts_code=stockCode,start_date=tmpSTARTDATE,end_date=tmpENDDATE)
            
            if daily_basic_data.empty:
                #如果没有任何返回值，说明该时间段内没有上市交易过该股票
                log.logger.warning('%s在%s到%s时段内无每日数据'%(stockCode,tmpSTARTDATE,tmpENDDATE))
            else:
                daily_basic_data.sort_index(inplace=True,ascending=False)
                #存入数据库
                try:
                    daily_basic_data.to_sql(name='s_daily_basic', con=mysqlEngine, chunksize=1000, if_exists='append', index=None)
                except sqlalchemy.exc.IntegrityError:
                    log.logger.warning("此前处理过该股票%s在%s到%s区间的dailybasic"%(stockCode,tmpSTARTDATE,tmpENDDATE))


        #将2000-01-01到2009-12-31该股票数据导入数据库
        if startday>'20091231' or endday<'20000101':
            pass
        else:
            if startday<'20000101':
                tmpSTARTDATE='20000101'
            else:
                tmpSTARTDATE=startday
            if endday>'20091231':
                tmpENDDATE='20091231'
            else:
                tmpENDDATE=endday
                     
            #获取该股票数据并写入数据库
            daily_basic_data=sdDataAPI.daily_basic(ts_code=stockCode,start_date=tmpSTARTDATE,end_date=tmpENDDATE)
    
            if daily_basic_data.empty:
                #如果没有任何返回值，说明该时间段内没有上市交易过该股票
                log.logger.warning('%s在%s到%s时段内无每日数据'%(stockCode,tmpSTARTDATE,tmpENDDATE))
            else:
                daily_basic_data.sort_index(inplace=True,ascending=False)
                #存入数据库
                try:
                    daily_basic_data.to_sql(name='s_daily_basic', con=mysqlEngine, chunksize=1000, if_exists='append', index=None)
                except sqlalchemy.exc.IntegrityError:
                    log.logger.warning("此前处理过该股票%s在%s到%s区间的dailybasic"%(stockCode,tmpSTARTDATE,tmpENDDATE))


        #将2010-01-01到2018-12-31该股票数据导入数据库
        if startday>'20181231' or endday<'20100101':
            pass
        else:
            if startday<'20100101':
                tmpSTARTDATE='20100101'
            else:
                tmpSTARTDATE=startday
            if endday>'20181231':
                tmpENDDATE='20181231'
            else:
                tmpENDDATE=endday
        
            #获取该股票数据并写入数据库
            daily_basic_data=sdDataAPI.daily_basic(ts_code=stockCode,start_date=tmpSTARTDATE,end_date=tmpENDDATE)
            
            if daily_basic_data.empty:
                #如果没有任何返回值，说明该时间段内没有上市交易过该股票
                log.logger.warning('%s在%s到%s时段内无每日数据'%(stockCode,tmpSTARTDATE,tmpENDDATE))
            else:
                daily_basic_data.sort_index(inplace=True,ascending=False)
                #存入数据库
                try:
                    daily_basic_data.to_sql(name='s_daily_basic', con=mysqlEngine, chunksize=1000, if_exists='append', index=None)
                except sqlalchemy.exc.IntegrityError:
                    log.logger.warning("此前处理过该股票%s在%s到%s区间的dailybasic"%(stockCode,tmpSTARTDATE,tmpENDDATE))

        #将2019-01-01以后该股票数据导入数据库
        if startday>dt.now().strftime('%Y%m%d') or endday<'20190101':
            pass
        else:
            if startday<'20190101':
                tmpSTARTDATE='20190101'
            else:
                tmpSTARTDATE=startday
            if endday>dt.now().strftime('%Y%m%d'):
                tmpENDDATE=dt.now().strftime('%Y%m%d')
            else:
                tmpENDDATE=endday
            
            #获取该股票数据并写入数据库
            daily_basic_data=sdDataAPI.daily_basic(ts_code=stockCode,start_date=tmpSTARTDATE,end_date=tmpENDDATE)
       
            if daily_basic_data.empty:
                #如果没有任何返回值，说明该时间段内没有上市交易过该股票
                log.logger.warning('%s在%s到%s时段内无每日数据'%(stockCode,tmpSTARTDATE,tmpENDDATE))
    
            else:
                daily_basic_data.sort_index(inplace=True,ascending=False)
                #存入数据库
                try:
                    daily_basic_data.to_sql(name='s_daily_basic', con=mysqlEngine, chunksize=1000, if_exists='append', index=None)
                except sqlalchemy.exc.IntegrityError:
                    log.logger.warning("此前处理过该股票%s在%s到%s区间的dailybasic"%(stockCode,tmpSTARTDATE,tmpENDDATE))

        log.logger.info('处理完股票%s的每日数据'%(stockCode))
        
        #再partialUpdate时会进行commit，不需要再单独commit
        partialUpdate(mysqlSession)
        lastDataUpdate(mysqlSession,stockCode,"DB")


    #6、获取复权因子数据，并存入数据库
    
    #找到之前处理的最后一个股票的代码
    sql="select content from u_data_desc where content_name='adjdata_update_to'"
    res=mysqlProcessor.querySql(sql)
    adjdata_update_to=res.at[0,'content']
    
    for index,stockCode in stockCodeList.items():
    
        if stockCode<=adjdata_update_to:
            continue
        
        #获取该股票的复权因子数据并写入数据库
        adj_data=sdDataAPI.adj_factor(ts_code=stockCode,start_date=startday,end_date=endday)
        
        adj_data.sort_index(inplace=True,ascending=False)
        
        #存入数据库
        try:
            adj_data.to_sql(name='s_adj_data', con=mysqlEngine, chunksize=1000, if_exists='append', index=None)
        except sqlalchemy.exc.IntegrityError:
            log.logger.warning("此前处理过该股票%s在%s到%s区间的复权因子"%(stockCode,tmpSTARTDATE,tmpENDDATE))

        log.logger.info('处理完股票%s的复权因子'%(stockCode))
    
        partialUpdate(mysqlSession)
        lastDataUpdate(mysqlSession,stockCode, "ADJ")
    

    #完成所有数据的更新
    totalUpdate(mysqlSession,sdProcessor)
    
    
    #完成所有数据更新，把数据库表重置，以便下一次处理使用
    lastDataUpdate(mysqlSession,"","FR_StockCode")
    lastDataUpdate(mysqlSession,"","KD")
    lastDataUpdate(mysqlSession,"","DB")
    lastDataUpdate(mysqlSession,"","ADJ")
      
    
    mysqlSession.close()
    