# 해외 주식 데이터 크롤링(Lazyquant 참고)
# 1. 종목리스트 추출
# 2. DB TABLE 생성
# 3. 데이터 크롤링
# 4. DB 입력


import datetime
import yahoo_fin.stock_info as si
import FinanceDataReader as fdr
import pymysql


# 1.종목 리스트 추출
sp500 = fdr.StockListing('S&P500')
target_list = sp500.Symbol
today = (datetime.datetime.now()).strftime('%Y-%m-%d')

# 2.DB TABLE 생성
conn = ''
cursor = conn.cursor()
sql = """
CREATE TABLE IF NOT EXISTS stock_info (
code VARCHAR(20),
company VARCHAR(50),
sector VARCHAR(40),
industry VARCHAR(50),
opm BIGINT(20),
roe BIGINT(20),
roa BIGINT(20),
rim BIGINT(20),
mktval BIGINT(20),
per BIGINT(20),
pegr BIGINT(20),
pbr BIGINT(20),
psr BIGINT(20),
evebitda BIGINT(20),
ncav BIGINT(20),
gpa BIGINT(20),
re_qoq BIGINT(20),
oi_qoq BIGINT(20),
ni_qoq BIGINT(20),
re_qoq_p BIGINT(20),
oi_qoq_p BIGINT(20),
ni_qoq_p BIGINT(20),
last_update DATE,
PRIMARY KEY (code))
"""
cursor.execute(sql)
conn.commit()

# 3.데이터 크롤링 실행
def get_fundamental(TICKER) :
    df_fundamental = si.get_stats(TICKER).fillna(0)
    opm = df_fundamental.iloc[31]['Value']
    roe = df_fundamental.iloc[32]['Value']
    roa = df_fundamental.iloc[33]['Value']
    rim = round(float(df_fundamental.iloc[48,1]) * float(df_fundamental.iloc[34,1].replace('%','')) * 1/100,2)
    if type(opm) == str :
        opm = float(opm.replace('%', '').replace(',', ''))
    if type(roe) == str :
        roe = float(roe.replace('%', '').replace(',', ''))
    if type(roa) == str :
        roa = float(roa.replace('%', '').replace(',', ''))
    return opm, roe, roa, rim

def get_valuation(TICKER) :
    df_valuation = si.get_stats_valuation(TICKER).fillna(0)
    mktval = df_valuation.iloc[0, 1]
    if mktval[-1] == 'K':
        mktval = float(mktval.replace('K', '')) * 1000
    elif mktval[-1] == 'M':
        mktval = float(mktval.replace('M', '')) * 1000000
    elif mktval[-1] == 'B':
        mktval = float(mktval.replace('B', '')) * 1000000000
    elif mktval[-1] == 'T':
        mktval = float(mktval.replace('T', '')) * 1000000000000
    per = df_valuation.iloc[2, 1]
    pegr = df_valuation.iloc[4, 1]
    pbr = df_valuation.iloc[5, 1]
    psr = df_valuation.iloc[6, 1]
    evebitda = df_valuation.iloc[8,1]
    if type(per) == str and per[-1] == 'K':
        per = float(per.replace('K', '')) * 1000
    if type(per) == str and per[-1] == 'B':
        per = float(per.replace('B', '')) * 1000000000
    if type(pegr) == str and pegr[-1] == 'K':
        pegr = float(pegr.replace('K', '')) * 1000
    if type(pegr) == str and pegr[-1] == 'B':
        pegr = float(pegr.replace('B', '')) * 1000000000
    if type(pbr) == str and pbr[-1] == 'K':
        pbr = float(pbr.replace('K', '')) * 1000
    if type(pbr) == str and pegr[-1] == 'B':
        pbr = float(pbr.replace('B', '')) * 1000000000
    if type(psr) == str and psr[-1] == 'K':
        psr = float(psr.replace('K', '')) * 1000
    if type(psr) == str and psr[-1] == 'B':
        psr = float(psr.replace('B', '')) * 1000000000
    if type(evebitda) == str and psr[-1] == 'K':
        psr = float(evebitda.replace('K', '')) * 1000
    if type(evebitda) == str and psr[-1] == 'B':
        psr = float(evebitda.replace('B', '')) * 1000000000
    return mktval, per, pegr, pbr, psr, evebitda

def get_bs_stats(TICKER) :
    df_balance = si.get_balance_sheet(TICKER, yearly=False).fillna(0)
    df_income = si.get_income_statement(TICKER, yearly=False).fillna(0)
    ncav = float(df_balance.iloc[18,0]) - float(df_balance.iloc[2,0])
    gpa = df_income.iloc[6,0] * 1 / (1/2 * (df_balance.iloc[6,0] + df_balance.iloc[6,3]))
    return ncav, gpa

def get_is_stats(TICKER):
    df_income = si.get_income_statement(TICKER, yearly=False).loc[['totalRevenue', 'operatingIncome', 'netIncome']].fillna(0)
    re_qoq, oi_qoq, ni_qoq, re_qoq_p, oi_qoq_p, ni_qoq_p = 0, 0, 0, 0, 0, 0
    if float(df_income.iloc[0, 1]) != 0: re_qoq = round((float(df_income.iloc[0, 0]) - float(df_income.iloc[0, 1])) / abs(float(df_income.iloc[0, 1])) * 100, 2)
    if float(df_income.iloc[1, 1]) != 0: oi_qoq = round((float(df_income.iloc[1, 0]) - float(df_income.iloc[1, 1])) / abs(float(df_income.iloc[1, 1])) * 100, 2)
    if float(df_income.iloc[2, 1]) != 0: ni_qoq = round((float(df_income.iloc[2, 0]) - float(df_income.iloc[2, 1])) / abs(float(df_income.iloc[2, 1])) * 100, 2)
    if float(df_income.iloc[0, 2]) != 0: re_qoq_p = round((float(df_income.iloc[0, 1]) - float(df_income.iloc[0, 2])) / abs(float(df_income.iloc[0, 2])) * 100, 2)
    if float(df_income.iloc[1, 2]) != 0: oi_qoq_p = round((float(df_income.iloc[1, 1]) - float(df_income.iloc[1, 2])) / abs(float(df_income.iloc[1, 2])) * 100, 2)
    if float(df_income.iloc[2, 2]) != 0: ni_qoq_p = round((float(df_income.iloc[2, 1])  - float(df_income.iloc[2, 2]) ) / abs(float(df_income.iloc[2, 2]) ) * 100, 2)
    today = (datetime.datetime.now()).strftime('%Y-%m-%d')
    return re_qoq, oi_qoq, ni_qoq, re_qoq_p, oi_qoq_p, ni_qoq_p, today


# 4.DB 입력
sql = """
insert into stock_info
(code, company, sector, industry, opm, roe, roa, rim, mktval, per, pegr, pbr, psr, evebitda, ncav, 
gpa, re_qoq, oi_qoq, ni_qoq, re_qoq_p, oi_qoq_p, ni_qoq_p, last_update)
 values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""
for i, TICKER in enumerate(target_list):
    try:
        stock_info = tuple((sp500[sp500['Symbol'] == TICKER]).iloc[0]) + get_fundamental(TICKER)
        stock_info = stock_info + get_valuation(TICKER)
        stock_info = stock_info + get_bs_stats(TICKER)
        stock_info = stock_info + get_is_stats(TICKER)

        cursor.execute(sql, stock_info)
        conn.commit()
        print(i, ' 번째 입력완료 : ', TICKER)
    except Exception as e:
        print(i, ' 번째 오류발생 : ', TICKER, ' 오류:', str(e))