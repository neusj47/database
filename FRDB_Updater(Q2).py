import requests
from bs4 import BeautifulSoup
import pandas as pd
import pymysql
import FinanceDataReader as fdr

# 0. DB 생성하기
conn = ''
cursor = conn.cursor()
sql = """
CREATE TABLE IF NOT EXISTS amt_info (
code VARCHAR(20),
company VARCHAR(50),
sector VARCHAR(50),
industry VARCHAR(50),
amt BIGINT(20),
last_update DATE,
PRIMARY KEY (last_update, code)
)
"""
cursor.execute(sql)
conn.commit()

# 1. 대상 ticker 입력
sp500 = fdr.StockListing('S&P500')
sp500 = sp500.rename(columns={'Symbol':'code','Name':'company','Sector':'sector','Industry':'industry'})
target_list = sp500.code

# 2. 데이터 크롤링하기
def get_amt_data(TICKER, sp500) :
    header = {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    df_temp = pd.DataFrame(columns=['13f_transaction'])
    t = 0
    while True:
        soup = None
        try:
            t += 1
            url = 'https://www.dataroma.com/m/activity.php?sym={}&typ=a&L={}'.format(TICKER,t)
            html = requests.get(url,headers = header)
            soup = BeautifulSoup(html.text, 'html.parser')
        except:
            break
        trs = soup.select('tbody')[0].select('tr')
        if len(trs) == 0:
            break
        for tr in trs:
            if 'q_chg' in str(tr):
                last_update = pd.to_datetime(tr.select('b')[1].text + '-' + str('{:02d}'.format(int(tr.select('b')[0].text[-1])*3)) + '-' + '01')
            else:
                transact = tr.select('.sell')
                sign = -1
                if len(tr.select('.sell')) == 0:
                    transact = tr.select('.buy')
                    sign = 1
                amt = int(transact[1].text.replace(',' , '')) * sign
                df_temp = df_temp.append({'date':last_update,'amt':amt},ignore_index=True)
    df_temp = df_temp.groupby('date').sum()[::-1].reset_index()
    df = df_temp.iloc[0:3]
    df = df.assign(code = TICKER)
    df = pd.merge(sp500,df, on = 'code', how = 'inner')
    df = df[['code','company','sector','industry','amt','date']]
    df = df.rename(columns={'date':'last_update'})
    return df

# 3. DB 입력하기
for i, TICKER in enumerate(target_list):
    try :
        df = get_amt_data(TICKER, sp500)
        for row in df.itertuples():
            if row ==None :
                break
            sql = """
                insert into amt_info
                (code, company, sector, industry, amt, last_update)
                 values (%s, %s, %s, %s, %s, %s)
                """
            cursor.execute(sql, (row[1], row[2], row[3], row[4], row[5], row[6]))
        conn.commit()
        # print(i, ' 번째 입력완료 : ', TICKER)
    except Exception as e :
        print (i, ' 번째 오류발생 : ', TICKER, ' 오류:', str(e))
