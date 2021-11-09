import requests
import pandas as pd
import pymysql
import FinanceDataReader as fdr
import datetime


file_path = "C:/Users/ysj/Desktop/python/"


# 1.종목 리스트 추출
sp500 = fdr.StockListing('S&P500')
target_list = sp500.Symbol
today = (datetime.datetime.now()).strftime('%Y-%m-%d')

# 2. DB 생성하기
conn = ''
cursor = conn.cursor()
sql = """
CREATE TABLE IF NOT EXISTS stats_info (
code VARCHAR(20),
revenue BIGINT(20),
op_inc BIGINT(20),
net_inc BIGINT(20),
eps float,
rev_g float,
oi_g float,
ni_g float,
eps_g float,
bvps_g float,
fcf_g float,
last_update DATE,
PRIMARY KEY (last_update, code)
)
"""
cursor.execute(sql)
conn.commit()

# 3. 데이터 크롤링 하기
def download_stats(code) :
    income_url = "https://stockrow.com/api/companies/{TICKER}/financials.xlsx?dimension=Q&section=Income%20Statement&sort=desc"
    growth_url = "https://stockrow.com//api/companies/{TICKER}/financials.xlsx?dimension=Q&amp;section=Growth&amp;sort=desc"
    file_name_income = income_url.split('/')[-2].format(TICKER=code)+'_INCOME.xlsx'
    file_name_growth = income_url.split('/')[-2].format(TICKER=code)+'_GROWTH.xlsx'
    with open(file_path + file_name_income, 'wb') as file:
        response = requests.get(income_url.format(TICKER = code))
        if response.status_code == 200:
            file.write(response.content)
    with open(file_path + file_name_growth, 'wb') as file:
        response = requests.get(growth_url.format(TICKER = code))
        if response.status_code == 200:
            file.write(response.content)

def get_stock_stats(code ,t) :
    df_income = pd.read_excel(file_path +"{TICKER}_INCOME.xlsx".format(TICKER=code), index_col=0).fillna(0)
    df_income = df_income.T
    df_growth = pd.read_excel(file_path +"{TICKER}_GROWTH.xlsx".format(TICKER=code), index_col=0).fillna(0)
    df_growth = df_growth.T

    temp = []
    temp.append(code)
    temp.append(float(df_income.iloc[t]['Revenue']))
    temp.append(float(df_income.iloc[t]['Operating Income']))
    temp.append(float(df_income.iloc[t]['Net Income Common']))
    temp.append(float(df_income.iloc[t]['EPS (Basic)']))
    temp.append(float(df_income.iloc[t]['Revenue Growth']))
    temp.append(float(df_growth.iloc[t]['Operating Income Growth']))
    temp.append(float(df_growth.iloc[t]['Net Income Growth']))
    temp.append(float(df_growth.iloc[t]['EPS Growth (basic)']))
    temp.append(float(df_growth.iloc[t]['Book Value per Share Growth']))
    temp.append(float(df_growth.iloc[t]['Free Cash Flow Growth']))
    temp.append(datetime.datetime.strftime(df_income.index[t], '%Y-%m-%d'))
    return temp

# 4. DB 입력하기
sql = """
INSERT INTO stats_info
(code, revenue, op_inc, net_inc, eps, rev_g, oi_g, ni_g, eps_g, bvps_g, fcf_g, last_update)
 values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""
for i, ticker in enumerate(target_list):
    try :
        # download_stats(ticker)
        stock_stats = get_stock_stats(ticker,5)
        cursor.execute(sql, stock_stats)
        conn.commit()

    except Exception as e:
        print(i, ' 번 째 오류 발생 : ', ticker, ' 오류:', str(e))

conn.close()