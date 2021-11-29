from urllib import parse
import pandas as pd
import requests
from datetime import datetime, timedelta
from pykrx import stock
import numpy as np
import pymysql

pd.set_option('mode.chained_assignment',  None)
conn = ''
cursor = conn.cursor()

stddate = stock.get_nearest_business_day_in_a_week(datetime.strftime(datetime.strptime(datetime.now().strftime('%Y%m%d'), "%Y%m%d") - timedelta(days=1), "%Y%m%d"))

t = [0,11, ['IFRS(연결)','매출액','영업이익','당기순이익','자산총계','부채총계','자본총계','ROA','ROE','EPS(원)','BPS(원)','DPS(원)','PER','PBR','배당수익률','발행주식수'],
       ['Date','REV','OI','NI','ASS','LI','EQ','ROA','ROE','EPS','BPS','DPS','PER','PBR','DIV','SHARES']]
i = [1,1, ['index','매출액','매출총이익','영업이익','당기순이익'], ['Date','REV','GP','OI','NI']]
b = [1,3, ['index','자산','부채','자본'], ['Date','ASS','LIA','EQ']]
c = [1,5, ['index','영업활동으로인한현금흐름','*영업에서창출된현금흐름','투자활동으로인한현금흐름','재무활동으로인한현금흐름'], ['Date','CFO','FCF','CFI','CFF']]

# 0. 대상 ticker 선정
def ticker_info(stddate):
    sector = {1010: '에너지',
              1510: '소재',
              2010: '자본재',
              2020: '상업서비스와공급품',
              2030: '운송',
              2510: '자동차와부품',
              2520: '내구소비재와의류',
              2530: '호텔,레스토랑,레저 등',
              2550: '소매(유통)',
              2560: '교육서비스',
              3010: '식품과기본식료품소매',
              3020: '식품,음료,담배',
              3030: '가정용품과개인용품',
              3510: '건강관리장비와서비스',
              3520: '제약과생물공학',
              4010: '은행',
              4020: '증권',
              4030: '다각화된금융',
              4040: '보험',
              4050: '부동산',
              4510: '소프트웨어와서비스',
              4520: '기술하드웨어와장비',
              4530: '반도체와반도체장비',
              4535: '전자와 전기제품',
              4540: '디스플레이',
              5010: '전기통신서비스',
              5020: '미디어와엔터테인먼트',
              5510: '유틸리티'}
    df = pd.DataFrame(columns=['code', 'name', 'sector_l', 'sector_m', 'mktval', 'wgt'])
    for i, sec_code in enumerate(sector.keys()):
        response = requests.get(
            'http://www.wiseindex.com/Index/GetIndexComponets?ceil_yn=0&''dt=' + stddate + '&sec_cd=G' + str(sec_code))
        if (response.status_code == 200):
            json_list = response.json()
            for json in json_list['list']:
                code = json['CMP_CD']
                name = json['CMP_KOR']
                sector_l = json['SEC_NM_KOR']
                sector_m = json['IDX_NM_KOR'][5:]
                mktval = json['MKT_VAL']
                wgt = json['WGT']
                df = df.append(
                    {'code': code, 'name': name, 'sector_l': sector_l, 'sector_m': sector_m, 'mktval': mktval,
                     'wgt': wgt}, ignore_index=True)
    return df
ticker = ticker_info(stddate)

def get_fn(code) :
    param_input = {
        'pGB':1,
        'gicode':'A%s'%(code),
        'cID':'',
        'MenuYn':'Y',
        'ReportGB':'',
        'NewMenuID':101,
        'stkGb':701
    }
    url_core = "http://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?%s" % (parse.urlencode(param_input))
    url_spec = "http://comp.fnguide.com/SVO2/ASP/SVD_Finance.asp?%s" % (parse.urlencode(param_input))
    table_core = pd.read_html(url_core, header=0)
    table_spec = pd.read_html(url_spec, header=0)
    return table_core, table_spec

def get_stat (int, n, stat_col, col, code) :
    temp = get_fn(code)[int][n]
    df = temp.T[1:len(temp.columns)]
    df.columns = temp.T.iloc[0]
    df = df.iloc[0:4].reset_index()[stat_col]
    df.columns = col
    df['Date'] = pd.to_datetime(df['Date'].str.split('(').str[0])
    df = df.fillna(0)
    return df

for i, code in enumerate(ticker.code) :
    try :
        df_t = get_stat(t[0], t[1], t[2], t[3], code)
        df_b = get_stat(b[0], b[1], b[2], b[3], code)
        df_i = get_stat(i[0], i[1], i[2], i[3], code)
        df_c = get_stat(c[0], c[1], c[2], c[3], code)
        df_all = pd.merge(df_b, df_i, on='Date', how='left')
        df_all = pd.merge(df_all, df_c, on='Date', how='left')
        df_all['LIEQ'] = round((df_all['LI'] / df_all['EQ']).astype(float), 2)
        df_all['GPR'] = round((df_all['GP'] / df_all['REV']).astype(float), 2)
        df_all['OIR'] = round((df_all['OI'] / df_all['REV']).astype(float), 2)
        df_all['NIR'] = round((df_all['NI'] / df_all['REV']).astype(float), 2)
        df_all['period'] = 'QOQ'
        df_all['code'] = code
        df_all = df_all.replace([np.inf, -np.inf], np.nan).fillna(0)

        for row in df_all.itertuples():
            sql = """
            insert into stat_info
            (code, last_update, ASS, LIA, EQ, REV, GP, OI, NI, CFO, FCF, CFI, CFF, LIEQ, GPR, OIR, NIR, period)
             values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(sql, (row[18], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11]
                                 , row[12],row[13], row[14], row[15], row[16], row[17]))
            conn.commit()
        # print(i, ' 번 째 산출 완료 : ', code)
    except Exception as e:
        print(i, ' 번 째 오류 발생 : ', code, ' 오류:', str(e))