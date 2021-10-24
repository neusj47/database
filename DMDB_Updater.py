# 국내주식 크롤링 (Investar 참고)
# 0. WICS 기준 섹터기준 분류
# 1. 종목 정보 크롤링
# 2. DB 생성 및 저장
# 3. 일별 자동생성

import pandas as pd
from datetime import datetime
import pymysql
import calendar
import requests
from threading import Timer

class DMDBUpdater:
    def __init__(self):
        self.conn = pymysql.connect(host='localhost', user='root', password='sjyoo1~', db='domestic', charset='utf8')
        with self.conn.cursor() as curs:
            sql = """
               CREATE TABLE IF NOT EXISTS stock_info (
                   code VARCHAR(20),
                   name VARCHAR(40),
                   sector_l VARCHAR(20),
                   sector_m VARCHAR(20),
                   mktval BIGINT,
                   wgt FLOAT,
                   last_update DATE,
                   PRIMARY KEY (last_update, code)
                   )
               """
            curs.execute(sql)
        self.conn.commit()
        self.codes = dict()  # code_temp

    def get_sectors(self):
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
        return sector

    def update_stock_info(self):
        date = '20211020'
        df = pd.DataFrame(columns=['code', 'name', 'sector_l', 'sector_m', 'mktval', 'wgt'])
        sector = self.get_sectors()
        for i, sec_code in enumerate(sector.keys()):
            response = requests.get('http://www.wiseindex.com/Index/GetIndexComponets?ceil_yn=0&''dt=' + date + '&sec_cd=G' + str(sec_code))
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
                        {'code': code, 'name': name, 'sector_l': sector_l, 'sector_m': sector_m, 'mktval': mktval,'wgt': wgt}, ignore_index=True)
        with self.conn.cursor() as curs:
            sql = "SELECT max(last_update) from stock_info"
            curs.execute(sql)
            rs = curs.fetchone()  # last_update
            today = datetime.today().strftime('%Y-%m-%d')
            if rs[0] == None or rs[0].strftime('%Y-%m-%d') < today:
                sql = "DELETE FROM stock_info"
                curs.execute(sql)
                self.conn.commit()
                for idx in range(len(df)):
                    code = df.code.values[idx]
                    name = df.name.values[idx]
                    sector_l = df.sector_l.values[idx]
                    sector_m = df.sector_m.values[idx]
                    mktval = df.mktval.values[idx]
                    wgt = df.wgt.values[idx]
                    today = datetime.today().strftime('%Y-%m-%d')
                    sql = f"INSERT INTO stock_info (code, name, sector_l, sector_m, mktval, wgt, last_update) " \
                          f"VALUES ('{code}', '{name}', '{sector_l}','{sector_m}', '{mktval}', '{wgt}', '{today}')"
                    curs.execute(sql)
                    tmnow = datetime.now().strftime('%Y-%m-%d %H:%M')
                    print(f"[{tmnow}] #{idx + 1:04d} INSERT INTO stock_info " \
                          f"VALUES ({code}, {name}, {today})")
                self.conn.commit()

    def __del__(self):
        self.conn.close()

    def execute_daily(self):
        self.update_stock_info()
        tmnow = datetime.now()
        lastday = calendar.monthrange(tmnow.year, tmnow.month)[1]
        if tmnow.month == 12 and tmnow.day == lastday:
            tmnext = tmnow.replace(year=tmnow.year + 1, month=1, day=1,
                                   hour=17, minute=0, second=0)
        elif tmnow.day == lastday:
            tmnext = tmnow.replace(month=tmnow.month + 1, day=1, hour=17,
                                   minute=0, second=0)
        else:
            tmnext = tmnow.replace(day=tmnow.day + 1, hour=17, minute=0,
                                   second=0)
        tmdiff = tmnext - tmnow
        secs = tmdiff.seconds
        t = Timer(secs, self.execute_daily)
        print("Waiting for next update ({}) ... ".format(tmnext.strftime('%Y-%m-%d %H:%M')))
        t.start()

if __name__ == '__main__':
    DMDBUpdater().execute_daily()