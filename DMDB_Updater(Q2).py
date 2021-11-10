# 국내 재무제표 크롤링
# 0. 대상 ticker 선정
# 1. 재무제표 가져오기
# 2. 대상 값 가공
# 3. DB 입력하기

import dart_fss as dart
import pandas as pd
import requests

file_path = "C:/Users/ysj/PycharmProjects/database/dmfs/"
api_key= ''
dart.set_api_key(api_key=api_key)

start_date = '20181201'
stddate = '20211108'
period = 'quarter' # annual, semiannual, quarter

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

# 1. 재무제표 가져오기
def download_fin_stats(start_date, ticker, period):
    fs  = dart.get_corp_list().find_by_stock_code(ticker).extract_fs(bgn_de=start_date, report_tp = [period])
    fs.save(ticker + '.xlsx', path = file_path)
    return


# 2. 재무제표 가공하기
def get_fstats(ticker):
    fs = dart.get_corp_list().find_by_stock_code(code).extract_fs(bgn_de=start_date, report_tp=[period])
    bs = fs.show('bs')
    bs_cols = ['ifrs-full_CurrentAssets', 'ifrs-full_Assets', 'ifrs-full_CurrentLiabilities', 'ifrs-full_Liabilities',
               'ifrs-full_RetainedEarnings', 'ifrs-full_Equity']
    bs_stats = bs[bs[
        '[D210000] Statement of financial position, current/non-current - Consolidated financial statements (Unit: KRW)'].concept_id.isin(
        bs_cols)].reset_index(drop=True)
    bs_stats_temp = bs_stats.iloc[0, 7:len(bs_stats.columns)].reset_index()
    bs_period = bs_stats_temp['level_0']
    df = pd.DataFrame(index=bs_period, columns=bs_cols)
    df = df.reset_index()
    df['TICKER'] = ticker.code[0]
    df[bs_cols[0]] = bs_stats_temp[0]

    for i in range(1, len(bs_cols) - 1):
        bs_stats_temp = bs_stats.iloc[i, 7:len(bs_stats.columns)].reset_index()
        df['TICKER'] = ticker.code[i]
        df[bs_cols[i]] = bs_stats_temp[i]
    return df



# 3. DB 입력하기
for i, code in enumerate(ticker.code) :
    try :
        download_fin_stats(start_date, code, period)
        print(i, ' 번 째 입력완료 : ', code)
    except Exception as e:
            print(i, ' 번 째 오류 발생 : ', code, ' 오류:', str(e))
