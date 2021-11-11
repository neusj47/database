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

# 1. 재무제표 크롤링
def download_fin_stats(start_date, ticker, period):
    fs  = dart.get_corp_list().find_by_stock_code(ticker).extract_fs(bgn_de=start_date, report_tp = [period])
    fs.save(ticker + '.xlsx', path = file_path)
    return

code = ticker.code[1]

# 2. 재무제표 가공하기

# def get_bstats(code):
#     fs = dart.get_corp_list().find_by_stock_code(code).extract_fs(bgn_de=start_date, report_tp=[period])
#     b = fs.show('bs')
#     b_cols = ['ifrs-full_CurrentAssets', 'ifrs-full_Assets', 'ifrs-full_CurrentLiabilities', 'ifrs-full_Liabilities',
#                'ifrs-full_RetainedEarnings', 'ifrs-full_Equity']
#     b_stats = b[b[
#         '[D210000] Statement of financial position, current/non-current - Consolidated financial statements (Unit: KRW)'].concept_id.isin(b_cols)].reset_index(drop=True)
#     temp = b_stats.iloc[0, 7:len(b_stats.columns)].reset_index()
#     df_b = pd.DataFrame(index=temp['level_0'], columns=b_cols).reset_index()
#     df_b[b_cols[0]] = temp[0]
#     for i in range(1, len(b_cols)):
#         temp = b_stats.iloc[i, 7:len(b_stats.columns)].reset_index()
#         df_b[b_cols[i]] = temp[i]
#     df_b['code'] = code
#     return df_b

def get_bstats(code) :
    fs = dart.get_corp_list().find_by_stock_code(code).extract_fs(bgn_de=start_date, report_tp=[period])
    b = fs.show('bs')
    b_cols = ['ifrs-full_CurrentAssets','ifrs-full_Inventories','ifrs-full_Assets','ifrs-full_CurrentLiabilities',
            'ifrs-full_Liabilities', 'ifrs-full_RetainedEarnings', 'ifrs-full_Equity']
    entrance = '[D210000] Statement of financial position, current/non-current - Consolidated financial statements (Unit: KRW)'
    b_stats = b[b[entrance].concept_id.isin(b_cols)].reset_index(drop=True)
    b_list = b_stats[entrance].concept_id.to_list()
    temp = b_stats.iloc[0,7:len(b_stats.columns)].reset_index()
    df_b = pd.DataFrame(index=temp['level_0'], columns=b_cols).reset_index()
    for i in range(0, len(b_cols)):
        if b_cols[i] not in b_list :
            df_b[b_cols[i]] = 0
        else :
            temp = b_stats[b_stats[entrance].concept_id == b_cols[i]].iloc[0,7:len(b_stats.columns)].reset_index()
            df_b[b_cols[i]] = temp[i]
    df_b = df_b.dropna(axis=0)
    return df_b

def get_istats(code) :
    fs = dart.get_corp_list().find_by_stock_code(code).extract_fs(bgn_de=start_date, report_tp=[period])
    i = fs.show('cis')
    i_cols = ['ifrs-full_Revenue', 'ifrs-full_GrossProfit','dart_OperatingIncomeLoss','ifrs-full_ProfitLoss',
              'ifrs-full_ComprehensiveIncome', 'ifrs-full_BasicEarningsLossPerShare']
    tg_col = '[D431410] Statement of comprehensive income, by function of expense - Consolidated financial statements (Unit: KRW)'
    i_stats = i[i[tg_col].concept_id.isin(i_cols)].reset_index(drop=True)
    i_list = i_stats[tg_col].concept_id.to_list()
    temp = i_stats.iloc[0, 7:len(i_stats.columns)].reset_index()
    df_i = pd.DataFrame(index=temp['level_0'], columns=i_cols).reset_index()
    for i in range(0, len(i_cols)):
        if i_cols[i] not in i_list :
            df_i[i_cols[i]] = 0
        else :
            temp = i_stats[i_stats[tg_col].concept_id == i_cols[i]].iloc[0,7:len(i_stats.columns)].reset_index()
            df_i[i_cols[i]] = temp[i]
    df_i = df_i.dropna(axis=0)
    return df_i


# 3. DB 입력하기
for i, code in enumerate(ticker.code) :
    try :
        df_b = get_bstats(code)
        df_i = get_istats(code)
        print(i, ' 번 째 입력완료 : ', code)
    except Exception as e:
            print(i, ' 번 째 오류 발생 : ', code, ' 오류:', str(e))
