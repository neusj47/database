# 국내 주식 데이터 크롤링
# 1. WICS 기준 섹터 정보 가져오기
# 2. DB TABLE 생성
# 3. 데이터 크롤링
# 4. DB 입력

import requests
import pandas as pd
from datetime import datetime

wics_lc = {10: '에너지',
           15: '소재',
           20: '산업재',
           25: '경기관련소비재',
           30: '필수소비재',
           35: '건강관리',
           40: '금융',
           45: 'IT',
           50: '커뮤니케이션서비스',
           55: '유틸리티'}

wics_mc = {1010: '에너지',
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


def wics_url(date, wics_code):
    '''
    Parameter
    - date[str] : the date corresponding data (yyyymmdd)
    - wics_code[int] : the wics code corresponding data (use wics_lc or wics_mc)

    Return
    - url[str]
    '''
    url = 'http://www.wiseindex.com/Index/GetIndexComponets?ceil_yn=0&' \
          'dt=' + date + '&sec_cd=G' + str(wics_code)
    return url


def comp_url(code):
    '''
    Parameter
    - code[str] : the company code corresponding data

    Return
    - url[str]
    '''
    url = 'http://comp.fnguide.com/SVO2/ASP/SVD_Main.asp?pGB=1&' \
          'gicode=A' + code + \
          '&cID=&MenuYn=Y&ReportGB=&NewMenuID=Y&stkGb=701'
    return url



df = pd.DataFrame(columns=['code', 'name', 'ls', 'ms'])
date = datetime.today().strftime('%Y%m%d')
date = '20211020'

for i, wics_code in enumerate(wics_mc.keys()):
    response = requests.get(wics_url(date, wics_code))

    if (response.status_code == 200):  # request success
        json_list = response.json()  # dictionary
        for json in json_list['list']:
            ls = json['SEC_NM_KOR']  # Large sector
            ms = json['IDX_NM_KOR'][5:]  # Medium sector
            code = json['CMP_CD']  # Company code
            name = json['CMP_KOR']  # Company korean name
            mktval = json['MKT_VAL']  # MARKET BALANCE
            wgt = json['WGT']  # WEIGHT
            df = df.append({'code': code, 'name': name, 'ls': ls, 'ms': ms, 'mval' : mktval, 'wgt' : wgt},
                           ignore_index=True)
        # print(i, ' 번째 입력완료 : ', name, ls)
    else:
        print(i, ' 번째 오류발생 : ', response.status_code, ' 오류:',str(wics_code))
