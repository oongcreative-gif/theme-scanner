"""테마 스캐너 - 3개월 전고점을 돌파한 강양봉(+15%, 거래대금 500억 이상)을 찾고 테마별로 묶어 보여줍니다.

포착 조건 (하루 단위)
  1) 양봉이고 종가가 전일 종가보다 +15% 이상
  2) 거래대금 500억 이상  (거래대금 = (고가+저가+종가)/3 x 거래량 으로 추정)
  3) 종가가 직전 3개월(91일) 장중 고가 최고치보다 높음  -> 전고점 돌파
  4) 바닥 = 직전 3개월 최저 저가, '바닥 대비 상승률' = 종가 / 바닥 - 1
테마는 네이버 금융 테마(종목이 여러 테마에 속할 수 있음)를 매일 가져와 붙입니다.
순위표(1~10위)는 사이트에서 '최근 30일 포착 종목 수 + 거래대금 합계'로 계산합니다.

결과: docs/data/list.json, docs/data/charts/{code}.json
"""
import html
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import requests
import FinanceDataReader as fdr

ROOT = os.environ.get('SCAN_ROOT') or os.path.dirname(os.path.abspath(__file__))
KST = ZoneInfo('Asia/Seoul')
DATA_DIR = os.path.join(ROOT, 'docs', 'data')
LIST_JSON = os.path.join(DATA_DIR, 'list.json')
UNIVERSE_CSV = os.path.join(ROOT, 'data', 'th_universe.csv')
THEMES_JSON = os.path.join(ROOT, 'data', 'th_themes.json')

MIN_CHANGE = 15.0                    # 종가 등락률 (%)
MIN_AMOUNT = 50_000_000_000          # 거래대금 500억
LOOKBACK_DAYS = 91                   # 전고점/바닥을 보는 기간 (약 3개월)
MIN_LOOKBACK_BARS = 20
SINCE = os.environ.get('TH_SINCE') or '2026-06-01'
CHART_FROM_DAYS = 70                 # 차트는 SINCE 보다 이만큼 앞부터
FETCH_PAD_DAYS = LOOKBACK_DAYS + 20
MAX_WORKERS = 8
MAX_ERROR_RATIO = 0.2
MIN_LISTING = 1000                   # 종목 목록이 이보다 적으면 이상한 것으로 봄

UA = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) '
                    'Chrome/124.0 Safari/537.36', 'Accept-Language': 'ko-KR,ko;q=0.9'}


def save_json(path, obj, indent=None):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(obj, f, ensure_ascii=False, indent=indent, allow_nan=False)
    os.replace(tmp, path)


def load_json(path, default):
    try:
        with open(path, encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return default


def reference_date():
    start = (datetime.now(KST) - timedelta(days=14)).strftime('%Y-%m-%d')
    return fdr.DataReader('005930', start).index[-1].date()


# ---------------------------------------------------------------- 종목 목록
def get_universe():
    try:
        lst = fdr.StockListing('KRX')
        code_col = 'Code' if 'Code' in lst.columns else 'Symbol'
        lst = lst.rename(columns={code_col: 'Code'})
        lst = lst[lst['Market'].astype(str).str.startswith('KOS')]
        lst = lst[~lst['Name'].str.contains(r'스팩|제\d+호|ETN|ETF', regex=True, na=False)]
        lst = lst[~lst['Name'].str.contains(r'우$|우B$|우\(전환\)$', regex=True, na=False)]
        if 'Dept' in lst.columns:
            lst = lst[~lst['Dept'].astype(str).str.contains('관리|SPAC', na=False)]
        lst = lst.copy()
        lst['Marcap'] = lst['Marcap'].fillna(0)
        print(f"KRX 목록 {len(lst)}종목 (코스피+코스닥), 시총 값 있는 종목 {int((lst['Marcap'] > 0).sum())}개")
        if len(lst) < MIN_LISTING:
            raise RuntimeError('종목 목록이 너무 적습니다')
        uni = lst[['Code', 'Name', 'Market', 'Marcap']].reset_index(drop=True)
        uni['Code'] = uni['Code'].astype(str).str.zfill(6)
        os.makedirs(os.path.dirname(UNIVERSE_CSV), exist_ok=True)
        uni.to_csv(UNIVERSE_CSV, index=False)
        return uni
    except Exception as e:                                  # noqa: BLE001
        print(f'KRX 종목 목록 조회 실패: {e!r}')
        if os.path.exists(UNIVERSE_CSV):
            cached = pd.read_csv(UNIVERSE_CSV, dtype={'Code': str})
            if len(cached):
                print('-> 저장해 둔 data/th_universe.csv 를 대신 사용합니다')
                return cached
        raise


# ---------------------------------------------------------------- 네이버 테마
# 네이버가 증권 화면을 새로 바꿔서(React) 예전 finance.naver.com 표 방식은 못 읽어요.
# 지금은 새 증권(stock.naver.com)이 쓰는 데이터 주소를 읽습니다.
API = 'https://stock.naver.com/api/domestic/market/theme'
API_HEADERS = {**UA, 'Accept': 'application/json, text/plain, */*', 'Referer': 'https://stock.naver.com/market/stock/kr/theme'}
ID_KEYS = ('no', 'groupNo', 'themeNo', 'categoryNo', 'id')
NAME_KEYS = ('name', 'groupName', 'themeName', 'categoryName', 'title')
CODE_KEYS = ('itemCode', 'stockCode', 'symbolCode', 'code', 'reutersCode')
RE_CODE = re.compile(r'^[0-9A-Z]{6}$')


def http_json(url):
    r = requests.get(url, headers=API_HEADERS, timeout=20)
    r.raise_for_status()
    return json.loads(r.content.decode('utf-8', errors='replace'))


def _dicts(obj):
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from _dicts(v)
    elif isinstance(obj, list):
        for v in obj:
            yield from _dicts(v)


def parse_theme_list(j):
    """[(번호, 이름)] - 응답 모양이 조금 달라도 읽을 수 있게 느슨하게 찾습니다"""
    out = []
    for d in _dicts(j):
        if any(isinstance(d.get(k), str) and RE_CODE.match(d.get(k, '')) for k in CODE_KEYS):
            continue                                             # 종목 정보는 건너뜀
        tid = next((d[k] for k in ID_KEYS if k in d and str(d[k]).isdigit()), None)
        name = next((d[k] for k in NAME_KEYS if isinstance(d.get(k), str) and d[k].strip()), None)
        if tid is not None and name:
            out.append((str(tid), html.unescape(name).strip()))
    return out


def parse_theme_stocks(j):
    seen, out = set(), []
    for d in _dicts(j):
        for k in CODE_KEYS:
            v = d.get(k)
            if isinstance(v, str) and RE_CODE.match(v):
                if v not in seen:
                    seen.add(v)
                    out.append(v)
                break
    return out


def _paged(url_of, parse, first_diag=None, max_pages=40, size=100):
    """startIdx 가 '쪽 번호'인지 '건너뛸 개수'인지 확실하지 않아서 두 방식을 모두 시도해 합칩니다."""
    got, order = {}, []
    for mode in ('page', 'offset'):
        for n in range(max_pages):
            j = http_json(url_of(n if mode == 'page' else n * size))
            if first_diag and mode == 'page' and n == 0:
                first_diag(j)
            new = [x for x in parse(j) if (x[0] if isinstance(x, tuple) else x) not in got]
            if not new:
                break
            for x in new:
                got[x[0] if isinstance(x, tuple) else x] = x
                order.append(x)
            time.sleep(0.15)
    return order


def fetch_themes():
    """{테마이름: [종목코드,...]} - 네이버 증권 테마. 실패하면 저장해 둔 값을 씁니다."""
    themes = {}
    try:
        def diag(j):
            txt = json.dumps(j, ensure_ascii=False)
            print(f'[진단] 테마 목록 응답 {len(txt)}자, 앞부분: {txt[:200]}')
        listed = _paged(lambda n: f'{API}/list?startIdx={n}&pageSize=100&sortType=changeRate',
                        parse_theme_list, diag)
        if len(listed) < 30:
            raise RuntimeError(f'테마 목록을 {len(listed)}개밖에 못 읽었어요 (네이버 데이터 모양이 바뀌었거나 접속이 막혔을 수 있어요)')
        print(f'네이버 테마 {len(listed)}개 확인, 종목 가져오는 중...')
        fails = 0
        for tid, name in listed:
            try:
                codes = _paged(lambda n, tid=tid: f'{API}/{tid}/stocklist?marketType=ALL&orderType=quantTop&startIdx={n}&pageSize=20',
                               parse_theme_stocks, max_pages=15, size=20)
                if codes:
                    themes[name] = codes
                else:
                    fails += 1
            except Exception as e:                          # noqa: BLE001
                fails += 1
                print(f'  테마 {name}({tid}) 실패: {e!r}')
            time.sleep(0.1)
        if len(themes) < len(listed) * 0.7:
            raise RuntimeError(f'테마 종목을 너무 많이 못 읽었어요 ({len(themes)}/{len(listed)})')
        print(f'테마 {len(themes)}개 / 실패 {fails}개')
        save_json(THEMES_JSON, {'updated_at': datetime.now(KST).isoformat(timespec='seconds'), 'themes': themes})
        return themes, 'live'
    except Exception as e:                                  # noqa: BLE001
        print(f'테마 가져오기 실패: {e!r}')
        cached = load_json(THEMES_JSON, None)
        if cached and cached.get('themes'):
            print(f"-> 저장해 둔 테마({cached.get('updated_at')})를 대신 사용합니다")
            return cached['themes'], 'cache'
        return {}, 'none'


# ---------------------------------------------------------------- 분석
def prepare(raw):
    df = raw.dropna(subset=['Open', 'High', 'Low', 'Close']).copy()
    df = df[(df['Open'] > 0) & (df['High'] > 0) & (df['Low'] > 0) & (df['Close'] > 0) & (df['Volume'] > 0)]
    df['amount'] = (df['High'] + df['Low'] + df['Close']) / 3 * df['Volume']
    df['prev_close'] = df['Close'].shift(1)
    return df


def find_events(df, since):
    """포착일 목록 (오래된 순)"""
    out = []
    since_ts = pd.Timestamp(since)
    for i in range(1, len(df)):
        d = df.index[i]
        if d < since_ts:
            continue
        row = df.iloc[i]
        chg = (row['Close'] / row['prev_close'] - 1) * 100
        if chg < MIN_CHANGE or not row['Close'] > row['Open'] or row['amount'] < MIN_AMOUNT:
            continue
        win = df[(df.index >= d - timedelta(days=LOOKBACK_DAYS)) & (df.index < d)]
        if len(win) < MIN_LOOKBACK_BARS:
            continue
        prev_high = float(win['High'].max())
        if not row['Close'] > prev_high:
            continue
        bottom = min(float(win['Low'].min()), float(row['Low']))
        bdate = win['Low'].idxmin() if float(win['Low'].min()) <= float(row['Low']) else d
        out.append({
            'date': d.date().isoformat(), 'change_pct': round(float(chg), 1),
            'amount': int(round(float(row['amount']) / 1e8)),          # 억
            'close': float(row['Close']), 'prev_high': prev_high,
            'prev_high_date': win['High'].idxmax().date().isoformat(),
            'bottom': bottom, 'bottom_date': bdate.date().isoformat(),
            'rise_pct': round((float(row['Close']) / bottom - 1) * 100, 1),
        })
    return out


def make_chart(df, events):
    start = pd.Timestamp(SINCE) - timedelta(days=CHART_FROM_DAYS)
    tail = df[df.index >= start]

    def col(name, nd=2):
        return [None if pd.isna(x) else round(float(x), nd) for x in tail[name]]

    return {'t': [d.date().isoformat() for d in tail.index],
            'o': col('Open'), 'h': col('High'), 'l': col('Low'), 'c': col('Close'), 'v': col('Volume', 0),
            'events': [{k: e[k] for k in ('date', 'prev_high', 'bottom', 'bottom_date')} for e in events]}


def scan_one(code, start, asof):
    last_err = None
    for _ in range(3):
        try:
            raw = fdr.DataReader(code, start)
            if raw is None or raw.empty:
                return 'none', None
            if raw.index[-1].date() != asof:                # 오늘 봉이 없는 종목(거래정지 등)
                return 'none', None
            df = prepare(raw)
            events = find_events(df, SINCE)
            if not events:
                return 'none', None
            now_close = float(df['Close'].iloc[-1])
            return 'ok', (events, now_close, make_chart(df, events))
        except Exception as e:                              # noqa: BLE001
            last_err = repr(e)
            time.sleep(0.7)
    return 'error', last_err


def theme_ranking(items, asof, days=30, top=10):
    """사이트와 같은 방식의 순위 (로그 확인용)"""
    cut = (pd.Timestamp(asof) - timedelta(days=days)).date().isoformat()
    agg = {}
    for it in items:
        if it['date'] < cut:
            continue
        for t in it['themes']:
            a = agg.setdefault(t, {'codes': set(), 'amount': 0})
            a['codes'].add(it['code'])
            a['amount'] += it['amount']
    rows = sorted(agg.items(), key=lambda kv: (-len(kv[1]['codes']), -kv[1]['amount']))
    return [(t, len(a['codes']), a['amount']) for t, a in rows[:top]]


def main():
    now = datetime.now(KST)
    force = os.environ.get('FORCE') == '1'
    asof = reference_date()
    if not force:
        if asof != now.date():
            print(f'오늘({now.date()})은 휴장이거나 아직 데이터가 없어요 (최근 거래일 {asof}). 종료합니다.')
            return 0
        if now.hour < 16:
            print('장 마감 후 데이터가 확정되기 전(16시 이전)이라 종료합니다.')
            return 0

    uni = get_universe()
    themes, theme_status = fetch_themes()
    code_themes = {}
    for name, codes in themes.items():
        for c in codes:
            code_themes.setdefault(c, []).append(name)

    start = (pd.Timestamp(SINCE) - timedelta(days=FETCH_PAD_DAYS)).strftime('%Y-%m-%d')
    print(f'기준일 {asof} / {len(uni)}종목 / 포착 시작일 {SINCE} / 테마 {theme_status}')
    items, charts, errors, first_error = [], {}, 0, None
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {ex.submit(scan_one, r.Code, start, asof): r for r in uni.itertuples()}
        for f in as_completed(futs):
            r = futs[f]
            status, payload = f.result()
            if status == 'ok':
                events, now_close, chart = payload
                th = code_themes.get(r.Code, [])
                for e in events:
                    items.append({'code': r.Code, 'name': r.Name, 'market': r.Market, 'marcap': int(r.Marcap),
                                  **e, 'now_close': now_close,
                                  'since_pct': round((now_close / e['close'] - 1) * 100, 1), 'themes': th})
                charts[r.Code] = {**chart, 'code': r.Code, 'name': r.Name}
            elif status == 'error':
                errors += 1
                first_error = first_error or payload
    items.sort(key=lambda x: (x['date'], x['amount']), reverse=True)
    print(f"포착 {len(items)}건 ({len(charts)}종목), 조회 실패 {errors}종목")
    for k, (t, n, amt) in enumerate(theme_ranking(items, asof), start=1):
        print(f'  최근 30일 {k}위 {t}: {n}종목 / 거래대금 합 {amt:,}억')
    if len(uni) and errors / len(uni) > MAX_ERROR_RATIO:
        print(f'조회 실패가 너무 많아 결과를 저장하지 않습니다. 첫 에러: {first_error}')
        return 1

    for code, ch in charts.items():
        save_json(os.path.join(DATA_DIR, 'charts', f'{code}.json'), ch)
    save_json(LIST_JSON, {
        'updated_at': now.isoformat(timespec='seconds'), 'asof': asof.isoformat(), 'since': SINCE,
        'theme_status': theme_status, 'theme_count': len(themes),
        'params': {'min_change': MIN_CHANGE, 'min_amount_eok': MIN_AMOUNT // 100_000_000,
                   'lookback_days': LOOKBACK_DAYS},
        'items': items,
    }, indent=1)
    return 0


if __name__ == '__main__':
    sys.exit(main())
