#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
인삼 단가 분석 대시보드 - 데이터 업데이트 스크립트
============================================
새 CSV 파일이 추가될 때마다 이 스크립트를 실행하면
데이터가 자동으로 최신화됩니다.

사용법 (터미널/명령 프롬프트):
  python3 데이터_업데이트.py

Python 3.7+ 필요. 별도 패키지 설치 불필요.
"""
import os, re, json, statistics, sys
from collections import defaultdict

# =============================================
# ⚙️ 설정: 아래 경로를 실제 CSV 폴더 경로로 수정하세요
# =============================================
# 이 스크립트와 같은 폴더의 상위 폴더에 있는 'csv 변환 문서' 폴더 자동 탐색
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

def find_csv_folder():
    """CSV 폴더 자동 탐색"""
    candidates = [
        os.path.join(SCRIPT_DIR, '..', 'csv 변환 문서'),
        os.path.join(SCRIPT_DIR, 'csv 변환 문서'),
        os.path.join(os.path.expanduser('~'), 'Desktop', 'csv 변환 문서'),
        os.path.join(os.path.expanduser('~'), 'Documents', 'csv 변환 문서'),
    ]
    for c in candidates:
        if os.path.isdir(c):
            return os.path.abspath(c)
    return None

# 출력 JSON 파일 경로
OUTPUT_JSON = os.path.join(SCRIPT_DIR, '인삼단가_데이터.json')

# =============================================
# 품목 카테고리 정의
# =============================================
CATEGORIES = {
    '원삼': ['마편','별별대','별대','왕왕왕대','왕왕대','왕대','특대','대','중','소','믹서','믹사'],
    '삼계': ['대삼계','중삼계','소삼계','소삼계1','소삼계2','잔삼계','실실이','짠짠이','잔잔이'],
    '난발': ['별별난','별난','왕왕왕난','왕왕난','왕난','특난','대난','중난','소난','잔난','콩난','콩콩난','콩3난','콩4난','콩5난','콩콩콩난','콩6난'],
    '황다마': ['황별대','황왕대','황왕왕대','황특대','황대','황중','황소','황믹서','황믹사','황삼계','황난','황대난','황특난','황중난','황왕난','황소난','황대삼계'],
    '잡삼': ['파삼','파','깍기','막삼','흠서리','썩삼','절삼','중미','미삼','동가리','대동가리','중동가리','소동가리','잔동가리','특동가리','왕동가리','황동가리',
             '썩삼(알속)','절삼(알속)','동가리(알속)','대동가리(알속)','소동가리(알속)','중동가리(알속)','황동가리(알속)','흠서리(알속)','막삼(알속)'],
    '기타': ['묘삼','묘파','달랭이','세근파','세근','제무기','재무기','은피','은피(알속)']
}

item_to_cat = {item: cat for cat, items in CATEGORIES.items() for item in items}

def get_category(name):
    if name in item_to_cat: return item_to_cat[name]
    if name.startswith('황') and len(name) > 1: return '황다마'
    if '동가리' in name: return '잡삼'
    if re.match(r'콩\d+난', name): return '난발'
    return None

def parse_date(fname):
    m = re.match(r'^(\d{6})', fname)
    if m:
        d = m.group(1)
        yy, mm, dd = int(d[:2]), int(d[2:4]), int(d[4:6])
        if 1 <= mm <= 12 and 1 <= dd <= 31:
            return f"{2000+yy}-{mm:02d}-{dd:02d}"
    return None

def detect_csv_format(header_line):
    """
    CSV 헤더를 보고 포맷 감지:
    - '구매자'로 시작하면 2017+ 신형식 (item=col[1], price=col[8])
    - '이름'으로 시작하면 2016 이전 구형식 (item=col[0], price=col[7])
    반환: (item_col, price_col)
    """
    # BOM 제거 후 첫 번째 컬럼 확인
    first_col = header_line.lstrip('\ufeff').split(',')[0].strip().strip('"')
    if first_col == '구매자':
        return 1, 8   # 2017+ 신형식
    else:
        return 0, 7   # 2016 이전 구형식

def parse_csv(path):
    try:
        with open(path, 'rb') as f: content = f.read()
        text = None
        for enc in ['utf-8-sig', 'utf-8', 'euc-kr', 'cp949']:
            try: text = content.decode(enc); break
            except: pass
        if not text: return {}

        lines = text.strip().split('\n')
        if not lines: return {}

        # 헤더로 포맷 감지
        item_col, price_col = detect_csv_format(lines[0])

        result = {}
        skip_values = {'이름','0','','총차수','채당 단가','1차당 가격','구매자','무게'}
        skip_prices = {'0','','1차당 가격','채당 단가','총액'}

        for line in lines[2:]:   # 첫 2행(헤더+서브헤더) 건너뜀
            cols = line.split(',')
            if len(cols) <= max(item_col, price_col): continue
            item = cols[item_col].strip().strip('"')
            price_s = cols[price_col].strip().strip('"')
            if not item or item in skip_values: continue
            if not price_s or price_s in skip_prices: continue
            try:
                float(item)
                continue
            except: pass
            try:
                p = float(price_s.replace(',',''))
                if 3000 <= p <= 500000:
                    result[item] = p
            except: pass
        return result
    except: return {}

def should_skip_file(fname):
    """
    건너뛸 파일 판별.
    원래 코드는 '정산' 포함 파일 전부 스킵했으나,
    '최종정산', '정산완료' 등이 파일명에 들어간 정상 거래파일도 스킵되는 버그 수정.
    실제로 건너뛰어야 할 파일: 월별/연도별 합계 파일 등
    → 단순히 '정산'만으론 판별 불가하므로 날짜 패턴(YYMMDD)이 없는 파일만 스킵.
    """
    # 날짜 패턴이 있으면 정상 거래 파일
    if re.match(r'^\d{6}', fname):
        return False
    # 날짜 패턴 없으면 집계/요약 파일로 간주하고 스킵
    return True

def build_data(csv_folder):
    raw = defaultdict(lambda: defaultdict(list))
    cats = defaultdict(lambda: defaultdict(list))
    count = 0
    skipped_no_date = 0
    format_counts = {'old': 0, 'new': 0}

    for root, dirs, files in os.walk(csv_folder):
        for f in files:
            if not f.lower().endswith('.csv'): continue
            if should_skip_file(f):
                skipped_no_date += 1
                continue
            date = parse_date(f)
            if not date: continue
            fpath = os.path.join(root, f)
            # 포맷 감지용 헤더 읽기
            try:
                with open(fpath, 'rb') as fh:
                    raw_header = fh.readline()
                for enc in ['utf-8-sig','utf-8','euc-kr','cp949']:
                    try: header = raw_header.decode(enc); break
                    except: header = ''
                item_col, _ = detect_csv_format(header)
                format_counts['old' if item_col == 0 else 'new'] += 1
            except: pass

            prices = parse_csv(fpath)
            for item, price in prices.items():
                cat = get_category(item)
                if not cat: continue
                raw[date][item].append(int(price))
                cats[date][cat].append(int(price))
                count += 1

    avg = lambda lst: round(statistics.mean(lst)) if lst else None

    im = defaultdict(lambda: defaultdict(list))
    ia = defaultdict(lambda: defaultdict(list))
    cm = defaultdict(lambda: defaultdict(list))
    ca = defaultdict(lambda: defaultdict(list))

    for d, items in raw.items():
        for item, ps in items.items():
            im[item][d[:7]].extend(ps)
            ia[item][d[:4]].extend(ps)
    for d, cs in cats.items():
        for cat, ps in cs.items():
            cm[cat][d[:7]].extend(ps)
            ca[cat][d[:4]].extend(ps)

    out = {'categories': {}, 'items': {}, 'category_list': CATEGORIES, 'updated': str(__import__('datetime').datetime.now())[:10]}

    for cat in CATEGORIES:
        out['categories'][cat] = {
            'monthly': {m: avg(p) for m, p in sorted(cm[cat].items()) if avg(p)},
            'annual': {y: avg(p) for y, p in sorted(ca[cat].items()) if avg(p)}
        }

    for item in set(k for d in raw.values() for k in d):
        cat = get_category(item)
        if not cat: continue
        monthly = {m: avg(p) for m, p in sorted(im[item].items()) if avg(p)}
        annual = {y: avg(p) for y, p in sorted(ia[item].items()) if avg(p)}
        if len(annual) >= 2:
            out['items'][item] = {'category': cat, 'monthly': monthly, 'annual': annual}

    print(f"  처리된 레코드 수: {count:,}개")
    print(f"  추적된 품목 수: {len(out['items'])}개")
    print(f"  포맷별 처리: 구형식(2016이전)={format_counts['old']}개, 신형식(2017+)={format_counts['new']}개")
    if skipped_no_date:
        print(f"  날짜패턴 없어 건너뜀: {skipped_no_date}개")
    return out

if __name__ == '__main__':
    print("=" * 55)
    print("🌿  인삼 단가 데이터 업데이트")
    print("=" * 55)

    csv_folder = find_csv_folder()
    if not csv_folder:
        print("❌ CSV 폴더를 찾을 수 없습니다.")
        print("   스크립트 상단의 SCRIPT_DIR 근처 경로를 확인하세요.")
        sys.exit(1)

    print(f"📂 CSV 폴더: {csv_folder}")
    print("⚙️  데이터 분석 중... (수초 소요)")

    data = build_data(csv_folder)

    with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, separators=(',', ':'))

    size = os.path.getsize(OUTPUT_JSON) / 1024
    print(f"\n✅  완료! 파일 저장: {OUTPUT_JSON}")
    print(f"   파일 크기: {size:.1f} KB")
    print(f"\n👉  이제 '인삼단가분석_대시보드.html' 파일을 브라우저에서 새로고침하세요.")
    print(f"   (이미 열려있다면 Ctrl+R 또는 Cmd+R)")
