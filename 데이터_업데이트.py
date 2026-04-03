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
import os, re, json, statistics, sys, unicodedata
from collections import defaultdict

# =============================================
# ⚙️ 설정: 아래 경로를 실제 CSV 폴더 경로로 수정하세요
# =============================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

def find_csv_folder():
    """
    CSV 폴더 자동 탐색.
    macOS에서 os.walk가 NFD 인코딩 경로를 반환하므로
    부모 디렉토리를 listdir해서 실제 이름을 얻는 방식으로 탐색.
    """
    # 탐색할 부모 디렉토리 목록
    parent_candidates = [
        os.path.dirname(SCRIPT_DIR),          # 스크립트 상위 폴더 (마운트 루트)
        SCRIPT_DIR,
        os.path.expanduser('~/Desktop'),
        os.path.expanduser('~/Documents'),
    ]
    for parent in parent_candidates:
        if not os.path.isdir(parent):
            continue
        try:
            for name in os.listdir(parent):
                # NFC로 정규화해서 비교
                if unicodedata.normalize('NFC', name).lower() == 'csv 변환 문서':
                    full = os.path.join(parent, name)
                    if os.path.isdir(full):
                        return full
        except Exception:
            pass
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
    '황다마': ['황별대','황왕대','황왕왕대','황특대','황대','황중','황소','황믹서','황믹사','황삼계','황난','황대난','황특난','황중난','황왕난','황소난','황대삼계','황'],
    '잡삼': ['파삼','파','깍기','대깍기','막삼','흠서리','썩삼','절삼','중미','미삼','동가리','대동가리','중동가리','소동가리','잔동가리','특동가리','왕동가리','황동가리',
             '썩삼(알속)','절삼(알속)','동가리(알속)','대동가리(알속)','소동가리(알속)','중동가리(알속)','황동가리(알속)','흠서리(알속)','막삼(알속)'],
    '기타': ['묘삼','묘파','달랭이','세근파','세근','제무기','재무기','은피','은피(알속)']
}

item_to_cat = {item: cat for cat, items in CATEGORIES.items() for item in items}

def get_category(name):
    """품목명 → 카테고리. 복합('+') 품목은 첫 번째 유효 부품목 기준."""
    if not name:
        return None

    # 직접 매핑
    if name in item_to_cat:
        return item_to_cat[name]

    # 복합 품목 처리: '+' 또는 ',' 구분자로 나누어 첫 번째 매핑 가능한 부품목 사용
    for sep in ('+', ','):
        if sep in name:
            parts = [p.strip() for p in name.split(sep)]
            for part in parts:
                cat = get_category(part)   # 재귀 (단순 품목으로)
                if cat:
                    return cat
            break

    # 패턴 기반 폴백
    if name.startswith('황') and len(name) > 1:
        return '황다마'
    if '동가리' in name:
        return '잡삼'
    if re.match(r'콩\d+난', name):
        return '난발'

    return None

def parse_date(fname):
    m = re.match(r'^(\d{6})', fname)
    if m:
        d = m.group(1)
        yy, mm, dd = int(d[:2]), int(d[2:4]), int(d[4:6])
        if 1 <= mm <= 12 and 1 <= dd <= 31:
            return f"{2000+yy}-{mm:02d}-{dd:02d}"
    return None

def detect_csv_format(lines):
    """
    CSV 헤더를 분석해 (item_col, price_col, data_start) 반환.

    발견된 실제 포맷:
    - col[0] == '이름'          : item=0, price=7,  data_start=2 (서브헤더 있음)
    - col[1] == '이름' (기타)   : item=1, price=8,  data_start=2 or 1
    - col[2] == '이름' (기타)   : item=2, price=9,  data_start=2 or 1

    핵심 규칙:
    - price_col = item_col + 7   (모든 포맷에서 일정)
    - 서브헤더 여부: row1[item_col+1] == '1' 이면 서브헤더 있음 → data_start=2
                   아니면 서브헤더 없음 → data_start=1
    """
    if not lines:
        return 1, 8, 2

    # 헤더 행 파싱 (BOM, 따옴표, 공백 제거)
    header_cols = [c.strip().strip('"').lstrip('\ufeff') for c in lines[0].split(',')]

    # 첫 5열에서 '이름' 위치 탐색
    item_col = next((i for i, c in enumerate(header_cols[:5]) if c == '이름'), None)
    if item_col is None:
        item_col = 1   # 헤더 없는 형식 (blank / 구매자) → col[1] 기본

    price_col = item_col + 7  # 모든 포맷 공통 오프셋

    # 서브헤더 여부 확인
    data_start = 2
    if len(lines) > 1:
        sub_cols = lines[1].split(',')
        chk_idx = item_col + 1
        if chk_idx < len(sub_cols):
            val = sub_cols[chk_idx].strip().strip('"')
            if val != '1':
                data_start = 1   # 서브헤더 없음 → 2행부터 데이터

    return item_col, price_col, data_start

def parse_csv(path):
    """CSV 파일 파싱 → {품목명: 단가} dict 반환"""
    try:
        with open(path, 'rb') as f:
            content = f.read()
        text = None
        for enc in ['utf-8-sig', 'utf-8', 'euc-kr', 'cp949']:
            try:
                text = content.decode(enc)
                break
            except Exception:
                pass
        if not text:
            return {}

        lines = text.strip().split('\n')
        if not lines:
            return {}

        item_col, price_col, data_start = detect_csv_format(lines)

        result = {}
        skip_values = {'이름','0','','총차수','채당 단가','1차당 가격','구매자','무게',
                       '품목','합계','소계','합 계','소 계'}
        skip_prices = {'0','','1차당 가격','채당 단가','총액','합계','소계'}

        for line in lines[data_start:]:
            cols = line.split(',')
            if len(cols) <= max(item_col, price_col):
                continue
            item = cols[item_col].strip().strip('"')
            price_s = cols[price_col].strip().strip('"')
            if not item or item in skip_values:
                continue
            if not price_s or price_s in skip_prices:
                continue
            # 순수 숫자 품목명 제외
            try:
                float(item)
                continue
            except ValueError:
                pass
            # 단가 파싱
            try:
                p = float(price_s.replace(',', ''))
                if 3000 <= p <= 500000:
                    result[item] = p
            except ValueError:
                pass
        return result
    except Exception:
        return {}

def should_skip_file(fname):
    """
    날짜 패턴(YYMMDD)이 없는 파일은 집계/요약 파일로 간주해 건너뜀.
    macOS NFD/NFC 차이에도 안전하게 동작.
    """
    fname_nfc = unicodedata.normalize('NFC', fname)
    # 날짜 패턴 있으면 정상 거래 파일
    if re.match(r'^\d{6}', fname_nfc):
        return False
    return True

_SUFFIX_MAP = {
    '삼계': {'잔': '잔삼계', '소': '소삼계', '대': '대삼계', '중': '중삼계'},
    '난발': {'잔': '잔난', '소': '소난', '대': '대난', '중': '중난', '특': '특난', '왕': '왕난', '콩': '콩난'},
}

def _detect_group_context(parts):
    for p in parts:
        cat = get_category(p)
        if cat in ('삼계', '난발', '황다마'):
            return cat
        if p.endswith('삼계'): return '삼계'
        if p.endswith('난'): return '난발'
    return None

def expand_combined_item(name, price):
    """복합 품목을 개별 품목으로 분해. 문맥 인식 확장."""
    if '+' not in name:
        return [(name, price)]
    parts = [p.strip() for p in name.split('+')]
    results = []
    has_hwang_prefix = parts[0].startswith('황') if parts else False
    ctx = _detect_group_context(parts)
    for part in parts:
        if not part: continue
        resolved = part
        if has_hwang_prefix and not part.startswith('황'):
            hwang_ver = '황' + part
            if hwang_ver in item_to_cat:
                resolved = hwang_ver
        if get_category(resolved) is None and ctx in _SUFFIX_MAP:
            expanded = _SUFFIX_MAP[ctx].get(resolved)
            if expanded and get_category(expanded):
                resolved = expanded
        cat = get_category(resolved)
        if cat:
            results.append((resolved, price))
    return results if results else [(name, price)]

def build_data(csv_folder):
    raw = defaultdict(lambda: defaultdict(list))
    cats = defaultdict(lambda: defaultdict(list))
    count = 0
    skipped_no_date = 0
    format_stats = defaultdict(int)

    for root, dirs, files in os.walk(csv_folder):
        for f in files:
            if not f.lower().endswith('.csv'):
                continue
            if should_skip_file(f):
                skipped_no_date += 1
                continue
            date = parse_date(unicodedata.normalize('NFC', f))
            if not date:
                continue
            fpath = os.path.join(root, f)

            # 포맷 통계용 헤더 확인
            try:
                with open(fpath, 'rb') as fh:
                    raw_header = fh.readline()
                header_text = None
                for enc in ['utf-8-sig', 'utf-8', 'euc-kr', 'cp949']:
                    try:
                        header_text = raw_header.decode(enc)
                        break
                    except Exception:
                        pass
                if header_text:
                    ic, _, _ = detect_csv_format([header_text])
                    format_stats[f'item_col={ic}'] += 1
            except Exception:
                pass

            prices = parse_csv(fpath)
            for item, price in prices.items():
                # 복합 품목 분해: 각 개별 품목에 동일 단가 적용
                expanded = expand_combined_item(item, price)
                for sub_item, sub_price in expanded:
                    cat = get_category(sub_item)
                    if not cat:
                        continue
                    raw[date][sub_item].append(int(sub_price))
                    cats[date][cat].append(int(sub_price))
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

    out = {
        'categories': {},
        'items': {},
        'category_list': CATEGORIES,
        'updated': str(__import__('datetime').datetime.now())[:10]
    }

    for cat in CATEGORIES:
        out['categories'][cat] = {
            'monthly': {m: avg(p) for m, p in sorted(cm[cat].items()) if avg(p)},
            'annual':  {y: avg(p) for y, p in sorted(ca[cat].items()) if avg(p)}
        }

    for item in set(k for d in raw.values() for k in d):
        cat = get_category(item)
        if not cat:
            continue
        monthly = {m: avg(p) for m, p in sorted(im[item].items()) if avg(p)}
        annual  = {y: avg(p) for y, p in sorted(ia[item].items()) if avg(p)}
        if len(annual) >= 2:
            out['items'][item] = {'category': cat, 'monthly': monthly, 'annual': annual}

    print(f"  처리된 레코드 수: {count:,}개")
    print(f"  추적된 품목 수: {len(out['items'])}개")
    print(f"  포맷 분포: {dict(sorted(format_stats.items()))}")
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
    print("⚙️  데이터 분석 중... (수초~수십초 소요)")

    data = build_data(csv_folder)

    with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, separators=(',', ':'))

    size = os.path.getsize(OUTPUT_JSON) / 1024
    print(f"\n✅  완료! 파일 저장: {OUTPUT_JSON}")
    print(f"   파일 크기: {size:.1f} KB")
    print(f"\n👉  이제 '인삼단가분석_대시보드.html' 파일을 브라우저에서 새로고침하세요.")
    print(f"   (이미 열려있다면 Ctrl+R 또는 Cmd+R)")
