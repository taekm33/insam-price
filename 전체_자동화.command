#!/bin/bash
# ============================================================
# 🌿 인삼 단가 대시보드 - 전체 자동화 프로그램
# ============================================================
# 이 파일 하나만 더블클릭하면:
#   1. 📦 필요 패키지 자동 확인/설치
#   2. 🔄 .numbers / .xlsx → CSV 자동 변환 (새 파일만)
#   3. 📊 전체 데이터 분석 (모든 연도 자동 반영)
#   4. 🔑 GitHub 토큰 유효성 자동 확인
#   5. 📤 GitHub 자동 배포
#   6. 🌐 브라우저 자동 오픈
# ============================================================
# ※ numbers_parser 우선 / 구형 포맷은 Numbers.app 자동 사용
# ※ iCloud 파일 자동 다운로드 처리
# ============================================================

cd "$(dirname "$0")"
# 시스템 Python 우선 사용 (venv 간섭 방지)
if [ -x /usr/bin/python3 ]; then
    PYTHON=/usr/bin/python3
elif [ -x /usr/local/bin/python3 ]; then
    PYTHON=/usr/local/bin/python3
else
    PYTHON=python3
fi
$PYTHON - "$@" << 'PYTHON_EOF'
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os, sys, re, csv, json, time, statistics, subprocess, shutil, urllib.request, urllib.error
from collections import defaultdict
from datetime import datetime
from pathlib import Path

# ─── 실행 모드 ────────────────────────────────────────────────
HEADLESS   = '--headless' in sys.argv
SCRIPT_DIR = Path(os.getcwd())

# ─── 중복 실행 방지 (PID 락 파일) ─────────────────────────────
_LOCK_FILE = SCRIPT_DIR / '.insam_running.pid'
def _acquire_lock():
    if _LOCK_FILE.exists():
        try:
            old_pid = int(_LOCK_FILE.read_text().strip())
            # 해당 PID가 실제로 실행 중인지 확인
            os.kill(old_pid, 0)
            print(f"⚠️  이미 실행 중입니다 (PID {old_pid}). 종료합니다.")
            print(f"   강제 재시작: rm '{_LOCK_FILE}' 후 다시 실행")
            sys.exit(0)
        except (ProcessLookupError, ValueError):
            pass  # 이전 PID가 없으면 스테일 락 → 무시
    _LOCK_FILE.write_text(str(os.getpid()))

def _release_lock():
    try:
        _LOCK_FILE.unlink()
    except Exception:
        pass

import atexit
_acquire_lock()
atexit.register(_release_lock)

# ─── 품목 카테고리 ─────────────────────────────────────────────
CATEGORIES = {
    '원삼':  ['마편','별별대','별대','왕왕왕대','왕왕대','왕대','특대','대','중','소','믹서','믹사'],
    '삼계':  ['대삼계','중삼계','소삼계','소삼계1','소삼계2','잔삼계','실실이','짠짠이','잔잔이'],
    '난발':  ['별별난','별난','왕왕왕난','왕왕난','왕난','특난','대난','중난','소난','잔난','콩난','콩콩난','콩3난','콩4난','콩5난','콩콩콩난','콩6난'],
    '황다마': ['황별대','황왕대','황왕왕대','황특대','황대','황중','황소','황믹서','황믹사','황삼계','황난','황대난','황특난','황중난','황왕난','황소난','황대삼계','황'],
    '잡삼':  ['파삼','파','깍기','대깍기','막삼','흠서리','썩삼','절삼','중미','미삼','동가리','대동가리','중동가리','소동가리','잔동가리','특동가리','왕동가리','황동가리',
              '썩삼(알속)','절삼(알속)','동가리(알속)','대동가리(알속)','소동가리(알속)','중동가리(알속)','황동가리(알속)','흠서리(알속)','막삼(알속)'],
    '기타':  ['묘삼','묘파','달랭이','세근파','세근','제무기','재무기','은피','은피(알속)']
}
item_to_cat = {item: cat for cat, items in CATEGORIES.items() for item in items}

# ════════════════════════════════════════════════════════════
#  로깅
# ════════════════════════════════════════════════════════════
LOG_FILE = SCRIPT_DIR / '자동화_로그.txt'

def log(msg, echo=True):
    ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    line = f"[{ts}] {msg}"
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(line + '\n')
    if echo:
        print(msg)

def sep():    log("─" * 52)
def ok(s):    log(f"  ✅  {s}")
def info(s):  log(f"  ℹ️   {s}")
def warn(s):  log(f"  ⚠️   {s}")
def err(s):   log(f"  ❌  {s}")
def banner(title, icon="🌿"):
    log(f"\n{'═'*52}")
    log(f"  {icon}  {title}")
    log(f"{'═'*52}")

def pause(msg="  Enter 키를 누르면 닫힙니다..."):
    if not HEADLESS:
        try:
            sys.stdout.write(msg)
            sys.stdout.flush()
            with open('/dev/tty') as tty:
                tty.readline()
        except Exception:
            pass

# ════════════════════════════════════════════════════════════
#  설정 파일 읽기
# ════════════════════════════════════════════════════════════
def load_config() -> dict:
    conf_path = SCRIPT_DIR / '.인삼_설정.conf'
    if not conf_path.exists():
        return {}
    cfg = {}
    for line in conf_path.read_text(encoding='utf-8').splitlines():
        line = line.strip()
        if '=' in line and not line.startswith('#'):
            k, v = line.split('=', 1)
            cfg[k.strip()] = v.strip()
    return cfg

# ════════════════════════════════════════════════════════════
#  GitHub 토큰 유효성 확인
# ════════════════════════════════════════════════════════════
def check_token(token):
    try:
        req = urllib.request.Request(
            'https://api.github.com/user',
            headers={
                'Authorization': f'token {token}',
                'User-Agent': 'insam-price-dashboard'
            }
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            user = json.loads(r.read().decode())
            return True, user.get('login', 'OK')
    except urllib.error.HTTPError as e:
        if e.code == 401:
            return False, '토큰이 만료되었거나 잘못된 토큰입니다 (HTTP 401)'
        return False, f'GitHub API 오류: HTTP {e.code}'
    except Exception as e:
        return False, f'네트워크 오류: {e}'

def handle_expired_token():
    err("GitHub 토큰이 만료되었습니다!")
    log("")
    log("  ┌─────────────────────────────────────────────┐")
    log("  │  🔑 토큰 재발급 방법                         │")
    log("  │                                             │")
    log("  │  1. 아래 주소에서 새 토큰을 발급받으세요:     │")
    log("  │     https://github.com/settings/tokens/new  │")
    log("  │                                             │")
    log("  │  2. 설정:                                   │")
    log("  │     - Note: insam-price                     │")
    log("  │     - Expiration: No expiration  ← 중요!    │")
    log("  │     - Scope: repo 체크            ← 중요!   │")
    log("  │                                             │")
    log("  │  3. 생성된 토큰 복사 후                      │")
    log("  │     '토큰_업데이트.command' 를 실행하세요    │")
    log("  └─────────────────────────────────────────────┘")
    log("")
    if not HEADLESS:
        ans = input("  지금 바로 GitHub 토큰 발급 페이지를 열까요? (y/n): ").strip().lower()
        if ans == 'y':
            url = 'https://github.com/settings/tokens/new?description=insam-price&scopes=repo'
            subprocess.Popen(['open', url])
    pause()
    sys.exit(1)

# ════════════════════════════════════════════════════════════
#  패키지 설치
# ════════════════════════════════════════════════════════════
def ensure_packages():
    needed = []
    try:
        import numbers_parser
    except ImportError:
        needed.append('numbers-parser')
    try:
        import openpyxl
    except ImportError:
        needed.append('openpyxl')
    try:
        import xlrd
    except ImportError:
        needed.append('xlrd')
    if not needed:
        return
    log(f"  📦 필요 패키지 설치 중: {', '.join(needed)}")
    for pkg in needed:
        r = subprocess.run(
            [sys.executable, '-m', 'pip', 'install', pkg, '--quiet', '--break-system-packages'],
            capture_output=True
        )
        if r.returncode != 0:
            subprocess.run([sys.executable, '-m', 'pip', 'install', pkg, '--quiet'], capture_output=True)
    # 설치 후 importlib 캐시 무효화 및 모듈 재로드
    import importlib
    importlib.invalidate_caches()
    # site-packages 경로를 sys.path에 추가 (pip 설치 경로가 다를 수 있음)
    import site
    site.main()
    for mod_name in ['numbers_parser', 'openpyxl', 'xlrd']:
        try:
            importlib.import_module(mod_name)
        except ImportError:
            pass
    ok("패키지 준비 완료")

# ════════════════════════════════════════════════════════════
#  폴더 탐색
# ════════════════════════════════════════════════════════════
def find_numbers_folder():
    """iCloud Numbers 앱 폴더 탐색 (원본 .numbers 파일 위치)"""
    candidates = [
        # Numbers 앱 iCloud 전용 경로 (가장 일반적)
        Path.home() / 'Library' / 'Mobile Documents' / 'com~apple~Numbers' / 'Documents',
        # iCloud Drive 안의 Numbers 폴더
        Path.home() / 'Library' / 'Mobile Documents' / 'com~apple~iCloud' / 'Numbers',
        # 홈 폴더 직접 접근
        Path('/Users') / os.environ.get('USER', 'taekyungmin') / 'Library' / 'Mobile Documents' / 'com~apple~Numbers' / 'Documents',
    ]
    for c in candidates:
        if c.is_dir():
            return c
    # shell로 최종 확인
    try:
        mobile_docs = Path.home() / 'Library' / 'Mobile Documents'
        r = subprocess.run(['/bin/ls', str(mobile_docs)], capture_output=True, text=True, timeout=5)
        for line in r.stdout.strip().split('\n'):
            if 'Numbers' in line:
                p = mobile_docs / line.strip() / 'Documents'
                if p.is_dir():
                    return p
    except Exception:
        pass
    return None

def find_csv_folder():
    """CSV 변환 결과를 저장하는 폴더 탐색 (출력 폴더)"""
    candidates = [
        SCRIPT_DIR.parent / 'csv 변환 문서',
        Path.home() / 'Desktop' / 'csv 변환 문서',
        SCRIPT_DIR / 'csv 변환 문서',
        Path.home() / 'Documents' / 'csv 변환 문서',
    ]
    for c in candidates:
        if c.is_dir():
            return c
    return None

def find_year_folders(base: Path) -> dict:
    """YYYY 정산완료 폴더 탐색 (자동으로 새 연도 인식 + iCloud 동기화)"""
    # iCloud stub 폴더 강제 다운로드 트리거
    try:
        subprocess.run(['brctl', 'download', str(base)], capture_output=True, timeout=30)
        time.sleep(2)
    except Exception:
        pass

    result = {}

    def _scan(items_iter):
        for item in items_iter:
            if not item.is_dir():
                continue
            import unicodedata
            name_nfc = unicodedata.normalize('NFC', item.name)
            m = re.match(r'^(\d{4})\s*정산완료', name_nfc)
            if m:
                result[int(m.group(1))] = item

    try:
        items = list(base.iterdir())
        _scan(iter(items))

        # iterdir가 비어있으면 shell ls로 재시도 (iCloud 미동기화 대응)
        if not result:
            r = subprocess.run(['/bin/ls', '-1', str(base)], capture_output=True, text=True, timeout=15)
            shell_names = [n for n in r.stdout.strip().split('\n') if n]
            if shell_names:
                log(f"  ℹ️   shell ls로 재탐색: {len(shell_names)}개 항목 발견")
                for name in shell_names:
                    p = base / name
                    import unicodedata
                    name_nfc = unicodedata.normalize('NFC', name)
                    m = re.match(r'^(\d{4})\s*정산완료', name_nfc)
                    if m:
                        result[int(m.group(1))] = p
    except Exception as e:
        err(f"폴더 탐색 오류: {e}")

    return dict(sorted(result.items()))

def normalize(name: str) -> str:
    """macOS NFD/NFC 차이를 무시하고 파일명 비교"""
    import unicodedata
    return unicodedata.normalize('NFC', name)

# ════════════════════════════════════════════════════════════
#  iCloud 파일 다운로드 보장
# ════════════════════════════════════════════════════════════
def ensure_local_file(path: Path, max_wait: int = 30) -> bool:
    """
    iCloud에 저장된 파일이 로컬에 다운로드돼 있는지 확인.
    macOS HFS+ 는 한국어 파일명을 NFD로 저장하지만 Python Path 는 NFC로
    경로를 구성하므로, 스텁 탐지는 iterdir() 기반으로 NFD/NFC 무관하게 수행.
    스텁 발견 시 brctl download 트리거 후 max_wait 초 대기.
    Numbers.app 을 절대 열지 않음.
    """
    import unicodedata

    # ① 파일/패키지가 로컬에 있는지 직접 확인
    # .numbers 파일은 디렉토리(패키지) 형태 → st_size ≈ 256 이므로 별도 처리
    try:
        st = path.stat()
        if st.st_size >= 512:
            return True
        if path.is_dir() and (path / 'Index.zip').exists():
            return True  # .numbers 패키지: Index.zip 존재 = 로컬에 있음
    except Exception:
        pass  # 로컬에 없음 → evicted 또는 존재하지 않음

    # ② .icloud 스텁을 iterdir()로 탐색 (NFD/NFC 차이 우회)
    stub_target = unicodedata.normalize('NFC', f".{path.name}.icloud")
    stub_exists = False
    try:
        for f in path.parent.iterdir():
            if unicodedata.normalize('NFC', f.name) == stub_target:
                stub_exists = True
                break
    except Exception:
        pass

    if not stub_exists:
        return False  # 스텁도 없음 → 파일 자체가 없는 것

    # ③ brctl download 트리거
    try:
        subprocess.run(['brctl', 'download', str(path)],
                       capture_output=True, timeout=5)
    except Exception:
        pass

    # ④ max_wait초 동안 1초 간격으로 파일이 로컬에 나타나길 대기
    for _ in range(max_wait):
        time.sleep(1)
        try:
            if path.stat().st_size >= 512:
                return True
        except Exception:
            pass

    return False  # 타임아웃 — iCloud 다운로드 미완료

# ════════════════════════════════════════════════════════════
#  파일 변환 (.numbers / .xlsx → .csv)
# ════════════════════════════════════════════════════════════
def cell_to_str(cell) -> str:
    try:
        val = cell.value
    except Exception:
        return ''
    if val is None:
        return ''
    if isinstance(val, float):
        return str(int(val)) if val == int(val) else str(val)
    if isinstance(val, datetime):
        return val.strftime('%Y-%m-%d')
    return str(val)

def convert_numbers(src, dst):
    """
    .numbers 파일 → .csv 변환.
    Numbers.app 을 열지 않고 numbers_parser(순수 Python) 사용.
    반환: (성공여부, 실패사유)
    """
    # 1. iCloud 스텁이면 먼저 다운로드
    if not ensure_local_file(src):
        return False, "iCloud 다운로드 실패 (타임아웃)"

    try:
        import numbers_parser
        doc = numbers_parser.Document(str(src))
        sheet = doc.sheets[0]
        table = sheet.tables[0]
        rows_out = []
        for row in table.iter_rows():
            rows_out.append([cell_to_str(c) for c in row])
        # 빈 마지막 행 제거
        while rows_out and all(v == '' for v in rows_out[-1]):
            rows_out.pop()
        if not rows_out:
            return False, "빈 파일"
        with open(dst, 'w', encoding='utf-8-sig', newline='') as f:
            csv.writer(f).writerows(rows_out)
        return True, ""
    except ImportError:
        return False, "numbers_parser 미설치"
    except Exception as e:
        err_msg = str(e)
        if ('Pre-BNC' in err_msg or 'pre-bnc' in err_msg.lower()
                or 'invalid IWA' in err_msg):
            return False, '__OLD_FORMAT__'   # → batch_convert_via_applescript 로 처리
        return False, err_msg[:80]

def convert_xlsx(src, dst):
    """
    .xlsx / .xls 파일 → .csv 변환.
    .xls (구형 Excel) → xlrd 사용
    .xlsx (신형 Excel) → openpyxl 사용
    반환: (성공여부, 실패사유)
    """
    ext = src.suffix.lower()

    # 구형 .xls 파일은 xlrd로 처리
    if ext == '.xls':
        try:
            import xlrd
            wb = xlrd.open_workbook(str(src))
            ws = wb.sheet_by_index(0)
            rows_out = []
            for rx in range(ws.nrows):
                cells = []
                for cx in range(ws.ncols):
                    cell = ws.cell(rx, cx)
                    if cell.ctype == xlrd.XL_CELL_EMPTY:
                        cells.append('')
                    elif cell.ctype == xlrd.XL_CELL_NUMBER:
                        v = cell.value
                        cells.append(str(int(v)) if v == int(v) else str(v))
                    elif cell.ctype == xlrd.XL_CELL_DATE:
                        try:
                            dt = xlrd.xldate_as_tuple(cell.value, wb.datemode)
                            cells.append(f"{dt[0]:04d}-{dt[1]:02d}-{dt[2]:02d}")
                        except Exception:
                            cells.append(str(cell.value))
                    else:
                        cells.append(str(cell.value))
                rows_out.append(cells)
            while rows_out and all(v == '' for v in rows_out[-1]):
                rows_out.pop()
            if not rows_out:
                return False, "빈 파일"
            with open(dst, 'w', encoding='utf-8-sig', newline='') as f:
                csv.writer(f).writerows(rows_out)
            return True, ""
        except ImportError:
            return False, "xlrd 미설치 (.xls 파일용)"
        except Exception as e:
            return False, str(e)[:80]

    # 신형 .xlsx 파일은 openpyxl로 처리
    try:
        import openpyxl
        wb = openpyxl.load_workbook(str(src), data_only=True)
        ws = wb.active
        rows_out = []
        for row in ws.iter_rows(values_only=True):
            cells = []
            for v in row:
                if v is None:
                    cells.append('')
                elif isinstance(v, float) and v == int(v):
                    cells.append(str(int(v)))
                elif isinstance(v, datetime):
                    cells.append(v.strftime('%Y-%m-%d'))
                else:
                    cells.append(str(v))
            rows_out.append(cells)
        while rows_out and all(v == '' for v in rows_out[-1]):
            rows_out.pop()
        if not rows_out:
            return False, "빈 파일"
        with open(dst, 'w', encoding='utf-8-sig', newline='') as f:
            csv.writer(f).writerows(rows_out)
        return True, ""
    except Exception as e:
        return False, str(e)[:80]

# ════════════════════════════════════════════════════════════
#  구형 Numbers 파일 → Numbers.app + osascript 변환
#  (업그레이드 다이얼로그 자동 클릭 지원)
# ════════════════════════════════════════════════════════════
def _applescript_open_one(src, dst):
    """
    구형(Pre-BNC) .numbers 파일 한 개를 Numbers.app으로 열어 CSV 내보내기.

    핵심 설계:
    - 메인 osascript: 파일 열기 → 문서 로드 대기 → CSV 내보내기
    - 별도 Python 쓰레드: 0.3초 간격으로 독립 osascript를 실행해 Numbers
      시트(모달 다이얼로그)의 button 1을 클릭 (유지/OK 등).
      같은 osascript 내에서 System Events를 호출하면 Apple Event 큐 데드락이
      발생할 수 있으므로, 반드시 독립 프로세스로 처리.

    Returns: (성공여부, 실패사유)
    """
    import tempfile, shutil as _shutil, threading

    tmp_dir  = Path(tempfile.mkdtemp())
    tmp_csv  = tmp_dir / "out.csv"
    src_esc  = str(src).replace("'", "'\\''")   # shell single-quote escape
    tmp_esc  = str(tmp_csv).replace('"', '\\"')  # AppleScript double-quote escape

    # ── 배경 쓰레드: 독립 osascript로 다이얼로그 button 1 클릭 ────────────
    _stop_clicker = threading.Event()

    def _dialog_clicker():
        # 다이얼로그 sheet가 있을 때:
        #   1. 체크박스(버전 선택)가 있으면 checkbox 1 클릭 → "유지" 버튼 활성화
        #   2. "유지" 버튼(button 1)이 활성화되면 클릭
        click_script = (
            'tell application "System Events"\n'
            '  tell process "Numbers"\n'
            '    try\n'
            '      set w to front window\n'
            '      if exists (sheet 1 of w) then\n'
            '        set s to sheet 1 of w\n'
            '        -- 체크박스 그룹이 있으면 첫번째 체크박스 선택\n'
            '        try\n'
            '          if (count of groups of s) > 0 then\n'
            '            set grp to group 1 of s\n'
            '            if (count of checkboxes of grp) > 0 then\n'
            '              if (value of checkbox 1 of grp) = 0 then\n'
            '                click checkbox 1 of grp\n'
            '              end if\n'
            '            end if\n'
            '          end if\n'
            '        end try\n'
            '        -- "유지" 버튼(button 1) 클릭\n'
            '        try\n'
            '          if enabled of button 1 of s then\n'
            '            click button 1 of s\n'
            '          end if\n'
            '        end try\n'
            '      end if\n'
            '    end try\n'
            '  end tell\n'
            'end tell'
        )
        while not _stop_clicker.is_set():
            try:
                subprocess.run(
                    ['osascript', '-e', click_script],
                    capture_output=True, timeout=3
                )
            except Exception:
                pass
            time.sleep(0.3)

    clicker_thread = threading.Thread(target=_dialog_clicker, daemon=True)
    clicker_thread.start()

    # ── 메인 AppleScript: 열기 → 대기 → 내보내기 ─────────────────────────
    script = f"""
-- ① 이미 열린 Numbers 문서 모두 닫기 (안전하게)
tell application "Numbers"
    repeat 10 times
        if (count of documents) = 0 then exit repeat
        try
            close document 1 saving no
        on error
            exit repeat
        end try
        delay 0.3
    end repeat
end tell

-- ② Numbers 활성화 후 파일을 비동기(셸)로 열기
tell application "Numbers"
    activate
end tell
do shell script "open -a Numbers " & quoted form of "{src_esc}"
delay 2

-- ③ 최대 55초: 문서 로드 대기
--    다이얼로그 클릭은 Python 쓰레드(별도 osascript)가 처리
set elapsed to 0
set docReady to false
repeat while elapsed < 55
    delay 1
    set elapsed to elapsed + 1
    tell application "Numbers"
        try
            if (count of documents) > 0 then
                if (count of sheets of document 1) > 0 then
                    set docReady to true
                    exit repeat
                end if
            end if
        end try
    end tell
end repeat

-- ④ 내보내기
if not docReady then
    tell application "Numbers"
        try
            if (count of documents) > 0 then close document 1 saving no
        end try
    end tell
    error "문서 열기 실패 (타임아웃)"
end if

tell application "Numbers"
    export document 1 to POSIX file "{tmp_esc}" as CSV
    close document 1 saving no
end tell
"""

    try:
        result = subprocess.run(
            ['osascript', '-e', script],
            capture_output=True, text=True, timeout=90
        )
    except subprocess.TimeoutExpired:
        result = None
    except Exception as e:
        result = None
        warn(f"    osascript 예외: {e}")
    finally:
        _stop_clicker.set()  # 클리커 쓰레드 중지
        clicker_thread.join(timeout=2)

    # ── 결과 수집 ─────────────────────────────────────────────────────────
    if result is None:
        # 타임아웃 시 Numbers 강제 정리
        try:
            subprocess.run(
                ['osascript', '-e',
                 'tell application "Numbers"\n'
                 '  try\n'
                 '    if (count of documents) > 0 then close document 1 saving no\n'
                 '  end try\n'
                 'end tell'],
                capture_output=True, timeout=10
            )
        except Exception:
            pass
        _shutil.rmtree(tmp_dir, ignore_errors=True)
        return False, "타임아웃 (90초)"

    if tmp_csv.is_dir():
        csvs = sorted(tmp_csv.glob('*.csv'),
                      key=lambda f: f.stat().st_size, reverse=True)
        if csvs:
            _shutil.copy2(csvs[0], dst)
            _shutil.rmtree(tmp_dir, ignore_errors=True)
            return True, ""
    elif tmp_csv.is_file() and tmp_csv.stat().st_size > 0:
        _shutil.copy2(tmp_csv, dst)
        _shutil.rmtree(tmp_dir, ignore_errors=True)
        return True, ""

    stderr = (result.stderr or "").strip()
    _shutil.rmtree(tmp_dir, ignore_errors=True)
    return False, stderr[:120] if stderr else "CSV 파일 생성 안 됨"


def batch_convert_via_applescript(pairs, label):
    """
    구형 포맷 .numbers 파일 목록을 Numbers.app으로 순차 변환.
    각 파일마다 업그레이드 다이얼로그를 System Events로 자동 처리.
    Returns: (converted, failed, error_details)
    """
    total_conv, total_err, total_details = 0, 0, []
    n = len(pairs)

    info(f"  Numbers.app 구형 포맷 변환: {label} ({n}개) — 파일별 순차 처리")

    # Numbers.app 미리 실행 (첫 파일 열기 전 초기화 시간 단축)
    try:
        subprocess.run(['open', '-a', 'Numbers'],
                       capture_output=True, timeout=10)
        time.sleep(2)
    except Exception:
        pass

    for idx, (src, dst) in enumerate(pairs):
        info(f"  [{idx+1}/{n}] {src.name} ...")
        ok_flag, reason = _applescript_open_one(src, dst)
        if ok_flag:
            total_conv += 1
            ok(f"    → 변환 완료")
        else:
            total_err += 1
            total_details.append(f"{src.name}: {reason}")
            warn(f"    → 실패: {reason}")

    # 모든 변환 후 Numbers.app 종료
    try:
        subprocess.run(
            ['osascript', '-e', 'tell application "Numbers" to quit'],
            capture_output=True, timeout=15
        )
    except Exception:
        pass

    return total_conv, total_err, total_details

# ════════════════════════════════════════════════════════════
#  파일 변환 메인 (신규 파일만, 이미 CSV 있으면 스킵)
# ════════════════════════════════════════════════════════════
def convert_new_files(src_year_folders, dst_base):
    """
    원본 폴더(Numbers iCloud)의 .numbers/.xlsx를 찾아
    출력 폴더(csv 변환 문서)에 CSV로 저장.
    - 이미 동일 이름의 .csv가 있으면 건너뜀 (중복 변환 방지)
    - 출력 연도 폴더 없으면 자동 생성
    - iCloud 스텁 파일 자동 다운로드 처리
    """
    total_conv, total_err = 0, 0

    for year, src_folder in src_year_folders.items():
        year_conv, year_err = 0, 0
        err_details = []
        applescript_queue = []   # 구형 포맷 파일 (src, dst) — batch_convert로 처리

        # 출력(CSV) 폴더 준비 — 없으면 생성
        dst_folder = dst_base / f"{year} 정산완료"
        dst_folder.mkdir(parents=True, exist_ok=True)

        # 원본 파일 목록 (iCloud stub 포함)
        all_files = list_folder_with_icloud(src_folder)
        if not all_files:
            warn(f"{year}년 원본 폴더 비어있음 (iCloud 미동기화 가능성)")
            continue

        # 이미 존재하는 CSV stems (NFD/NFC 정규화로 안전 비교) — 출력 폴더 기준
        dst_files = list_folder_with_icloud(dst_folder)
        existing_csv_stems = {
            normalize(f.stem)
            for f in dst_files
            if f.suffix.lower() == '.csv'
        }

        # 원본 파일(.numbers/.xlsx) 탐색 및 변환
        for src in sorted(all_files):
            ext = src.suffix.lower()
            if ext not in ('.numbers', '.xlsx', '.xls'):
                continue
            if '정산' in src.stem:
                continue  # '정산' 포함 원본 파일은 건너뜀

            # 이미 CSV가 있으면 스킵
            if normalize(src.stem) in existing_csv_stems:
                continue

            # 출력 경로는 dst_folder 안으로
            dst = dst_folder / (src.stem + '.csv')

            if ext == '.numbers':
                success, reason = convert_numbers(src, dst)
                if not success and reason == '__OLD_FORMAT__':
                    applescript_queue.append((src, dst))   # 큐에 보류
                    continue
            else:
                success, reason = convert_xlsx(src, dst)

            if success:
                year_conv += 1
                existing_csv_stems.add(normalize(src.stem))
            else:
                year_err += 1
                err_details.append(f"{src.name}: {reason}")

        # ── 구형 포맷 파일 Numbers.app 일괄 변환 ──────────────────────
        if applescript_queue:
            info(f"  {year}년 구형 포맷 {len(applescript_queue)}개 → "
                 f"Numbers.app 일괄 변환 시작...")
            a_conv, a_err, a_details = batch_convert_via_applescript(
                applescript_queue, f"{year}년")
            year_conv += a_conv
            year_err  += a_err
            err_details.extend(a_details)
            # 성공 파일 stems 갱신
            for src, dst in applescript_queue:
                if dst.exists():
                    existing_csv_stems.add(normalize(src.stem))

        if year_conv > 0:
            ok(f"{year}년: {year_conv}개 신규 변환 완료")
        if year_err > 0:
            warn(f"{year}년: {year_err}개 변환 실패")
            for d in err_details[:5]:  # 최대 5개만 출력
                log(f"      ▸ {d}")
            if len(err_details) > 5:
                log(f"      ▸ ... 외 {len(err_details)-5}개")

        total_conv += year_conv
        total_err  += year_err

    return total_conv, total_err

# ════════════════════════════════════════════════════════════
#  폴더 현황 요약
# ════════════════════════════════════════════════════════════
def list_folder_with_icloud(folder: Path) -> list:
    """
    폴더 내 파일 목록 반환. iCloud stub(.icloud) 파일도 인식.
    .NAME.EXT.icloud → NAME.EXT 로 실제 경로 반환.
    """
    try:
        raw = list(folder.iterdir())
    except Exception:
        return []
    resolved = []
    for f in raw:
        name = f.name
        if name.startswith('.') and name.endswith('.icloud'):
            # iCloud stub: .REALNAME.icloud → REALNAME
            real_name = name[1:-len('.icloud')]
            real_path = folder / real_name
            resolved.append(real_path)
        else:
            resolved.append(f)
    return resolved

def summarize_folders(src_year_folders: dict, dst_base: Path):
    """각 연도별 원본(Numbers)/CSV(csv 변환 문서) 파일 개수 출력"""
    total_src, total_csv = 0, 0
    for year, src_folder in src_year_folders.items():
        src_files = list_folder_with_icloud(src_folder)
        src_cnt = sum(1 for f in src_files if f.suffix.lower() in ('.numbers', '.xlsx', '.xls')
                      and '정산' not in f.stem)

        dst_folder = dst_base / f"{year} 정산완료"
        dst_files = list_folder_with_icloud(dst_folder) if dst_folder.exists() else []
        csv_cnt = sum(1 for f in dst_files if f.suffix.lower() == '.csv')

        total_src += src_cnt
        total_csv += csv_cnt
        status = "✅" if src_cnt == csv_cnt and src_cnt > 0 else ("⚠️ " if csv_cnt > 0 else "❌")
        log(f"    {status} {year}년: 원본 {src_cnt}개 / CSV {csv_cnt}개")
    log(f"    {'─'*35}")
    log(f"    합계: 원본 {total_src}개 / CSV {total_csv}개")
    return total_src, total_csv

# ════════════════════════════════════════════════════════════
#  데이터 분석 (CSV → JSON)
# ════════════════════════════════════════════════════════════
def get_category(name):
    """품목명 → 카테고리. 복합('+') 품목은 첫 번째 유효 부품목 기준."""
    if not name:
        return None
    if name in item_to_cat:
        return item_to_cat[name]
    # 복합 품목 처리: '+' 또는 ',' 구분자
    for sep_char in ('+', ','):
        if sep_char in name:
            parts = [p.strip() for p in name.split(sep_char)]
            for part in parts:
                cat = get_category(part)
                if cat:
                    return cat
            break
    # 패턴 기반 폴백
    if name.startswith('황') and len(name) > 1: return '황다마'
    if '동가리' in name: return '잡삼'
    if re.match(r'콩\d+난', name): return '난발'
    return None

def parse_date(fname):
    import unicodedata
    fname = unicodedata.normalize('NFC', fname)
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
    핵심 규칙: price_col = item_col + 7
    서브헤더 여부: row1[item_col+1] == '1' 이면 data_start=2, 아니면 1
    """
    if not lines:
        return 1, 8, 2
    header_cols = [c.strip().strip('"').lstrip('\ufeff') for c in lines[0].split(',')]
    # 첫 5열에서 '이름' 위치 탐색
    item_col = next((i for i, c in enumerate(header_cols[:5]) if c == '이름'), None)
    if item_col is None:
        item_col = 1   # 헤더 없는 형식 → col[1] 기본
    price_col = item_col + 7  # 모든 포맷 공통 오프셋
    # 서브헤더 여부 확인
    data_start = 2
    if len(lines) > 1:
        sub_cols = lines[1].split(',')
        chk_idx = item_col + 1
        if chk_idx < len(sub_cols):
            val = sub_cols[chk_idx].strip().strip('"')
            if val != '1':
                data_start = 1
    return item_col, price_col, data_start

def parse_csv(path: Path) -> dict:
    try:
        content = path.read_bytes()
        text = None
        for enc in ['utf-8-sig', 'utf-8', 'euc-kr', 'cp949']:
            try: text = content.decode(enc); break
            except: pass
        if not text: return {}
        lines = text.strip().split('\n')
        if not lines: return {}
        item_col, price_col, data_start = detect_csv_format(lines)
        result = {}
        skip_items   = {'이름','0','','총차수','채당 단가','1차당 가격','구매자','무게',
                        '품목','합계','소계','합 계','소 계'}
        skip_prices  = {'0','','1차당 가격','채당 단가','총액','합계','소계'}
        for line in lines[data_start:]:
            cols = line.split(',')
            if len(cols) <= max(item_col, price_col): continue
            item    = cols[item_col].strip().strip('"')
            price_s = cols[price_col].strip().strip('"')
            if not item or item in skip_items: continue
            if not price_s or price_s in skip_prices: continue
            try:
                float(item); continue
            except: pass
            try:
                p = float(price_s.replace(',', ''))
                if 3_000 <= p <= 500_000:
                    result[item] = p
            except: pass
        return result
    except: return {}

# 삼계/난발 약칭 확장 맵: 복합품목에서 축약된 이름을 정식 이름으로 복원
_SUFFIX_MAP = {
    '삼계': {'잔': '잔삼계', '소': '소삼계', '대': '대삼계', '중': '중삼계'},
    '난발': {'잔': '잔난', '소': '소난', '대': '대난', '중': '중난', '특': '특난', '왕': '왕난', '콩': '콩난'},
}

def _detect_group_context(parts):
    """복합 품목 내 다른 파트를 보고 카테고리 문맥 추정"""
    for p in parts:
        cat = get_category(p)
        if cat in ('삼계', '난발', '황다마'):
            return cat
        if p.endswith('삼계'):
            return '삼계'
        if p.endswith('난'):
            return '난발'
    return None

def expand_combined_item(name, price):
    """
    복합 품목(A+B, A+B+C)을 개별 품목으로 분해.
    같은 단가로 함께 판매된 것이므로 각 품목에 동일 단가 적용.
    문맥 인식: "잔+실실이" → "잔삼계"+"실실이" (삼계 문맥)
              "황대+중" → "황대"+"황중" (황다마 문맥)
    반환: [(품목명, 단가), ...]
    """
    if '+' not in name:
        return [(name, price)]
    parts = [p.strip() for p in name.split('+')]
    results = []

    # 문맥 감지
    has_hwang_prefix = parts[0].startswith('황') if parts else False
    ctx = _detect_group_context(parts)

    for part in parts:
        if not part:
            continue
        resolved = part

        # 1. 황다마 접두어 전파: "황대+중" → "황중"
        if has_hwang_prefix and not part.startswith('황'):
            hwang_ver = '황' + part
            if hwang_ver in item_to_cat:
                resolved = hwang_ver

        # 2. 삼계/난발 약칭 확장: "잔+실실이" → "잔삼계"
        if get_category(resolved) is None and ctx in _SUFFIX_MAP:
            expanded = _SUFFIX_MAP[ctx].get(resolved)
            if expanded and get_category(expanded):
                resolved = expanded

        cat = get_category(resolved)
        if cat:
            results.append((resolved, price))

    return results if results else [(name, price)]

def build_data(csv_folder):
    raw  = defaultdict(lambda: defaultdict(list))
    cats = defaultdict(lambda: defaultdict(list))
    count = 0
    for root, dirs, files in os.walk(csv_folder):
        for fname in files:
            if not fname.lower().endswith('.csv'): continue
            if not re.match(r'^\d{6}', fname): continue
            date = parse_date(fname)
            if not date: continue
            prices = parse_csv(Path(root) / fname)
            for item, price in prices.items():
                # 복합 품목 분해: 각 개별 품목에 동일 단가 적용
                expanded = expand_combined_item(item, price)
                for sub_item, sub_price in expanded:
                    cat = get_category(sub_item)
                    if not cat: continue
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
    out = {'categories': {}, 'items': {}, 'category_list': CATEGORIES,
           'updated': str(datetime.now())[:10]}
    for cat in CATEGORIES:
        out['categories'][cat] = {
            'monthly': {m: avg(p) for m, p in sorted(cm[cat].items()) if avg(p)},
            'annual':  {y: avg(p) for y, p in sorted(ca[cat].items()) if avg(p)}
        }
    for item in set(k for d in raw.values() for k in d):
        cat = get_category(item)
        if not cat: continue
        monthly = {m: avg(p) for m, p in sorted(im[item].items()) if avg(p)}
        annual  = {y: avg(p) for y, p in sorted(ia[item].items()) if avg(p)}
        if len(annual) >= 2:
            out['items'][item] = {'category': cat, 'monthly': monthly, 'annual': annual}
    return out, count

# ════════════════════════════════════════════════════════════
#  GitHub 배포
# ════════════════════════════════════════════════════════════
def run_git(args, cwd):
    r = subprocess.run(['git'] + args, cwd=str(cwd), capture_output=True, text=True)
    return r.returncode, (r.stdout + r.stderr).strip()

def deploy(cfg: dict) -> bool:
    GH_USER  = cfg.get('GH_USER', '')
    GH_REPO  = cfg.get('GH_REPO', '')
    GH_TOKEN = cfg.get('GH_TOKEN', '')
    if not all([GH_USER, GH_REPO, GH_TOKEN]):
        err("GitHub 설정이 불완전합니다.")
        return False

    remote_url = f"https://{GH_USER}:{GH_TOKEN}@github.com/{GH_USER}/{GH_REPO}.git"

    if not (SCRIPT_DIR / '.git').exists():
        run_git(['init', '-b', 'main'], SCRIPT_DIR)
        run_git(['remote', 'add', 'origin', remote_url], SCRIPT_DIR)
    else:
        run_git(['remote', 'set-url', 'origin', remote_url], SCRIPT_DIR)

    run_git(['config', 'user.email', f"{GH_USER}@users.noreply.github.com"], SCRIPT_DIR)
    run_git(['config', 'user.name', GH_USER], SCRIPT_DIR)

    src_html = SCRIPT_DIR / '인삼단가분석_대시보드.html'
    if src_html.exists():
        shutil.copy2(src_html, SCRIPT_DIR / 'index.html')

    run_git(['add', 'index.html', '인삼단가_데이터.json'], SCRIPT_DIR)

    code, diff_stat = run_git(['status', '--porcelain'], SCRIPT_DIR)
    if not diff_stat.strip():
        info("변경된 데이터가 없습니다. (이미 최신 상태)")
        return True

    today = datetime.now().strftime('%Y년 %m월 %d일 %H:%M')
    run_git(['commit', '-m', f'📊 데이터 업데이트: {today}'], SCRIPT_DIR)
    run_git(['fetch', 'origin'], SCRIPT_DIR)
    code, msg = run_git(['push', '--force-with-lease', 'origin', 'main'], SCRIPT_DIR)
    if code != 0:
        code, msg = run_git(['push', '--force', 'origin', 'main'], SCRIPT_DIR)
    if code != 0:
        err(f"Git push 실패: {msg[:200]}")
    return code == 0

# ════════════════════════════════════════════════════════════
#  메인
# ════════════════════════════════════════════════════════════
def main():
    banner("인삼 단가 대시보드 전체 자동화", "🌿")
    log(f"  실행 시각 : {datetime.now().strftime('%Y년 %m월 %d일 %H:%M:%S')}")
    log(f"  실행 모드 : {'자동 스케줄' if HEADLESS else '수동 실행'}")

    # 1. 설정 로드
    sep()
    log("  [1/5] 설정 확인")
    cfg = load_config()
    if not cfg.get('GH_TOKEN'):
        err("설정 파일(.인삼_설정.conf)에 토큰이 없습니다.")
        err("'토큰_업데이트.command'를 실행하여 토큰을 등록하세요.")
        pause(); sys.exit(1)
    ok("설정 파일 로드 완료")

    # 2. 토큰 유효성 확인
    log("  🔑 GitHub 토큰 유효성 확인 중...")
    valid, status = check_token(cfg['GH_TOKEN'])
    if not valid:
        handle_expired_token()
        return
    ok(f"토큰 정상 (계정: {status})")

    # 3. 패키지 확인
    sep()
    log("  [2/5] 필요 패키지 확인")
    ensure_packages()

    # 4. 폴더 탐색
    sep()
    log("  [3/5] 폴더 탐색 및 현황 파악")

    # 원본 폴더 (iCloud Numbers)
    numbers_folder = find_numbers_folder()
    if not numbers_folder:
        err("iCloud Numbers 폴더를 찾을 수 없습니다.")
        err("  확인: ~/Library/Mobile Documents/com~apple~Numbers/Documents/")
        pause(); sys.exit(1)
    ok(f"원본 폴더 (Numbers): {numbers_folder}")

    # 출력 폴더 (csv 변환 문서)
    csv_folder = find_csv_folder()
    if not csv_folder:
        err("'csv 변환 문서' 폴더를 찾을 수 없습니다.")
        err("  확인: ~/Desktop/csv 변환 문서/")
        pause(); sys.exit(1)
    ok(f"출력 폴더 (CSV):    {csv_folder}")

    # 원본 연도 폴더 탐색 (Numbers 폴더 기준)
    src_year_folders = find_year_folders(numbers_folder)
    if not src_year_folders:
        err("Numbers 폴더에서 연도별 정산완료 폴더를 찾을 수 없습니다.")
        pause(); sys.exit(1)

    # iCloud 강제 다운로드: Numbers 원본 폴더 및 각 연도 폴더 (재귀 다운로드)
    info(f"iCloud 파일 다운로드 트리거 중 ({len(src_year_folders)}개 연도)...")
    try:
        subprocess.run(['brctl', 'download', str(numbers_folder)],
                       capture_output=True, timeout=15)
    except Exception:
        pass
    for yr, yfolder in sorted(src_year_folders.items()):
        try:
            # -r 플래그로 재귀 다운로드 우선 시도 (폴더 내 모든 파일 일괄 트리거)
            r = subprocess.run(['brctl', 'download', '-r', str(yfolder)],
                               capture_output=True, timeout=30)
            if r.returncode != 0:
                subprocess.run(['brctl', 'download', str(yfolder)],
                               capture_output=True, timeout=15)
        except Exception:
            try:
                subprocess.run(['brctl', 'download', str(yfolder)],
                               capture_output=True, timeout=15)
            except Exception:
                pass
    info("iCloud 다운로드 트리거 완료 — 파일 동기화 초기 대기 중 (10초)...")
    time.sleep(10)

    years = list(src_year_folders.keys())
    ok(f"발견 연도: {min(years)}년 ~ {max(years)}년 ({len(years)}개 폴더)")
    log("")
    log("  📋 연도별 현황 (원본→CSV):")
    total_src, total_csv = summarize_folders(src_year_folders, csv_folder)

    # 5. 파일 변환
    sep()
    log("  [4/5] 원본 파일 변환 (.numbers / .xlsx → CSV)")
    log("  ※ 신형 파일은 순수 Python(numbers_parser), 구형은 Numbers.app 자동 처리")
    log(f"  ※ 원본: {numbers_folder}")
    log(f"  ※ 출력: {csv_folder}")

    need_conv = total_src - total_csv
    if need_conv <= 0:
        info("변환할 신규 파일 없음 (모두 이미 CSV 변환됨)")
    else:
        log(f"  → 변환 필요: 약 {need_conv}개 파일")

    conv, errs = convert_new_files(src_year_folders, csv_folder)
    if conv == 0 and errs == 0 and need_conv <= 0:
        info("모든 파일이 최신 상태입니다")
    elif conv > 0:
        ok(f"총 {conv}개 신규 변환 완료")
    if errs > 0:
        warn(f"총 {errs}개 변환 실패 (위 상세 내용 확인)")

    # 변환 후 현황 재확인
    log("")
    log("  📋 변환 후 현황:")
    summarize_folders(src_year_folders, csv_folder)

    # 6. 데이터 분석
    sep()
    log("  [5/5] 전체 데이터 분석 중... (잠시 기다려 주세요)")
    data, count = build_data(csv_folder)

    if count == 0:
        warn("분석된 데이터가 0건입니다!")
        warn("CSV 파일이 올바른 형식인지 확인하세요.")
        warn("'변환결과리포트.txt'를 생성하여 상태를 기록합니다.")
        # 빈 데이터라도 배포는 계속 진행 (대신 경고)

    output_json = SCRIPT_DIR / '인삼단가_데이터.json'
    with open(output_json, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, separators=(',', ':'))
    size_kb = output_json.stat().st_size / 1024
    ok(f"분석 완료: {count:,}건 / {len(data['items'])}개 품목 / {size_kb:.1f}KB")

    if count == 0:
        warn("0건 데이터로는 배포를 건너뜁니다. CSV 파일 복구 후 다시 실행하세요.")
        pause()
        sys.exit(0)

    # 7. GitHub 배포
    sep()
    log("  📤 GitHub Pages 배포 중...")
    success = deploy(cfg)
    if success:
        ok("업로드 완료!")
    else:
        warn("업로드 실패 — 인터넷 연결 또는 토큰을 확인하세요.")

    # 완료
    pages_url = f"https://{cfg['GH_USER']}.github.io/{cfg['GH_REPO']}/"
    banner("완료! 30초 후 사이트에서 확인하세요", "✅")
    log(f"\n  🌐  {pages_url}\n")
    if not HEADLESS:
        subprocess.Popen(['open', pages_url])
        pause("  창을 닫으려면 Enter 키를 누르세요...")

if __name__ == '__main__':
    main()
PYTHON_EOF
