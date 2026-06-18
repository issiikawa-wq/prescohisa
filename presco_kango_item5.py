# presco_sync.py
# 看護（item5）・介護（item5）を1回の実行でまとめて同期する統合版

import os
import time
import csv
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from urllib.parse import quote
from playwright.sync_api import sync_playwright
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json


# ============================================================
#  設定
# ============================================================

SPREADSHEET_ID = '1x7xkMomtb81GXqd5XF0b3_q59BuOSoHypTyyLqFWKow'
DAYS_BACK      = 180  # 何日前からのデータを取得するか

# 同期対象のジョブ一覧。ここに追記すれば対象を増やせる
JOBS = [
    {
        'name':            '看護',
        'slug':            'kango_item5',
        'sheet_name':      'Presco_kango_item5',
        'partner_site_id': '37502',
        'item_type':       '5',
    },
    {
        'name':            '介護',
        'slug':            'kaigo_item5',
        'sheet_name':      'Presco_kaigo_item5',
        'partner_site_id': '37215',
        'item_type':       '5',
    },
]


# ============================================================
#  Googleスプレッドシート接続（1回だけ認証して使い回す）
# ============================================================

def get_spreadsheet():
    creds_json = os.environ.get('GOOGLE_CREDENTIALS')
    if not creds_json:
        raise Exception("環境変数 GOOGLE_CREDENTIALS が設定されていません")

    creds_dict  = json.loads(creds_json)
    scope       = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'
    ]
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    gc          = gspread.authorize(credentials)
    return gc.open_by_key(SPREADSHEET_ID)


# ============================================================
#  CSVダウンロード（ジョブ単位）
#  ※ページ操作のみ。ブラウザ起動・ログインは呼び出し側で実施
# ============================================================

def download_csv(page, job):
    JST       = ZoneInfo("Asia/Tokyo")
    today     = datetime.now(JST)
    date_from = (today - timedelta(days=DAYS_BACK)).strftime("%Y/%m/%d")
    date_to   = today.strftime("%Y/%m/%d")

    report_url = (
        "https://presco.ai/partner/report/search"
        f"?searchDateTimeFrom={quote(date_from, safe='')}"
        f"&searchDateTimeTo={quote(date_to, safe='')}"
        f"&searchItemType={job['item_type']}"
        f"&searchPeriodType=4"
        f"&searchProgramId="
        f"&searchDateType=3"
        f"&searchPartnerSiteId={job['partner_site_id']}"
        f"&searchProgramUrlId="
        f"&searchPartnerSitePageId="
        f"&searchLargeGenreId="
        f"&searchMediumGenreId="
        f"&searchSmallGenreId="
        f"&_searchJoinType=on"
    )

    print(f"[{datetime.now()}] [{job['name']}] レポートページにアクセスします")
    print(f"[{datetime.now()}] [{job['name']}] 期間: {date_from} 〜 {date_to} / "
          f"itemType={job['item_type']} / siteId={job['partner_site_id']}")
    page.goto(report_url, timeout=60000)
    time.sleep(5)

    # ── CSVダウンロード ──
    csv_selectors = [
        '#report-link',
        'a:has-text("ログ集計CSVダウンロード")',
        '#csv-link',
    ]

    csv_clicked = False
    download_info = None
    for selector in csv_selectors:
        try:
            page.wait_for_selector(selector, state='visible', timeout=10000)
            print(f"[{datetime.now()}] [{job['name']}] CSVボタンを確認しました: {selector}")

            with page.expect_download(timeout=60000) as dl:
                page.click(selector)
            download_info = dl
            csv_clicked = True
            break
        except Exception:
            continue

    if not csv_clicked:
        page.screenshot(path=f"/tmp/error_{job['slug']}_csv.png")
        raise Exception("CSVダウンロードボタンが見つかりませんでした")

    download = download_info.value
    csv_path = f"/tmp/presco_{job['slug']}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    download.save_as(csv_path)

    file_size = os.path.getsize(csv_path)
    print(f"[{datetime.now()}] [{job['name']}] CSVダウンロード完了: {csv_path} ({file_size} bytes)")

    if file_size == 0:
        raise Exception("ダウンロードしたCSVファイルが空です")

    return csv_path


# ============================================================
#  CSVデータ整形（F列・G列・K列以降を抽出）
# ============================================================

def extract_columns(data):
    """
    F列（インデックス5）、G列（インデックス6）、
    K列以降（インデックス10〜）のみ返す。
    他の列（A〜E、H〜J）は除外する。
    """
    result = []
    for row in data:
        new_row = []
        # F列（インデックス5）
        if len(row) > 5:
            new_row.append(row[5])
        else:
            new_row.append('')
        # G列（インデックス6）
        if len(row) > 6:
            new_row.append(row[6])
        else:
            new_row.append('')
        # K列以降（インデックス10〜）
        if len(row) > 10:
            new_row.extend(row[10:])
        result.append(new_row)
    return result


# ============================================================
#  スプレッドシートへ上書き（ジョブ単位）
# ============================================================

def upload_to_spreadsheet(spreadsheet, csv_path, job):
    print(f"[{datetime.now()}] [{job['name']}] スプレッドシートへのアップロードを開始します")

    sheet_name = job['sheet_name']
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
        print(f"[{datetime.now()}] [{job['name']}] 既存シート '{sheet_name}' を使用します")
    except:
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=5000, cols=30)
        print(f"[{datetime.now()}] [{job['name']}] 新しいシート '{sheet_name}' を作成しました")

    # CSVを読み込む（文字コード自動判定）
    encodings = ['utf-8-sig', 'utf-8', 'shift_jis', 'cp932']
    data = None
    for encoding in encodings:
        try:
            with open(csv_path, 'r', encoding=encoding) as f:
                data = list(csv.reader(f))
            print(f"[{datetime.now()}] [{job['name']}] CSVを {encoding} で読み込みました（{len(data)}行）")
            break
        except UnicodeDecodeError:
            continue

    if data is None:
        raise Exception("CSVファイルの読み込みに失敗しました")

    # F列・G列・K列以降を抽出
    filtered_data = extract_columns(data)
    print(f"[{datetime.now()}] [{job['name']}] F列・G列・K列以降を抽出しました（{len(filtered_data)}行）")

    if filtered_data:
        print(f"[{datetime.now()}] [{job['name']}] ヘッダー確認: {filtered_data[0]}")

    # シートをクリアして書き込み
    print(f"[{datetime.now()}] [{job['name']}] シートをクリアして書き込みます")
    worksheet.clear()

    if filtered_data:
        worksheet.update(values=filtered_data, range_name="A1")
        print(f"[{datetime.now()}] [{job['name']}] 書き込み完了: {len(filtered_data)}行")


# ============================================================
#  ログイン（1回だけ実施）
# ============================================================

def login(page):
    email    = os.environ.get('PRESCO_EMAIL')
    password = os.environ.get('PRESCO_PASSWORD')
    if not email or not password:
        raise Exception("環境変数 PRESCO_EMAIL, PRESCO_PASSWORD が設定されていません")

    print(f"[{datetime.now()}] ログインページにアクセスします")
    page.goto('https://presco.ai/partner/', timeout=60000)
    time.sleep(3)

    page.wait_for_selector('input[name="username"]', timeout=10000)
    page.fill('input[name="username"]', email)
    page.fill('input[name="password"]', password)

    with page.expect_navigation(timeout=60000):
        page.click('input[type="submit"][value="ログイン"]')
    time.sleep(3)

    current_url = page.url
    print(f"[{datetime.now()}] 現在のURL: {current_url}")
    if not any(x in current_url for x in ['home', 'actionLog', 'report']):
        page.screenshot(path='/tmp/login_error.png')
        raise Exception(f"ログインに失敗しました。URL: {current_url}")

    print(f"[{datetime.now()}] ログインに成功しました")


# ============================================================
#  メイン
# ============================================================

def main():
    print("=" * 60)
    print(f"[{datetime.now()}] Presco レポート同期を開始します（対象: {len(JOBS)}件）")
    print("=" * 60)

    # スプレッドシート接続は最初に1回だけ
    spreadsheet = get_spreadsheet()

    results = []  # 各ジョブの成否を記録

    with sync_playwright() as p:
        print(f"[{datetime.now()}] ブラウザを起動します")
        browser = p.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-setuid-sandbox']
        )
        context = browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        )
        context.set_default_timeout(60000)
        page = context.new_page()

        try:
            # ── ログインは1回だけ ──
            login(page)

            # ── 各ジョブを順番に処理 ──
            for job in JOBS:
                print("-" * 60)
                print(f"[{datetime.now()}] [{job['name']}] 処理開始")
                try:
                    csv_path = download_csv(page, job)
                    upload_to_spreadsheet(spreadsheet, csv_path, job)
                    results.append((job['name'], '成功'))
                    print(f"[{datetime.now()}] [{job['name']}] 処理完了")
                except Exception as e:
                    # 1件失敗しても他のジョブは続行する
                    print(f"[{datetime.now()}] [{job['name']}] エラー: {str(e)}")
                    results.append((job['name'], f'失敗: {str(e)}'))

        finally:
            browser.close()
            print(f"[{datetime.now()}] ブラウザを閉じました")

    # ── 結果サマリー ──
    print("=" * 60)
    print(f"[{datetime.now()}] 処理結果サマリー")
    for name, status in results:
        print(f"  - {name}: {status}")
    print(f"[{datetime.now()}] スプレッドシートURL: "
          f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}")
    print("=" * 60)

    # 1件でも失敗があれば、GitHub Actions を異常終了させる
    failed = [name for name, status in results if status != '成功']
    if failed:
        raise Exception(f"失敗したジョブ: {', '.join(failed)}")


if __name__ == "__main__":
    main()
