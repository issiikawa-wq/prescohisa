# presco_kango_item5.py

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

SPREADSHEET_ID  = '1x7xkMomtb81GXqd5XF0b3_q59BuOSoHypTyyLqFWKow'
SHEET_NAME      = 'Presco_kango_item5'
PARTNER_SITE_ID = '37502'
DAYS_BACK       = 180  # 何日前からのデータを取得するか


# ============================================================
#  CSVダウンロード
# ============================================================

def login_and_download_csv():
    print(f"[{datetime.now()}] 処理を開始します")

    email    = os.environ.get('PRESCO_EMAIL')
    password = os.environ.get('PRESCO_PASSWORD')
    if not email or not password:
        raise Exception("環境変数 PRESCO_EMAIL, PRESCO_PASSWORD が設定されていません")

    JST       = ZoneInfo("Asia/Tokyo")
    today     = datetime.now(JST)
    date_from = (today - timedelta(days=DAYS_BACK)).strftime("%Y/%m/%d")
    date_to   = today.strftime("%Y/%m/%d")

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
            # ── ログイン ──
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
                page.screenshot(path='/tmp/login_error_kango_item5.png')
                raise Exception(f"ログインに失敗しました。URL: {current_url}")

            print(f"[{datetime.now()}] ログインに成功しました")

            # ── レポートページに直接アクセス ──
            report_url = (
                "https://presco.ai/partner/report/search"
                f"?searchDateTimeFrom={quote(date_from, safe='')}"
                f"&searchDateTimeTo={quote(date_to, safe='')}"
                f"&searchItemType=5"
                f"&searchPeriodType=4"
                f"&searchProgramId="
                f"&searchDateType=3"
                f"&searchPartnerSiteId={PARTNER_SITE_ID}"
                f"&searchProgramUrlId="
                f"&searchPartnerSitePageId="
                f"&searchLargeGenreId="
                f"&searchMediumGenreId="
                f"&searchSmallGenreId="
                f"&_searchJoinType=on"
            )

            print(f"[{datetime.now()}] レポートページにアクセスします")
            print(f"[{datetime.now()}] 期間: {date_from} 〜 {date_to}")
            print(f"[{datetime.now()}] searchItemType=5")
            page.goto(report_url, timeout=60000)
            time.sleep(5)

            # ── CSVダウンロード ──
            csv_selectors = [
                '#report-link',
                'a:has-text("ログ集計CSVダウンロード")',
                '#csv-link',
            ]

            csv_clicked = False
            for selector in csv_selectors:
                try:
                    page.wait_for_selector(selector, state='visible', timeout=10000)
                    print(f"[{datetime.now()}] CSVボタンを確認しました: {selector}")

                    with page.expect_download(timeout=60000) as download_info:
                        page.click(selector)

                    csv_clicked = True
                    break
                except Exception:
                    continue

            if not csv_clicked:
                page.screenshot(path='/tmp/error_kango_item5_csv.png')
                raise Exception("CSVダウンロードボタンが見つかりませんでした")

            download = download_info.value
            csv_path = f'/tmp/presco_kango_item5_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
            download.save_as(csv_path)

            file_size = os.path.getsize(csv_path)
            print(f"[{datetime.now()}] CSVダウンロード完了: {csv_path} ({file_size} bytes)")

            if file_size == 0:
                raise Exception("ダウンロードしたCSVファイルが空です")

            return csv_path

        except Exception as e:
            print(f"[{datetime.now()}] エラー: {str(e)}")
            try:
                page.screenshot(path='/tmp/error_kango_item5.png')
                print(f"[{datetime.now()}] スクリーンショットを保存しました")
            except:
                pass
            raise

        finally:
            browser.close()
            print(f"[{datetime.now()}] ブラウザを閉じました")


# ============================================================
#  スプレッドシートへ上書き（全列そのまま書き込み）
# ============================================================

def upload_to_spreadsheet(csv_path):
    print(f"[{datetime.now()}] スプレッドシートへのアップロードを開始します")

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

    spreadsheet = gc.open_by_key(SPREADSHEET_ID)

    try:
        worksheet = spreadsheet.worksheet(SHEET_NAME)
        print(f"[{datetime.now()}] 既存シート '{SHEET_NAME}' を使用します")
    except:
        worksheet = spreadsheet.add_worksheet(title=SHEET_NAME, rows=5000, cols=50)
        print(f"[{datetime.now()}] 新しいシート '{SHEET_NAME}' を作成しました")

    # CSVを読み込む（文字コード自動判定）
    encodings = ['utf-8-sig', 'utf-8', 'shift_jis', 'cp932']
    data = None
    for encoding in encodings:
        try:
            with open(csv_path, 'r', encoding=encoding) as f:
                data = list(csv.reader(f))
            print(f"[{datetime.now()}] CSVを {encoding} で読み込みました（{len(data)}行）")
            break
        except UnicodeDecodeError:
            continue

    if data is None:
        raise Exception("CSVファイルの読み込みに失敗しました")

    # 先頭行をログで確認
    if data:
        print(f"[{datetime.now()}] ヘッダー確認: {data[0]}")
        print(f"[{datetime.now()}] 列数: {len(data[0])}")

    # シートをクリアして書き込み（全列そのまま）
    print(f"[{datetime.now()}] シートをクリアして書き込みます")
    worksheet.clear()

    if data:
        worksheet.update(values=data, range_name="A1")
        print(f"[{datetime.now()}] 書き込み完了: {len(data)}行")

    print(f"[{datetime.now()}] スプレッドシートURL: https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}")


# ============================================================
#  メイン
# ============================================================

def main():
    try:
        print("=" * 60)
        print(f"[{datetime.now()}] Presco看護レポート（itemType=5）同期を開始します")
        print("=" * 60)

        csv_path = login_and_download_csv()
        upload_to_spreadsheet(csv_path)

        print("=" * 60)
        print(f"[{datetime.now()}] すべての処理が正常に完了しました")
        print("=" * 60)

    except Exception as e:
        print("=" * 60)
        print(f"[{datetime.now()}] エラーが発生しました: {str(e)}")
        print("=" * 60)
        raise


if __name__ == "__main__":
    main()
