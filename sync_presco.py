import os
import time
import csv
import re
from datetime import datetime
from playwright.sync_api import sync_playwright
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import json

def login_and_download_csv():
    """Presco.aiにログインしてCSVをダウンロード"""
    
    print(f"[{datetime.now()}] 処理を開始します")
    
    # 環境変数から認証情報を取得
    email = os.environ.get('PRESCO_EMAIL')
    password = os.environ.get('PRESCO_PASSWORD')
    
    if not email or not password:
        raise Exception("環境変数 PRESCO_EMAIL, PRESCO_PASSWORD が設定されていません")
    
    print(f"[{datetime.now()}] 認証情報を確認しました")
    
    with sync_playwright() as p:
        # ブラウザを起動（ヘッドレスモード）
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
            print(f"[{datetime.now()}] ログインページにアクセスします")
            page.goto('https://presco.ai/partner/', timeout=60000)
            time.sleep(3)
            
            page.wait_for_selector('input[name="username"]', timeout=10000)
            print(f"[{datetime.now()}] ログインフォームを確認しました")
            
            print(f"[{datetime.now()}] ログイン情報を入力します")
            page.fill('input[name="username"]', email)
            page.fill('input[name="password"]', password)
            
            print(f"[{datetime.now()}] ログインボタンをクリックします")
            with page.expect_navigation(timeout=60000):
                page.click('input[type="submit"][value="ログイン"]')
            
            time.sleep(3)
            
            current_url = page.url
            print(f"[{datetime.now()}] 現在のURL: {current_url}")
            
            if 'home' not in current_url and 'actionLog' not in current_url:
                page.screenshot(path='/tmp/login_error.png')
                raise Exception(f"ログインに失敗しました。URL: {current_url}")
            
            print(f"[{datetime.now()}] ログインに成功しました")
            
            print(f"[{datetime.now()}] 成果一覧ページに移動します")
            page.goto('https://presco.ai/partner/actionLog/list', timeout=60000)
            time.sleep(5)
            
            page.wait_for_selector('#csv-link', state='visible', timeout=30000)
            print(f"[{datetime.now()}] CSVダウンロードボタンを確認しました")
            
            print(f"[{datetime.now()}] CSVダウンロードを開始します")
            
            with page.expect_download(timeout=60000) as download_info:
                page.click('#csv-link')
                print(f"[{datetime.now()}] CSVダウンロードボタンをクリックしました")
            
            download = download_info.value
            csv_path = f'/tmp/presco_data_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
            download.save_as(csv_path)
            
            print(f"[{datetime.now()}] CSVをダウンロードしました: {csv_path}")
            
            import os as os_module
            file_size = os_module.path.getsize(csv_path)
            print(f"[{datetime.now()}] ファイルサイズ: {file_size} bytes")
            
            if file_size == 0:
                raise Exception("ダウンロードしたCSVファイルが空です")
            
            return csv_path
            
        except Exception as e:
            print(f"[{datetime.now()}] エラーが発生しました: {str(e)}")
            try:
                page.screenshot(path='/tmp/error_screenshot.png')
                print(f"[{datetime.now()}] エラー時のスクリーンショットを保存しました")
            except:
                pass
            raise
        
        finally:
            browser.close()
            print(f"[{datetime.now()}] ブラウザを閉じました")


def extract_gclid(referrer_url):
    """
    リファラURLからgclidを抽出
    
    Args:
        referrer_url: リファラURL
    
    Returns:
        gclid の値、見つからない場合は空文字列
    """
    if not referrer_url:
        return ""
    
    # gclid=XXX のパターンを検索
    match = re.search(r'gclid=([^&]+)', referrer_url)
    if match:
        return match.group(1)
    
    return ""


def datetime_to_excel_serial(date_string):
    """
    日付文字列をExcelのシリアル値に変換
    
    Args:
        date_string: "2026/02/19 15:30:00" 形式の日付文字列
    
    Returns:
        Excelのシリアル値（例: 46065.64583）
    """
    try:
        # 日付文字列をdatetimeオブジェクトに変換
        dt = datetime.strptime(date_string, '%Y/%m/%d %H:%M:%S')
        
        # Excelの基準日（1900年1月1日）との差分を計算
        # Excelは1900年1月1日を1とする（ただし1900年をうるう年として誤って扱う）
        excel_epoch = datetime(1899, 12, 30)  # Excelの実質的な基準日
        delta = dt - excel_epoch
        
        # 日数 + 時刻の小数部分
        serial = delta.days + (delta.seconds / 86400.0)
        
        return serial
        
    except Exception as e:
        print(f"[{datetime.now()}] 日付変換エラー: {date_string} - {str(e)}")
        return ""


def transform_csv_data(csv_path, existing_gclids):
    """
    CSVデータを変換して出力フォーマットに整形
    
    Args:
        csv_path: CSVファイルのパス
        existing_gclids: 既存のGCLIDセット
    
    Returns:
        変換されたデータ（ヘッダー含む）
    """
    print(f"[{datetime.now()}] CSVデータの変換を開始します")
    
    # CSVを読み込み
    encodings = ['utf-8-sig', 'utf-8', 'shift_jis', 'cp932']
    data = None
    
    for encoding in encodings:
        try:
            with open(csv_path, 'r', encoding=encoding) as f:
                csv_reader = csv.reader(f)
                data = list(csv_reader)
            print(f"[{datetime.now()}] CSVを {encoding} エンコーディングで読み込みました")
            break
        except UnicodeDecodeError:
            continue
    
    if data is None:
        raise Exception("CSVファイルの読み込みに失敗しました")
    
    if len(data) == 0:
        print(f"[{datetime.now()}] 警告: CSVファイルにデータがありません")
        return []
    
    print(f"[{datetime.now()}] CSVデータを読み込みました（{len(data)}行）")
    
    # ヘッダーは無視（元のCSVのヘッダーは使わない）
    data_rows = data[1:] if len(data) > 1 else []
    
    # 出力用のヘッダー
    output_header = [
        "Google Click ID",
        "Conversion Name",
        "Conversion Time",
        "Conversion Value",
        "Conversion Currency"
    ]
    
    transformed_data = []
    target_site_name = "Fast Baito 看護特化"
    
    total_count = len(data_rows)
    filtered_count = 0
    duplicate_count = 0
    no_gclid_count = 0
    new_count = 0
    
    for row in data_rows:
        # 列数チェック
        if len(row) < 13:  # M列（インデックス12）まで必要
            continue
        
        # F列（インデックス5）: サイト名
        site_name = row[5] if len(row) > 5 else ""
        
        # サイト名でフィルタリング
        if site_name != target_site_name:
            filtered_count += 1
            continue
        
        # M列（インデックス12）: リファラ
        referrer = row[12] if len(row) > 12 else ""
        
        # A列: Google Click ID（gclidを抽出）
        gclid = extract_gclid(referrer)
        
        # gclidがない場合はスキップ
        if not gclid:
            no_gclid_count += 1
            continue
        
        # 重複チェック
        if gclid in existing_gclids:
            duplicate_count += 1
            continue
        
        # D列（インデックス3）: 成果発生日時
        action_datetime = row[3] if len(row) > 3 else ""
        
        # B列: Conversion Name（固定値）
        conversion_name = "看護基本"
        
        # C列: Conversion Time（Excelシリアル値）
        conversion_time = datetime_to_excel_serial(action_datetime)
        
        # D列: Conversion Value（固定値）
        conversion_value = "6000"
        
        # E列: Conversion Currency（固定値）
        conversion_currency = "JPY"
        
        # 出力行を作成
        output_row = [
            gclid,              # A列: Google Click ID
            conversion_name,    # B列: Conversion Name
            conversion_time,    # C列: Conversion Time
            conversion_value,   # D列: Conversion Value
            conversion_currency # E列: Conversion Currency
        ]
        
        transformed_data.append(output_row)
        existing_gclids.add(gclid)
        new_count += 1
    
    print(f"[{datetime.now()}] 変換結果:")
    print(f"  - 総データ数: {total_count}行")
    print(f"  - サイト名不一致で除外: {filtered_count}行")
    print(f"  - GCLID未検出で除外: {no_gclid_count}行")
    print(f"  - 重複で除外: {duplicate_count}行")
    print(f"  - 新規追加: {new_count}行")
    
    if new_count > 0:
        return [output_header] + transformed_data
    else:
        return []


def upload_to_spreadsheet(csv_path):
    """CSVをGoogle Spreadsheetsに追記"""
    
    print(f"[{datetime.now()}] Google Sheetsへのアップロードを開始します")
    
    # Google Sheets認証情報を取得
    creds_json = os.environ.get('GOOGLE_CREDENTIALS')
    if not creds_json:
        raise Exception("環境変数 GOOGLE_CREDENTIALS が設定されていません")
    
    creds_dict = json.loads(creds_json)
    
    scope = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'
    ]
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    gc = gspread.authorize(credentials)
    
    spreadsheet_id = os.environ.get('SPREADSHEET_ID')
    if not spreadsheet_id:
        raise Exception("環境変数 SPREADSHEET_ID が設定されていません")
    
    spreadsheet = gc.open_by_key(spreadsheet_id)
    
    sheet_name = '成果情報_看護特化'
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
        print(f"[{datetime.now()}] 既存のワークシート '{sheet_name}' を使用します")
    except:
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=10)
        print(f"[{datetime.now()}] 新しいワークシート '{sheet_name}' を作成しました")
    
    # 既存データを取得
    existing_data = worksheet.get_all_values()
    
    if len(existing_data) == 0:
        print(f"[{datetime.now()}] シートは空です。新規データとして追加します")
        existing_gclids = set()
        has_header = False
    else:
        # 既存のGCLIDを取得（A列、ヘッダー行を除く）
        existing_gclids = set()
        for row in existing_data[1:]:
            if len(row) > 0 and row[0]:
                existing_gclids.add(row[0])
        
        print(f"[{datetime.now()}] 既存データ: {len(existing_data)}行（ヘッダー含む）")
        print(f"[{datetime.now()}] 既存GCLID数: {len(existing_gclids)}件")
        has_header = True
    
    # CSVを変換して重複除去
    new_data = transform_csv_data(csv_path, existing_gclids)
    
    if len(new_data) == 0:
        print(f"[{datetime.now()}] 追加する新規データはありません")
        return
    
    # ヘッダーを除いたデータ行のみ
    if has_header:
        rows_to_add = new_data[1:]
    else:
        rows_to_add = new_data
    
    if len(rows_to_add) == 0:
        print(f"[{datetime.now()}] 追加するデータ行はありません")
        return
    
    last_row = len(existing_data)
    next_row = last_row + 1
    
    print(f"[{datetime.now()}] {len(rows_to_add)}行を追加します（開始行: {next_row}）")
    
    worksheet.append_rows(rows_to_add, value_input_option='USER_ENTERED')
    
    print(f"[{datetime.now()}] Google Sheetsへの追記が完了しました")
    print(f"[{datetime.now()}] スプレッドシートURL: https://docs.google.com/spreadsheets/d/{spreadsheet_id}")


def main():
    """メイン処理"""
    try:
        print("=" * 60)
        print(f"[{datetime.now()}] Presco自動同期を開始します")
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
