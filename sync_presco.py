import os
import time
import csv
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
            args=['--no-sandbox', '--disable-setuid-sandbox']  # GitHub Actions用
        )
        context = browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        )
        
        # タイムアウトを60秒に延長
        context.set_default_timeout(60000)
        
        page = context.new_page()
        
        try:
            # ログインページに直接アクセス（logoutページを経由しない）
            print(f"[{datetime.now()}] ログインページにアクセスします")
            page.goto('https://presco.ai/partner/', timeout=60000)
            time.sleep(3)
            
            # ログインフォームが表示されるまで待機
            page.wait_for_selector('input[name="username"]', timeout=10000)
            print(f"[{datetime.now()}] ログインフォームを確認しました")
            
            # ログインフォームに入力
            print(f"[{datetime.now()}] ログイン情報を入力します")
            page.fill('input[name="username"]', email)
            page.fill('input[name="password"]', password)
            
            # ログインボタンをクリック
            print(f"[{datetime.now()}] ログインボタンをクリックします")
            
            # ナビゲーションを待機しながらクリック
            with page.expect_navigation(timeout=60000):
                page.click('input[type="submit"][value="ログイン"]')
            
            time.sleep(3)
            
            # ログイン成功を確認
            current_url = page.url
            print(f"[{datetime.now()}] 現在のURL: {current_url}")
            
            # ログイン後のページかどうか確認
            if 'home' not in current_url and 'actionLog' not in current_url:
                page.screenshot(path='/tmp/login_error.png')
                raise Exception(f"ログインに失敗しました。URL: {current_url}")
            
            print(f"[{datetime.now()}] ログインに成功しました")
            
            # 成果一覧ページに移動
            print(f"[{datetime.now()}] 成果一覧ページに移動します")
            page.goto('https://presco.ai/partner/actionLog/list', timeout=60000)
            time.sleep(5)
            
            # ページが完全に読み込まれるまで待機
            page.wait_for_selector('#csv-link', state='visible', timeout=30000)
            print(f"[{datetime.now()}] CSVダウンロードボタンを確認しました")
            
            # CSVダウンロードボタンをクリックしてダウンロード
            print(f"[{datetime.now()}] CSVダウンロードを開始します")
            
            # ダウンロード開始を待機
            with page.expect_download(timeout=60000) as download_info:
                page.click('#csv-link')
                print(f"[{datetime.now()}] CSVダウンロードボタンをクリックしました")
            
            # ダウンロード完了を待機
            download = download_info.value
            csv_path = f'/tmp/presco_data_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
            download.save_as(csv_path)
            
            print(f"[{datetime.now()}] CSVをダウンロードしました: {csv_path}")
            
            # ダウンロードしたファイルのサイズを確認
            import os as os_module
            file_size = os_module.path.getsize(csv_path)
            print(f"[{datetime.now()}] ファイルサイズ: {file_size} bytes")
            
            if file_size == 0:
                raise Exception("ダウンロードしたCSVファイルが空です")
            
            return csv_path
            
        except Exception as e:
            print(f"[{datetime.now()}] エラーが発生しました: {str(e)}")
            # エラー時のスクリーンショットを保存
            try:
                page.screenshot(path='/tmp/error_screenshot.png')
                print(f"[{datetime.now()}] エラー時のスクリーンショットを保存しました")
                # 現在のHTMLも保存
                html_content = page.content()
                with open('/tmp/error_page.html', 'w', encoding='utf-8') as f:
                    f.write(html_content)
                print(f"[{datetime.now()}] エラー時のHTMLを保存しました")
            except:
                pass
            raise
        
        finally:
            browser.close()
            print(f"[{datetime.now()}] ブラウザを閉じました")


def filter_and_deduplicate_csv(csv_path, existing_ids):
    """
    CSVをフィルタリングして重複を除去
    
    Args:
        csv_path: CSVファイルのパス
        existing_ids: 既存のIDセット
    
    Returns:
        フィルタリング＆重複除去されたデータ（ヘッダー含む）
    """
    print(f"[{datetime.now()}] CSVのフィルタリングと重複チェックを開始します")
    
    # CSVを読み込み（複数のエンコーディングを試行）
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
        raise Exception("CSVファイルの読み込みに失敗しました（エンコーディングエラー）")
    
    if len(data) == 0:
        print(f"[{datetime.now()}] 警告: CSVファイルにデータがありません")
        return []
    
    print(f"[{datetime.now()}] CSVデータを読み込みました（{len(data)}行）")
    
    # ヘッダー行を取得
    header = data[0] if len(data) > 0 else []
    
    # データ行のみを処理
    data_rows = data[1:] if len(data) > 1 else []
    
    # フィルタリングと重複チェック
    filtered_data = []
    target_site_name = "Fast Baito 看護特化"
    
    total_count = len(data_rows)
    filtered_count = 0
    duplicate_count = 0
    new_count = 0
    
    for row in data_rows:
        if len(row) < 6:  # 列数が足りない場合はスキップ
            continue
        
        # A列（インデックス0）: ID
        record_id = row[0]
        
        # F列（インデックス5）: サイト名
        site_name = row[5]
        
        # サイト名でフィルタリング
        if site_name != target_site_name:
            filtered_count += 1
            continue
        
        # 重複チェック
        if record_id in existing_ids:
            duplicate_count += 1
            continue
        
        # 新規データとして追加
        filtered_data.append(row)
        existing_ids.add(record_id)  # 今回追加するデータもセットに追加
        new_count += 1
    
    print(f"[{datetime.now()}] フィルタリング結果:")
    print(f"  - 総データ数: {total_count}行")
    print(f"  - サイト名不一致で除外: {filtered_count}行")
    print(f"  - 重複で除外: {duplicate_count}行")
    print(f"  - 新規追加: {new_count}行")
    
    # ヘッダーと新規データを結合して返す
    if new_count > 0:
        return [header] + filtered_data
    else:
        return []


def upload_to_spreadsheet(csv_path):
    """CSVをGoogle Spreadsheetsに追記"""
    
    print(f"[{datetime.now()}] Google Sheetsへのアップロードを開始します")
    
    # Google Sheets認証情報を取得
    creds_json = os.environ.get('GOOGLE_CREDENTIALS')
    if not creds_json:
        raise Exception("環境変数 GOOGLE_CREDENTIALS が設定されていません")
    
    # JSON文字列を辞書に変換
    creds_dict = json.loads(creds_json)
    
    # 認証
    scope = [
        'https://spreadsheets.google.com/feeds',
        'https://www.googleapis.com/auth/drive'
    ]
    credentials = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
    gc = gspread.authorize(credentials)
    
    # スプレッドシートを開く
    spreadsheet_id = os.environ.get('SPREADSHEET_ID')
    if not spreadsheet_id:
        raise Exception("環境変数 SPREADSHEET_ID が設定されていません")
    
    spreadsheet = gc.open_by_key(spreadsheet_id)
    
    # ワークシートを取得または作成
    sheet_name = '成果情報_看護特化'
    try:
        worksheet = spreadsheet.worksheet(sheet_name)
        print(f"[{datetime.now()}] 既存のワークシート '{sheet_name}' を使用します")
    except:
        worksheet = spreadsheet.add_worksheet(title=sheet_name, rows=1000, cols=50)
        print(f"[{datetime.now()}] 新しいワークシート '{sheet_name}' を作成しました")
    
    # 既存データを取得
    existing_data = worksheet.get_all_values()
    
    if len(existing_data) == 0:
        # シートが空の場合
        print(f"[{datetime.now()}] シートは空です。新規データとして追加します")
        existing_ids = set()
        has_header = False
    else:
        # 既存のIDを取得（A列、ヘッダー行を除く）
        existing_ids = set()
        for row in existing_data[1:]:  # ヘッダーをスキップ
            if len(row) > 0 and row[0]:  # A列が存在して空でない
                existing_ids.add(row[0])
        
        print(f"[{datetime.now()}] 既存データ: {len(existing_data)}行（ヘッダー含む）")
        print(f"[{datetime.now()}] 既存ID数: {len(existing_ids)}件")
        has_header = True
    
    # CSVをフィルタリングして重複除去
    new_data = filter_and_deduplicate_csv(csv_path, existing_ids)
    
    if len(new_data) == 0:
        print(f"[{datetime.now()}] 追加する新規データはありません")
        return
    
    # ヘッダーを除いたデータ行のみ
    if has_header:
        # 既存シートにヘッダーがある場合は、新規データのヘッダーを除外
        rows_to_add = new_data[1:]
    else:
        # 既存シートが空の場合は、ヘッダーも含めて追加
        rows_to_add = new_data
    
    if len(rows_to_add) == 0:
        print(f"[{datetime.now()}] 追加するデータ行はありません")
        return
    
    # 最終行の次の行番号を取得
    last_row = len(existing_data)
    next_row = last_row + 1
    
    print(f"[{datetime.now()}] {len(rows_to_add)}行を追加します（開始行: {next_row}）")
    
    # データを追加（append を使用）
    worksheet.append_rows(rows_to_add, value_input_option='RAW')
    
    print(f"[{datetime.now()}] Google Sheetsへの追記が完了しました")
    print(f"[{datetime.now()}] スプレッドシートURL: https://docs.google.com/spreadsheets/d/{spreadsheet_id}")


def main():
    """メイン処理"""
    try:
        print("=" * 60)
        print(f"[{datetime.now()}] Presco自動同期を開始します")
        print("=" * 60)
        
        # CSVをダウンロード
        csv_path = login_and_download_csv()
        
        # Google Sheetsに追記（フィルタリング＆重複チェック付き）
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
