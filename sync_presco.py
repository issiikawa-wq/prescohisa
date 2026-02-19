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
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        )
        page = context.new_page()
        
        try:
            # ログインページにアクセス
            print(f"[{datetime.now()}] ログインページにアクセスします")
            page.goto('https://presco.ai/partner/auth/logout', wait_until='networkidle')
            time.sleep(2)
            
            # ログインフォームに入力
            print(f"[{datetime.now()}] ログイン情報を入力します")
            page.fill('input[name="username"]', email)
            page.fill('input[name="password"]', password)
            
            # ログインボタンをクリック
            print(f"[{datetime.now()}] ログインボタンをクリックします")
            page.click('input[type="submit"][value="ログイン"]')
            
            # ログイン完了を待機
            page.wait_for_load_state('networkidle')
            time.sleep(3)
            
            # ログイン成功を確認
            current_url = page.url
            print(f"[{datetime.now()}] 現在のURL: {current_url}")
            
            if 'login' in current_url or 'logout' in current_url:
                # スクリーンショットを保存してデバッグ
                page.screenshot(path='/tmp/login_error.png')
                raise Exception("ログインに失敗しました。認証情報を確認してください。")
            
            print(f"[{datetime.now()}] ログインに成功しました")
            
            # CSV一覧ページに移動
            print(f"[{datetime.now()}] CSV一覧ページに移動します")
            page.goto('https://presco.ai/partner/actionLog/list', wait_until='networkidle')
            time.sleep(2)
            
            # CSVダウンロードボタンを探してクリック
            print(f"[{datetime.now()}] CSVダウンロードボタンを探します")
            
            # ダウンロード開始を待機
            with page.expect_download() as download_info:
                # 複数のセレクタパターンを試行
                button_selectors = [
                    'button:has-text("CSV")',
                    'a:has-text("CSV")',
                    'input[value*="CSV"]',
                    '.m-button:has-text("CSV")',
                    '[class*="csv"]',
                    '[class*="download"]'
                ]
                
                clicked = False
                for selector in button_selectors:
                    try:
                        if page.locator(selector).count() > 0:
                            print(f"[{datetime.now()}] セレクタ {selector} でボタンを発見")
                            page.click(selector)
                            clicked = True
                            break
                    except:
                        continue
                
                if not clicked:
                    page.screenshot(path='/tmp/page_screenshot.png')
                    raise Exception("CSVダウンロードボタンが見つかりませんでした")
            
            # ダウンロード完了を待機
            download = download_info.value
            csv_path = f'/tmp/presco_data_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
            download.save_as(csv_path)
            
            print(f"[{datetime.now()}] CSVをダウンロードしました: {csv_path}")
            
            return csv_path
            
        except Exception as e:
            print(f"[{datetime.now()}] エラーが発生しました: {str(e)}")
            # エラー時のスクリーンショットを保存
            try:
                page.screenshot(path='/tmp/error_screenshot.png')
                print(f"[{datetime.now()}] エラー時のスクリーンショットを保存しました")
            except:
                pass
            raise
        
        finally:
            browser.close()
            print(f"[{datetime.now()}] ブラウザを閉じました")


def upload_to_spreadsheet(csv_path):
    """CSVをGoogle Spreadsheetsにアップロード"""
    
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
    
    # スプレッドシートを開く（URLまたは名前で指定）
    # ここでは環境変数からスプレッドシートIDを取得
    spreadsheet_id = os.environ.get('SPREADSHEET_ID')
    if not spreadsheet_id:
        raise Exception("環境変数 SPREADSHEET_ID が設定されていません")
    
    spreadsheet = gc.open_by_key(spreadsheet_id)
    
    # 最初のワークシートを取得（または新規作成）
    try:
        worksheet = spreadsheet.worksheet('Presco Data')
    except:
        worksheet = spreadsheet.add_worksheet(title='Presco Data', rows=1000, cols=20)
    
    print(f"[{datetime.now()}] ワークシート 'Presco Data' を準備しました")
    
    # CSVを読み込み
    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        csv_reader = csv.reader(f)
        data = list(csv_reader)
    
    print(f"[{datetime.now()}] CSVデータを読み込みました（{len(data)}行）")
    
    # 既存データをクリア
    worksheet.clear()
    
    # データを書き込み
    worksheet.update('A1', data)
    
    print(f"[{datetime.now()}] Google Sheetsへのアップロードが完了しました")
    print(f"[{datetime.now()}] スプレッドシートURL: https://docs.google.com/spreadsheets/d/{spreadsheet_id}")


def main():
    """メイン処理"""
    try:
        print("=" * 60)
        print(f"[{datetime.now()}] Presco自動同期を開始します")
        print("=" * 60)
        
        # CSVをダウンロード
        csv_path = login_and_download_csv()
        
        # Google Sheetsにアップロード
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
