import pandas as pd
import sqlite3
import os
import sys

# プロジェクトルートをsys.pathに追加
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.database.connection import get_db_connection
from src.utils.config import LOCAL_DATA_PATHS, ENCODING, ZP02_COLUMNS, ZP51N_COLUMNS

def import_data_from_files():
    """
    設定ファイルで定義されたパスからデータを読み込み、SQLiteデータベースにインポートする。
    """
    print("データインポート処理を開始します...")

    try:
        conn = get_db_connection()
        print("データベース接続に成功しました。")

        # ZP02データのインポート
        _load_and_insert(
            conn=conn,
            file_path=LOCAL_DATA_PATHS['ZP02'],
            table_name='zp02',
            columns=ZP02_COLUMNS,
            encoding=ENCODING['input']
        )

        # ZP51Nデータのインポート
        _load_and_insert(
            conn=conn,
            file_path=LOCAL_DATA_PATHS['ZP51N'],
            table_name='zp51n',
            columns=ZP51N_COLUMNS,
            encoding=ENCODING['input']
        )

        conn.close()
        print("データベース接続を閉じました。")
        print("データインポート処理が正常に完了しました。")
        return True

    except Exception as e:
        print(f"データインポート中にエラーが発生しました: {e}")
        return False

def _load_and_insert(conn: sqlite3.Connection, file_path: str, table_name: str, columns: list, encoding: str):
    """
    単一のファイルを読み込み、データベースのテーブルに挿入するヘルパー関数。
    """
    print(f"'{file_path}' から '{table_name}' テーブルへデータをインポート中...")

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"データファイルが見つかりません: {file_path}")

    try:
        # タブ区切りのファイルを読み込む
        df = pd.read_csv(
            file_path,
            sep='\t',
            header=None,
            names=columns,
            encoding=encoding,
            dtype=str,  # 全てのカラムを文字列として読み込み、後の処理で型変換する
            skiprows=1, # 先頭行（ヘッダー）をスキップする
            on_bad_lines='warn' # 不正な行を警告として表示
        )

        # データベースに書き込む
        df.to_sql(table_name, conn, if_exists='replace', index=False)

        print(f"'{table_name}' テーブルへのインポートが完了しました。{len(df)}件")

    except Exception as e:
        print(f"'{file_path}' の処理中にエラーが発生しました: {e}")
        raise


if __name__ == '__main__':
    # このスクリプトを直接実行した場合のテスト処理
    print("--- データインポーターのテスト実行 ---")

    # 既存のDBファイルがあれば削除してクリーンな状態でテスト
    db_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'data', 'production.db')
    if os.path.exists(db_path):
        os.remove(db_path)
        print(f"既存のデータベースファイル '{db_path}' を削除しました。")

    success = import_data_from_files()

    if success:
        print("\n--- インポート結果の検証 ---")
        try:
            conn = get_db_connection()
            # zp02テーブルの件数を確認
            zp02_count = pd.read_sql_query("SELECT COUNT(*) FROM zp02", conn).iloc[0, 0]
            print(f"zp02テーブルの件数: {zp02_count}")
            # zp51nテーブルの件数を確認
            zp51n_count = pd.read_sql_query("SELECT COUNT(*) FROM zp51n", conn).iloc[0, 0]
            print(f"zp51nテーブルの件数: {zp51n_count}")
            conn.close()
        except Exception as e:
            print(f"検証中にエラーが発生しました: {e}")
    else:
        print("\nインポート処理に失敗したため、検証はスキップします。")

    print("\n--- テスト実行完了 ---")
