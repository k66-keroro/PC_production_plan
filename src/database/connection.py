import sqlite3
from sqlite3 import Connection
import os

# config.pyがsrc/utilsにあるため、sys.pathを調整してインポート
import sys
# connection.pyから見て、srcディレクトリにパスを追加
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config import DATABASE_PATH

def get_db_connection() -> Connection:
    """
    SQLiteデータベースへの接続を取得します。
    データベースファイルが存在しない場合は、新しく作成されます。

    Returns:
        Connection: sqlite3の接続オブジェクト
    """
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        # print(f"Successfully connected to database at {DATABASE_PATH}")
        return conn
    except sqlite3.Error as e:
        print(f"Database connection error: {e}")
        raise

if __name__ == '__main__':
    # モジュールの簡単なテスト
    print("Testing database connection...")
    try:
        connection = get_db_connection()
        print("Connection successful.")

        # データベースファイルの存在を確認
        if os.path.exists(DATABASE_PATH):
            print(f"Database file created at: {DATABASE_PATH}")
        else:
            print(f"Database file NOT found at: {DATABASE_PATH}")

        # 簡単なクエリを実行
        cursor = connection.cursor()
        cursor.execute("SELECT sqlite_version();")
        db_version = cursor.fetchone()
        print(f"SQLite version: {db_version[0]}")

        connection.close()
        print("Connection closed.")

        # テストで作成されたファイルを削除
        if os.path.exists(DATABASE_PATH):
            os.remove(DATABASE_PATH)
            print("Test database file removed.")

    except Exception as e:
        print(f"An error occurred during testing: {e}")
