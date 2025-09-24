import sqlite3
from sqlite3 import Connection
import os

from ..utils.config import DATABASE_PATH

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
