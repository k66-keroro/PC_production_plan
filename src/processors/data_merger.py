import pandas as pd
import os

from ..database.connection import get_db_connection

def update_completion_history(conn, merged_df):
    """
    完了したオーダーの情報を履歴テーブルに保存する。
    """
    print("完了実績の履歴を更新します...")
    try:
        # 履歴テーブルが存在しない場合は作成する
        conn.execute("""
            CREATE TABLE IF NOT EXISTS completion_history (
                "子指図番号" TEXT PRIMARY KEY,
                "完了日" TIMESTAMP,
                "計画終了日" TIMESTAMP,
                "所要日" TIMESTAMP
            )
        """)

        # 完了したオーダー（DLV日付がある）を抽出
        completed_orders = merged_df[merged_df['DLV日付'].notna()].copy()

        if completed_orders.empty:
            print("新規の完了オーダーはありませんでした。")
            return

        # 履歴テーブルに必要なカラムを選択・整形
        history_df = pd.DataFrame({
            '子指図番号': completed_orders['指図番号'],
            '完了日': pd.to_datetime(completed_orders['DLV日付'], errors='coerce'),
            '計画終了日': pd.to_datetime(completed_orders['計画終了'], errors='coerce'),
            '所要日': completed_orders['所要日_dt']
        })
        history_df.dropna(subset=['子指図番号', '完了日', '計画終了日', '所要日'], inplace=True)

        # 既存の履歴を読み込む
        try:
            existing_history = pd.read_sql_query("SELECT * FROM completion_history", conn)
            # 新規の履歴のみを抽出
            new_history = history_df[~history_df['子指図番号'].isin(existing_history['子指図番号'])]
        except pd.io.sql.DatabaseError:
            # テーブルが存在しない場合は、全ての完了オーダーが新規履歴
            new_history = history_df

        if not new_history.empty:
            # 新規履歴をDBに追記
            new_history.to_sql('completion_history', conn, if_exists='append', index=False)
            print(f"{len(new_history)}件の新規完了履歴を保存しました。")
        else:
            print("新規に保存する完了履歴はありませんでした。")

    except Exception as e:
        print(f"履歴更新中にエラーが発生しました: {e}")


def get_merged_data():
    """
    データベースからzp02とzp51nテーブルを読み込み、要件に従って統合・処理し、
    最終的な表示用データフレームを返す。
    """
    print("データ統合処理を開始します...")

    try:
        conn = get_db_connection()

        # 1. データベースからテーブルを読み込む
        df_zp02 = pd.read_sql_query("SELECT * FROM zp02", conn)
        df_zp51n = pd.read_sql_query("SELECT * FROM zp51n", conn)
        print(f"zp02: {len(df_zp02)}件, zp51n: {len(df_zp51n)}件 のデータを読み込みました。")

        # --- デバッグ出力ここから ---
        print("\n--- zp02 raw data ---")
        print(df_zp02.info())
        print(df_zp02.head())

        print("\n--- zp51n raw data ---")
        print(df_zp51n.info())
        print(df_zp51n.head())
        print("\n'子指図番号' in zp51n (non-null values):")
        print(df_zp51n[df_zp51n['子指図番号'].notna()][['子指図番号', '所要日']].head())
        # --- デバッグ出力ここまで ---

        # 2. zp51nをサマリー化する
        #    子指図番号ごとに所要日の最小値を持つレコードを抽出

        # 日付形式の変換 (YYYY/MM/DDなどを想定)
        df_zp51n['所要日_dt'] = pd.to_datetime(df_zp51n['所要日'], errors='coerce')

        # NaNを除外してソートし、重複を削除
        zp51_summary = df_zp51n.dropna(subset=['所要日_dt', '子指図番号'])
        zp51_summary = zp51_summary.sort_values('所要日_dt').drop_duplicates(subset='子指図番号', keep='first')
        print(f"zp51nをサマリー化しました。結果: {len(zp51_summary)}件")

        # 3. zp02とサマリー化したzp51nを結合する
        merged_df = pd.merge(
            df_zp02,
            zp51_summary,
            left_on='指図番号',
            right_on='子指図番号',
            how='left',
            suffixes=('_zp02', '_zp51n') # 重複カラムの接尾辞
        )
        print(f"zp02とzp51nをマージしました。結果: {len(merged_df)}件")

        # 5. 完了実績の履歴を更新
        update_completion_history(conn, merged_df)

        # 6. 表示用のカラムを選択・整形する
        final_df = pd.DataFrame()
        final_df['親指図番号'] = merged_df['親指図番号']
        final_df['親品目コード'] = merged_df['親品目コード']
        final_df['親品目テキスト'] = merged_df['親品目テキスト']
        final_df['子指図番号'] = merged_df['指図番号']
        final_df['子品目コード'] = merged_df['品目コード']
        final_df['子品目テキスト'] = merged_df['品目テキスト']
        final_df['所要日'] = merged_df['所要日_dt'].dt.strftime('%Y-%m-%d')
        final_df['子指図計画開始日'] = pd.to_datetime(merged_df['計画開始'], errors='coerce').dt.strftime('%Y-%m-%d')
        final_df['子指図計画終了日'] = pd.to_datetime(merged_df['計画終了'], errors='coerce').dt.strftime('%Y-%m-%d')

        # 「計画数量」を数値に変換
        final_df['計画数量'] = pd.to_numeric(merged_df['完成残数'], errors='coerce').fillna(0)

        # 進捗フィールドの作成
        def get_progress_status(row):
            if pd.notna(row['検査']):
                return '検査'
            if pd.notna(row['A']):
                return 'A'
            if pd.notna(row['C']):
                return 'C'
            if pd.notna(row['工程(子)']):
                return '工程'
            if pd.isna(row['親指図番号']):
                return '対象外'
            return '未着手'
        final_df['進捗'] = merged_df.apply(get_progress_status, axis=1)

        # 進捗詳細カラムの追加
        final_df['工程(子)'] = merged_df['工程(子)']
        final_df['C'] = merged_df['C']
        final_df['A'] = merged_df['A']
        final_df['C,A以外'] = merged_df['C,A以外']
        final_df['検査'] = merged_df['検査']

        final_df['完成日'] = pd.to_datetime(merged_df['DLV日付'], errors='coerce').dt.strftime('%Y-%m-%d')

        # 「子MRP管理者」をZP51Nから取得
        final_df['子MRP管理者'] = merged_df['子MRP管理者']

        # MRP管理者グルーピング
        def group_manager(manager):
            if isinstance(manager, str) and manager.startswith('PC') and manager[2:].isdigit():
                 num = int(manager[2:])
                 if 1 <= num <= 6:
                     return 'PC'
            return manager

        final_df['MRP管理者グループ'] = final_df['子MRP管理者'].apply(group_manager)

        # 遵守状況の計算
        plan_end_date = pd.to_datetime(final_df['子指図計画終了日'], errors='coerce')
        completion_date = pd.to_datetime(final_df['完成日'], errors='coerce')

        final_df['遵守状況'] = '未完成'
        final_df.loc[completion_date.notna(), '遵守状況'] = '未遵守'
        final_df.loc[completion_date <= plan_end_date, '遵守状況'] = '遵守'

        # 5. ソート順を適用
        #    所要日、子MRP管理者、子指図番号
        final_df = final_df.sort_values(
            by=['所要日', '子MRP管理者', '子指図番号'],
            ascending=[True, True, True]
        ).reset_index(drop=True)

        # No列を追加
        final_df.insert(0, 'No', final_df.index + 1)

        print("カラムの整形とソートが完了しました。")

        return final_df

    except Exception as e:
        print(f"データ統合中にエラーが発生しました: {e}")
        return pd.DataFrame() # エラー時は空のDataFrameを返す
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            print("データベース接続を閉じました。")
