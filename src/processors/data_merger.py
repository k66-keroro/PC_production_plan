import pandas as pd
import os
from datetime import date

from ..database.connection import get_db_connection

def update_plan_history(conn, df_zp02):
    """
    毎日の計画スナップショットを保存する。
    """
    print("計画履歴スナップショットを更新します...")
    today_str = date.today().strftime('%Y-%m-%d')
    table_name = "plan_history"

    try:
        # 履歴テーブルが存在しない場合は作成する
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                "snapshot_date" TEXT,
                "指図番号" TEXT,
                "計画終了" TIMESTAMP,
                "所要日" TIMESTAMP,
                PRIMARY KEY ("snapshot_date", "指図番号")
            )
        """)

        # 今日の日付のスナップショットが既に存在するか確認
        # Note: pandas.io.sql.DatabaseError will be raised if table does not exist.
        try:
            existing_today = pd.read_sql_query(
                f"SELECT 1 FROM {table_name} WHERE snapshot_date = '{today_str}' LIMIT 1", conn
            )
            if not existing_today.empty:
                print(f"{today_str} のスナップショットは既に存在するため、スキップします。")
                return
        except pd.io.sql.DatabaseError:
             # テーブルが存在しない場合は、そのまま進む
            pass


        # 保存するスナップショットデータを作成
        snapshot_df = df_zp02[['指図番号', '計画終了', '所要日']].copy()
        snapshot_df['snapshot_date'] = today_str

        # 型を変換
        snapshot_df['計画終了'] = pd.to_datetime(snapshot_df['計画終了'], errors='coerce')
        snapshot_df['所要日'] = pd.to_datetime(snapshot_df['所要日'], errors='coerce')

        # データベースに書き込む
        snapshot_df.to_sql(table_name, conn, if_exists='append', index=False)
        print(f"{len(snapshot_df)}件の計画スナップショットを '{table_name}' に保存しました。")

    except Exception as e:
        print(f"計画履歴の更新中にエラーが発生しました: {e}")


def update_completion_history(conn, merged_df):
    """
    完了したオーダーの情報を履歴テーブルに保存する。
    履歴には、完了日と、そのオーダーの初回計画日（基準計画終了日）を保存する。
    """
    print("完了実績の履歴を更新します...")
    completion_table = "completion_history"
    plan_table = "plan_history"

    try:
        # 完了履歴テーブルが存在しない場合は作成
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {completion_table} (
                "子指図番号" TEXT PRIMARY KEY,
                "完了日" TIMESTAMP,
                "基準計画終了日" TIMESTAMP
            )
        """)

        # 完了したオーダー（DLV日付がある）を抽出
        completed_orders = merged_df[merged_df['DLV日付'].notna()].copy()
        if completed_orders.empty:
            print("新規の完了オーダーはありませんでした。")
            return

        # 既存の完了履歴を読み込み、既に処理済みのオーダーを除外
        try:
            existing_completed = pd.read_sql_query(f"SELECT \"子指図番号\" FROM {completion_table}", conn)
            completed_orders = completed_orders[~completed_orders['指図番号'].isin(existing_completed['子指図番号'])]
        except pd.io.sql.DatabaseError:
            # テーブルがまだ存在しない場合は何もしない
            pass

        if completed_orders.empty:
            print("保存済みの完了オーダー以外に、新規の完了オーダーはありませんでした。")
            return

        # 計画履歴を読み込む
        plan_history = pd.read_sql_query(f"SELECT * FROM {plan_table}", conn)
        plan_history['計画終了'] = pd.to_datetime(plan_history['計画終了'])

        # 各完了オーダーの基準計画終了日（最も古い計画終了日）を見つける
        new_history_list = []
        for index, order in completed_orders.iterrows():
            order_number = order['指図番号']
            order_plan_history = plan_history[plan_history['指図番号'] == order_number]

            if not order_plan_history.empty:
                baseline_plan_end = order_plan_history['計画終了'].min()
                new_history_list.append({
                    '子指図番号': order_number,
                    '完了日': pd.to_datetime(order['DLV日付']),
                    '基準計画終了日': baseline_plan_end
                })

        if not new_history_list:
            print("新規完了オーダーに対応する計画履歴が見つかりませんでした。")
            return

        new_history_df = pd.DataFrame(new_history_list)
        new_history_df.dropna(subset=['子指図番号', '完了日', '基準計画終了日'], inplace=True)

        # 新規履歴をDBに追記
        new_history_df.to_sql(completion_table, conn, if_exists='append', index=False)
        print(f"{len(new_history_df)}件の新規完了履歴を保存しました。")

    except Exception as e:
        print(f"完了履歴の更新中にエラーが発生しました: {e}")


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

        # 計画履歴を更新
        update_plan_history(conn, df_zp02)

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

        # 5. [Req 2.1] MRP管理者が'PC'で始まるもののみにフィルタリング
        #    ZP51N由来の'子MRP管理者'を使用する
        merged_df['子MRP管理者'] = merged_df['子MRP管理者'] # ZP51N側のカラムを優先
        pc_orders_df = merged_df[merged_df['子MRP管理者'].str.startswith('PC', na=False)].copy()
        print(f"MRP管理者が'PC'で始まるオーダーにフィルタリングしました。結果: {len(pc_orders_df)}件")

        # 6. 完了実績の履歴を更新
        update_completion_history(conn, pc_orders_df)

        # 7. 表示用のカラムを選択・整形する
        final_df = pd.DataFrame()
        final_df['親指図番号'] = pc_orders_df['親指図番号']
        final_df['親品目コード'] = pc_orders_df['親品目コード']
        final_df['親品目テキスト'] = pc_orders_df['親品目テキスト']
        final_df['子指図番号'] = pc_orders_df['指図番号']
        final_df['子品目コード'] = pc_orders_df['品目コード']
        final_df['子品目テキスト'] = pc_orders_df['品目テキスト']
        final_df['所要日'] = pc_orders_df['所要日_dt'].dt.strftime('%Y-%m-%d')
        final_df['子指図計画開始日'] = pd.to_datetime(pc_orders_df['計画開始'], errors='coerce').dt.strftime('%Y-%m-%d')
        final_df['子指図計画終了日'] = pd.to_datetime(pc_orders_df['計画終了'], errors='coerce').dt.strftime('%Y-%m-%d')
        final_df['計画数量'] = pd.to_numeric(pc_orders_df['完成残数'], errors='coerce').fillna(0)
        final_df['子MRP管理者'] = pc_orders_df['子MRP管理者']

        # [Req 3] 進捗フィールドの作成 (ZP51の'工程(子)'を使用。'○'があれば完了)
        final_df['進捗'] = pc_orders_df['工程(子)'].apply(lambda x: '完了' if isinstance(x, str) and '○' in x else '未完了')

        # [Req 2.2] 生産タイプを分類
        def get_production_type(manager):
            if isinstance(manager, str):
                if manager in ['PC1', 'PC2', 'PC3']:
                    return '内製'
                if manager in ['PC4', 'PC5', 'PC6']:
                    return '外製'
            return 'その他'
        final_df['生産タイプ'] = final_df['子MRP管理者'].apply(get_production_type)


        # [Req 1] 遵守状況の計算
        # 完了履歴テーブルを読み込む
        try:
            completion_history = pd.read_sql_query("SELECT * FROM completion_history", conn)
            completion_history['完了日'] = pd.to_datetime(completion_history['完了日'])
            completion_history['基準計画終了日'] = pd.to_datetime(completion_history['基準計画終了日'])

            # 完了履歴をマージ
            final_df = pd.merge(final_df, completion_history, on='子指図番号', how='left')
        except pd.io.sql.DatabaseError:
            # 履歴テーブルがまだ存在しない場合は何もしない
            pass

        # マージ後にカラムが存在しない場合（履歴テーブルが空の場合など）に備えて、カラムを安全に追加
        if '完了日' not in final_df.columns:
            final_df['完了日'] = pd.NaT
        if '基準計画終了日' not in final_df.columns:
            final_df['基準計画終了日'] = pd.NaT

        # 遵守状況を計算
        final_df['遵守状況'] = '未完成'
        completed_mask = final_df['完了日'].notna()
        final_df.loc[completed_mask, '遵守状況'] = '未遵守'
        final_df.loc[completed_mask & (final_df['完了日'] <= final_df['基準計画終了日']), '遵守状況'] = '遵守'

        # 表示用に日付をフォーマット（NaTを無視）
        final_df['完了日'] = final_df['完了日'].dt.strftime('%Y-%m-%d').replace({pd.NaT: None})
        final_df['基準計画終了日'] = final_df['基準計画終了日'].dt.strftime('%Y-%m-%d').replace({pd.NaT: None})

        # 8. ソート順を適用
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
