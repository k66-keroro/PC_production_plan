import pandas as pd
import os
from datetime import date, datetime

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
        snapshot_df = df_zp02[['指図番号', '計画終了']].copy()
        snapshot_df['snapshot_date'] = today_str

        # 型を変換
        snapshot_df['計画終了'] = pd.to_datetime(snapshot_df['計画終了'], errors='coerce')

        # データベースに書き込む
        snapshot_df.to_sql(table_name, conn, if_exists='append', index=False)
        print(f"{len(snapshot_df)}件の計画スナップショットを '{table_name}' に保存しました。")

    except Exception as e:
        print(f"計画履歴の更新中にエラーが発生しました: {e}")


def _add_column_if_not_exists(conn, table_name, column_name, column_type):
    """
    テーブルに指定されたカラムが存在しない場合、追加する。
    """
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info({table_name})")
    columns = [row[1] for row in cursor.fetchall()]
    if column_name not in columns:
        print(f"'{table_name}'テーブルに'{column_name}'カラムを追加します...")
        cursor.execute(f'ALTER TABLE {table_name} ADD COLUMN "{column_name}" {column_type}')
        print(f"'{column_name}'カラムの追加が完了しました。")

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
                "完了日" TIMESTAMP
            )
        """)

        # スキーママイグレーション：旧バージョンとの互換性のため、カラムが存在しない場合は追加
        _add_column_if_not_exists(conn, completion_table, "基準計画終了日", "TIMESTAMP")

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
        print("DEBUG merged_df columns:", merged_df.columns.tolist())
        print("DEBUG merged_df sample:", merged_df.head(3).to_dict())

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

        # 2. ZP02の全データに対して計画履歴を更新
        update_plan_history(conn, df_zp02)

        # 3. ZP02をPC対象でフィルタリング (これがマスターデータになる)
        #    ZP02のMRP管理者を'PC'で始まるものでフィルタ
        base_df = df_zp02[df_zp02['MRP管理者'].str.startswith('PC', na=False)].copy()
        print(f"ZP02のPC対象オーダーをマスターデータとします。結果: {len(base_df)}件")

        # 4. ZP51Nをサマリー化する (所要日の最小値を取得)
        df_zp51n['所要日_dt'] = pd.to_datetime(df_zp51n['所要日'], infer_datetime_format=True, dayfirst=False, errors='coerce')
        zp51_summary = df_zp51n.dropna(subset=['所要日_dt', '子指図番号'])
        # drop_duplicatesの前に、必要なカラムだけを選択しておく
        zp51_summary = zp51_summary.sort_values('所要日_dt').drop_duplicates(
            subset='子指図番号',
            keep='first'
        )[['親指図番号','親品目コード', '親品目テキスト', '子指図番号', '所要日_dt', '子MRP管理者', '工程(子)', '進捗', '親指図計画開始日', '親指図計画終了日', '子指図計画開始日', '子指図計画終了日']] # 必要なカラムを追加
        print(f"zp51nをサマリー化しました。結果: {len(zp51_summary)}件")

        # 5. ZP02マスターにZP51NサマリーをLEFT JOIN
        merged_df = pd.merge(
            base_df,
            zp51_summary,
            left_on='指図番号',
            right_on='子指図番号',
            how='left'
        )
        print(f"PC対象のZP02マスターにZP51NサマリーをLEFT JOINしました。結果: {len(merged_df)}件")

        # ZP02の日付カラムをdatetimeに変換
        for col in ['計画開始', '計画終了', 'DLV日付']:
            merged_df[col] = pd.to_datetime(merged_df[col], infer_datetime_format=True, dayfirst=False, errors='coerce')

        # ZP51Nの日付カラムをdatetimeに変換 (merged_dfに結合されたもの)
        for col in ['親指図計画開始日', '親指図計画終了日', '子指図計画開始日', '子指図計画終了日']:
            merged_df[col] = pd.to_datetime(merged_df[col], infer_datetime_format=True, dayfirst=False, errors='coerce')

        # 6. 完了実績の履歴を更新 (JOIN後の全PCデータが対象)
        update_completion_history(conn, merged_df)

        # 7. 表示用のカラムを選択・整形する
        final_df = pd.DataFrame()
        final_df['親指図番号'] = merged_df['親指図番号']
        final_df['親品目コード'] = merged_df['親品目コード']
        final_df['親品目テキスト'] = merged_df['親品目テキスト']
        final_df['子指図番号'] = merged_df['指図番号']
        final_df['子品目コード'] = merged_df['品目コード']
        final_df['子品目テキスト'] = merged_df['品目テキスト']

        # 日付カラムの変換と所要日代替処理
        final_df['所要日_dt'] = merged_df['所要日_dt'] # ZP51Nから既にdatetimeとして取得
        final_df['子指図計画開始日_dt'] = merged_df['計画開始'] # ZP02からdatetimeとして取得
        final_df['子指図計画終了日_dt'] = merged_df['計画終了'] # ZP02からdatetimeとして取得

        # 所要日がない場合は計画終了日を代替とする
        final_df['基準所要日'] = final_df['所要日_dt'].fillna(final_df['子指図計画終了日_dt'])

        final_df['所要日'] = final_df['所要日_dt'].dt.strftime('%Y-%m-%d').replace({pd.NaT: None})
        final_df['子指図計画開始日'] = final_df['子指図計画開始日_dt'].dt.strftime('%Y-%m-%d').replace({pd.NaT: None})
        final_df['子指図計画終了日'] = final_df['子指図計画終了日_dt'].dt.strftime('%Y-%m-%d').replace({pd.NaT: None})
        
        final_df['計画数量'] = pd.to_numeric(merged_df['完成残数'], errors='coerce').fillna(0)
        # ZP51Nに情報がない場合、ZP02のMRP管理者とMRP管理者グループを結合して使用
        final_df['子MRP管理者'] = merged_df['子MRP管理者'].fillna(
            merged_df['MRP管理者'].fillna('') + merged_df['MRP管理者グループ'].fillna('')
        ).replace({'': None})

        # [Req 3] 進捗フィールドの作成
        final_df['進捗'] = merged_df['進捗']

        # [Req 2.2] 生産タイプを分類
        def get_production_type(manager):
            if isinstance(manager, str):
                normalized_manager = manager.strip().upper()
                if normalized_manager in ['PC1', 'PC2', 'PC3']: return '内製'
                if normalized_manager in ['PC4', 'PC5', 'PC6']: return '外製'
            return 'その他'
        final_df['生産タイプ'] = final_df['子MRP管理者'].apply(get_production_type)

        # [Req 1] 遵守状況の計算
        try:
            completion_history = pd.read_sql_query("SELECT * FROM completion_history", conn)
            completion_history['完了日'] = pd.to_datetime(completion_history['完了日'], infer_datetime_format=True, dayfirst=False, errors='coerce')
            completion_history['基準計画終了日'] = pd.to_datetime(completion_history['基準計画終了日'], infer_datetime_format=True, dayfirst=False, errors='coerce')
            final_df = pd.merge(final_df, completion_history, on='子指図番号', how='left')
        except pd.io.sql.DatabaseError:
            pass # テーブルがなければ何もしない

        if '完了日' not in final_df.columns: final_df['完了日'] = pd.NaT
        if '基準計画終了日' not in final_df.columns: final_df['基準計画終了日'] = pd.NaT

        # 新しい遵守状況ロジック
        today = pd.Timestamp.now().normalize() # 今日の日付（時間なし）

        final_df['遵守状況'] = '未完成'
        # DLVステータスを持つものを「完了」と判断する
        completed_mask = merged_df['指図ステータス'].str.contains('DLV', na=False)
        
        # 完了オーダーの遵守/未遵守
        final_df.loc[completed_mask & (final_df['完了日'] <= final_df['基準計画終了日']), '遵守状況'] = '遵守'
        final_df.loc[completed_mask & (final_df['完了日'] > final_df['基準計画終了日']), '遵守状況'] = '未遵守'

        # 未完成オーダーで所要日を過ぎているものを「遅延」とする
        # 基準所要日があり、かつ今日より過去の場合
        delayed_mask = (~completed_mask) & (final_df['基準所要日'].notna()) & (final_df['基準所要日'] < today)
        final_df.loc[delayed_mask, '遵守状況'] = '遅延'

        # 来月以降の生産完了（完成）の可視化 - 先行生産
        # 完了済みで、完了日が基準所要日より7日以上早く、かつ基準所要日が未来の場合
        early_production_mask = (completed_mask) & \
                                (final_df['完了日'].notna()) & \
                                (final_df['基準所要日'].notna()) & \
                                (final_df['完了日'] < (final_df['基準所要日'] - pd.Timedelta(days=7))) & \
                                (final_df['基準所要日'] > today)
        final_df['先行生産'] = False
        final_df.loc[early_production_mask, '先行生産'] = True

        # 計画終了日と所要日の乖離
        final_df['所要日乖離日数'] = (final_df['子指図計画終了日_dt'] - final_df['基準所要日']).dt.days

        final_df['完了日'] = final_df['完了日'].dt.strftime('%Y-%m-%d').replace({pd.NaT: None})
        final_df['基準計画終了日'] = final_df['基準計画終了日'].dt.strftime('%Y-%m-%d').replace({pd.NaT: None})
        final_df['基準所要日'] = final_df['基準所要日'].dt.strftime('%Y-%m-%d').replace({pd.NaT: None})


        # 8. ソート順を適用
        final_df = final_df.sort_values(
            by=['基準所要日', '子MRP管理者', '子指図番号'], # 所要日を基準所要日に変更
            ascending=[True, True, True],
            na_position='last' # NaN値を末尾に
        ).reset_index(drop=True)

        final_df.insert(0, 'No', final_df.index + 1)
        print("カラムの整形とソートが完了しました。")
        return final_df

    except Exception as e:
        print(f"データ統合中にエラーが発生しました: {e}")
        return pd.DataFrame()
    finally:
        if 'conn' in locals() and conn:
            conn.close()
            print("データベース接続を閉じました。")
