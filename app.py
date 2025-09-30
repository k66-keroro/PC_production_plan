import streamlit as st
import pandas as pd
from src.processors.data_merger import get_merged_data
from src.importers.data_importer import import_data_from_files
from src.utils.config import UI_CONFIG
from src.database.connection import get_db_connection
import os
import plotly.express as px

# --- ページ設定 ---
st.set_page_config(layout="wide", page_title=UI_CONFIG['title'])

# --- データ取得 ---
@st.cache_data(ttl=600) # 10分間キャッシュ
def get_completion_history():
    """完了履歴テーブルをDBから読み込む"""
    try:
        conn = get_db_connection()
        df = pd.read_sql_query("SELECT * FROM completion_history", conn)
        conn.close()
        return df
    except Exception as e:
        # テーブルがない場合も空のDataFrameを返す
        print(f"履歴テーブルの読み込みに失敗: {e}")
        return pd.DataFrame(columns=['子指図番号', '完了日', '基準計画終了日'])

# --- UIコンポーネント ---
def display_compliance_dashboard(df):
    """遵守率ダッシュボードを表示する"""
    st.header("📈 遵守率ダッシュボード")

    # 完了履歴データはdfから直接取得
    completed_df = df[df['遵守状況'].isin(['遵守', '未遵守'])].copy()
    completed_df['完了日_dt'] = pd.to_datetime(completed_df['完了日'])
    completed_df['基準計画終了日_dt'] = pd.to_datetime(completed_df['基準計画終了日'])

    # 遅延データ
    delayed_df = df[df['遵守状況'] == '遅延'].copy()
    delayed_df['基準所要日_dt'] = pd.to_datetime(delayed_df['基準所要日'])

    today = pd.Timestamp.now().normalize()

    col1, col2, col3 = st.columns(3)

    with col1:
        st.subheader("完了オーダーの遵守率")
        if not completed_df.empty:
            # 週次遵守率
            start_of_week = today - pd.to_timedelta(today.dayofweek, unit='d')
            weekly_completed = completed_df[completed_df['完了日_dt'] >= start_of_week]
            if not weekly_completed.empty:
                weekly_rate = (weekly_completed['遵守状況'] == '遵守').mean() * 100
                st.metric(
                    label="今週の遵守率",
                    value=f"{weekly_rate:.1f}%",
                    delta=f"{(weekly_completed['遵守状況'] == '遵守').sum()}件 / {len(weekly_completed)}件"
                )
            else:
                st.metric(label="今週の遵守率", value="N/A", delta="今週の完成実績なし")

            # 月次遵守率
            start_of_month = today.replace(day=1)
            monthly_completed = completed_df[completed_df['完了日_dt'] >= start_of_month]
            if not monthly_completed.empty:
                monthly_rate = (monthly_completed['遵守状況'] == '遵守').mean() * 100
                st.metric(
                    label="今月の遵守率",
                    value=f"{monthly_rate:.1f}%",
                    delta=f"{(monthly_completed['遵守状況'] == '遵守').sum()}件 / {len(monthly_completed)}件"
                )
            else:
                st.info("今月の遵守率")
        else:
            st.error('完了オーダーデータがまだありません。')

    with col2:
        st.subheader("未完成オーダーの遅延状況")
        if not delayed_df.empty:
            total_incomplete = len(df[df['遵守状況'] == '未完成']) + len(delayed_df)
            st.metric(
                label="現在の遅延件数",
                value=f"{len(delayed_df)}件",
                delta=f"全未完成オーダーの{len(delayed_df)/total_incomplete*100:.1f}%"
            )

            # 四半期遅延割合 (会計期間4-3月)
            current_month = today.month
            if current_month >= 4 and current_month <= 6: # Q1 (4-6月)
                current_quarter_start = pd.Timestamp(today.year, 4, 1)
            elif current_month >= 7 and current_month <= 9: # Q2 (7-9月)
                current_quarter_start = pd.Timestamp(today.year, 7, 1)
            elif current_month >= 10 and current_month <= 12: # Q3 (10-12月)
                current_quarter_start = pd.Timestamp(today.year, 10, 1)
            else: # Q4 (1-3月)
                current_quarter_start = pd.Timestamp(today.year - 1, 1, 1) if current_month >= 1 and current_month <= 3 else pd.Timestamp(today.year, 1, 1)

            # 期間内の全未完成オーダーを抽出
            all_incomplete_in_quarter = df[(~df['遵守状況'].isin(['遵守', '未遵守'])) & (pd.to_datetime(df['基準所要日'], errors='coerce') >= current_quarter_start)]
            quarterly_delayed = delayed_df[delayed_df['基準所要日_dt'] >= current_quarter_start]

            if not all_incomplete_in_quarter.empty:
                quarterly_rate = len(quarterly_delayed) / len(all_incomplete_in_quarter) * 100
                st.metric(
                    label="今四半期の遅延割合",
                    value=f"{quarterly_rate:.1f}%",
                    delta=f"{len(quarterly_delayed)}件 / {len(all_incomplete_in_quarter)}件"
                )
            else:
                st.info("今四半期の未完成オーダー実績なし")

            # 今期遅延割合 (会計期間4-3月)
            current_fiscal_year_start_month = 4
            if today.month >= current_fiscal_year_start_month:
                current_fiscal_year_start = pd.Timestamp(today.year, current_fiscal_year_start_month, 1)
            else:
                current_fiscal_year_start = pd.Timestamp(today.year - 1, current_fiscal_year_start_month, 1)
            
            # 期間内の全未完成オーダーを抽出
            all_incomplete_in_fiscal_year = df[
                (~df['遵守状況'].isin(['遵守', '未遵守'])) &
                (pd.to_datetime(df['基準所要日'], errors='coerce') >= current_fiscal_year_start)
            ]
            # fiscal_year_delayed を定義
            fiscal_year_delayed = delayed_df[delayed_df['基準所要日_dt'] >= current_fiscal_year_start]

            if not all_incomplete_in_fiscal_year.empty:
                fiscal_year_rate = len(fiscal_year_delayed) / len(all_incomplete_in_fiscal_year) * 100
                st.metric(
                    label="今期の遅延割合",
                    value=f"{fiscal_year_rate:.1f}%",
                    delta=f"{len(fiscal_year_delayed)}件 / {len(all_incomplete_in_fiscal_year)}件"
                )
            else:
                st.info("今期の未完成オーダー実績なし")

            # 当月遅延割合
            # 期間内の全未完成オーダーを抽出
            all_incomplete_in_month = df[
                (~df['遵守状況'].isin(['遵守', '未遵守'])) &
                (pd.to_datetime(df['基準所要日'], errors='coerce') >= today.replace(day=1))
            ]
            monthly_delayed = delayed_df[delayed_df['基準所要日_dt'] >= today.replace(day=1)]


            if not all_incomplete_in_month.empty:
                monthly_rate = len(monthly_delayed) / len(all_incomplete_in_month) * 100
                st.metric(
                    label="当月の遅延割合",
                    value=f"{monthly_rate:.1f}%",
                    delta=f"{len(monthly_delayed)}件 / {len(all_incomplete_in_month)}件"
                )
            else:
                st.info("当月の未完成オーダー実績なし")
        else:
            st.error('遅延オーダーデータがまだありません。')

    with col3:
        st.subheader("先行生産状況")
        early_production_df = df[df['先行生産'] == True]
        if not early_production_df.empty:
            st.metric(
                label="先行生産件数",
                value=f"{len(early_production_df)}件",
                delta=f"全オーダーの{len(early_production_df)/len(df)*100:.1f}%"
            )
        else:
            st.info("先行生産オーダーはありません。")


@st.cache_data
def to_excel(df):
    import io
    output = io.BytesIO()
    # xlsxwriter をエンジンとして指定
    with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
        # Excel出力用にデータをコピー
        df_excel = df.copy()
        
        # 日付カラムを強制的にYYYY-MM-DD形式に変換
        date_columns = ['子指図計画開始日', '子指図計画終了日', '所要日', '基準所要日', '完了日', '基準計画終了日']
        for col in date_columns:
            if col in df_excel.columns:
                # datetime型に変換
                df_excel[col] = pd.to_datetime(df_excel[col], errors='coerce')
                # YYYY-MM-DD形式の文字列に変換
                df_excel[col] = df_excel[col].dt.strftime('%Y-%m-%d')
                # NaTや無効な値を空文字に変換
                df_excel[col] = df_excel[col].replace({'NaT': '', 'None': '', 'nan': ''})
        
        # 乖離日数を整数に変換
        if '所要日乖離日数' in df_excel.columns:
            df_excel['所要日乖離日数'] = pd.to_numeric(df_excel['所要日乖離日数'], errors='coerce').fillna(0).astype(int)
        
        # Excelに書き出し
        df_excel.to_excel(writer, index=False, sheet_name='生産計画データ')
        workbook = writer.book
        worksheet = writer.sheets['生産計画データ']

        # 列幅を自動調整
        for i, col in enumerate(df_excel.columns):
            max_len = max(df_excel[col].astype(str).map(len).max(), len(str(col)))
            worksheet.set_column(i, i, max_len + 2) # +2は余白

        # ウィンドウ枠の固定 (1行目と1列目を固定)
        worksheet.freeze_panes(1, 1)

        # フィルタの追加
        (max_row, max_col) = df_excel.shape
        worksheet.autofilter(0, 0, max_row, max_col - 1)

    processed_data = output.getvalue()
    return processed_data
    return processed_data


# --- メインロジック ---

# --- タイトル ---
st.title(UI_CONFIG['title'])
st.caption("製造指図データ (ZP02) と部品構成データ (ZP51N) を統合し、生産計画の進捗を可視化します。")

# --- セッションステートの初期化 ---
if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False
    st.session_state.df = pd.DataFrame()
    st.session_state.last_update_time = None

# --- サイドバー ---
with st.sidebar:
    st.header("⚙️ 操作パネル")
    
    if st.button('🔄 データ更新', type="primary"):
        with st.spinner('ステップ1/2: データファイルをインポートしています...'):
            import_success = import_data_from_files()
        
        if import_success:
            st.toast("✅ ファイルのインポートが完了しました。")
            with st.spinner('ステップ2/2: データを統合・整形しています...'):
                df = get_merged_data()
                if not df.empty:
                    st.session_state.df = df
                    st.session_state.data_loaded = True
                    st.session_state.last_update_time = pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')
                    st.cache_data.clear()
                    
                    # ZP51N遅延データを自動保存
                    try:
                        # 遅延データを抽出
                        delayed_df = df[df['遵守状況'] == '遅延'].copy()
                        
                        if not delayed_df.empty:
                            # 必要なカラムを選択
                            columns_to_save = ['子指図番号', '子品目コード', '子品目テキスト', '基準所要日', '所要日', '所要日乖離日数']
                            existing_columns = [col for col in columns_to_save if col in delayed_df.columns]
                            df_to_save = delayed_df[existing_columns].copy()
                            
                            # カラム名をデータベース用にリネーム
                            column_mapping = {
                                '子指図番号': '子指図番号',
                                '子品目コード': '品目コード', 
                                '子品目テキスト': '品目テキスト',
                                '基準所要日': '基準所要日',
                                '所要日': '所要日',
                                '所要日乖離日数': '乖離日数'
                            }
                            df_to_save = df_to_save.rename(columns=column_mapping)
                            
                            # 日付カラムをフォーマット
                            date_cols = ['基準所要日', '所要日']
                            for col in date_cols:
                                if col in df_to_save.columns:
                                    df_to_save[col] = pd.to_datetime(df_to_save[col], errors='coerce').dt.strftime('%Y-%m-%d').replace({pd.NaT: None})
                            
                            # 乖離日数を整数に変換
                            if '乖離日数' in df_to_save.columns:
                                df_to_save['乖離日数'] = pd.to_numeric(df_to_save['乖離日数'], errors='coerce').fillna(0).astype(int)
                            
                            # データベースに保存
                            conn = get_db_connection()
                            
                            # テーブルが存在しない場合は作成
                            conn.execute('''
                                CREATE TABLE IF NOT EXISTS zp51n_delayed (
                                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                                    子指図番号 TEXT,
                                    品目コード TEXT,
                                    品目テキスト TEXT,
                                    基準所要日 TEXT,
                                    所要日 TEXT,
                                    乖離日数 INTEGER,
                                    登録日時 TEXT DEFAULT CURRENT_TIMESTAMP
                                )
                            ''')
                            
                            # 既存データを削除
                            conn.execute('DELETE FROM zp51n_delayed')
                            
                            # 新データを挿入
                            df_to_save.to_sql('zp51n_delayed', conn, if_exists='append', index=False)
                            
                            conn.commit()
                            conn.close()
                            
                            st.toast(f"✅ {len(df_to_save)}件の遅延データを自動保存しました。")
                        else:
                            st.toast("ℹ️ 遅延データはありません。")
                    except Exception as e:
                        st.toast(f"⚠️ 遅延データの保存に失敗しました: {e}")
                    
                    st.success('データの更新が完了しました。')
                else:
                    st.error('データの統合に失敗しました。詳細はコンソールログを確認してください。')
        else:
            st.error('データファイルのインポートに失敗しました。詳細はコンソールログを確認してください。')
    if st.session_state.data_loaded:
        st.info(f"最終更新: {st.session_state.last_update_time}")
        
        excel_data = to_excel(st.session_state.df)
        st.download_button(
            label="📥 Excel形式で全件ダウンロード",
            data=excel_data,
            file_name=f"production_plan_{pd.Timestamp.now().strftime('%Y%m%d')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

# --- メインコンテンツ ---
if st.session_state.data_loaded:
    tab1, tab2, tab3, tab4 = st.tabs(["生産計画一覧", "遵守率ダッシュボード", "詳細分析", "ZP51N遅延一覧"])

    with tab1:
        st.header("📊 生産計画一覧")
        df_display = st.session_state.df.copy()

        # 日付カラムの表示形式をYYYY-MM-DDに統一
        date_columns = ['子指図計画開始日', '子指図計画終了日', '所要日', '基準所要日', '完了日', '基準計画終了日']
        for col in date_columns:
            if col in df_display.columns:
                df_display[col] = pd.to_datetime(df_display[col], errors='coerce').dt.strftime('%Y-%m-%d').replace({pd.NaT: None})

        # 乖離日数を整数に変換
        if '所要日乖離日数' in df_display.columns:
            df_display['所要日乖離日数'] = pd.to_numeric(df_display['所要日乖離日数'], errors='coerce').fillna(0).astype(int)

        # --- フィルター機能 ---
        with st.expander("絞り込みフィルター", expanded=True):
            col1, col2, col3, col4, col5 = st.columns(5)
            
            with col1:
                # 基準所要日フィルター
                if '基準所要日' in df_display.columns:
                    df_display['基準所要日_dt'] = pd.to_datetime(df_display['基準所要日'], errors='coerce')
                    min_date = df_display['基準所要日_dt'].min()
                    max_date = df_display['基準所要日_dt'].max()
                    if pd.notna(min_date) and pd.notna(max_date):
                        date_range = st.date_input("基準所要日の範囲", value=(min_date, max_date), min_value=min_date, max_value=max_date, format="YYYY/MM/DD")
                        if len(date_range) == 2:
                            df_display = df_display[df_display['基準所要日_dt'].between(pd.to_datetime(date_range[0]), pd.to_datetime(date_range[1]))]
                    else:
                        st.info("基準所要日データがありません。")  # ← この行が抜けていました

            with col2:
                # 子指図計画終了日フィルター
                if '子指図計画終了日' in df_display.columns:
                    df_display['子指図計画終了日_dt'] = pd.to_datetime(df_display['子指図計画終了日'], errors='coerce')
                    min_p_date = df_display['子指図計画終了日_dt'].min()
                    max_p_date = df_display['子指図計画終了日_dt'].max()
                    if pd.notna(min_p_date) and pd.notna(max_p_date):
                        p_date_range = st.date_input("子指図計画終了日の範囲", value=(min_p_date, max_p_date), min_value=min_p_date, max_value=max_p_date, format="YYYY/MM/DD")
                        if len(p_date_range) == 2:
                            df_display = df_display[df_display['子指図計画終了日_dt'].between(pd.to_datetime(p_date_range[0]), pd.to_datetime(p_date_range[1]))]
                    else:
                        st.info("計画終了日データがありません。")  # ← この行も同様

            with col3:
                # 生産タイプフィルター
                prod_types = sorted(df_display['生産タイプ'].dropna().unique())
                selected_types = st.multiselect("生産タイプ", options=prod_types, default=prod_types)
                df_display = df_display[df_display['生産タイプ'].isin(selected_types)]

            with col4:
                # 遵守状況フィルター
                compliance_status = sorted(df_display['遵守状況'].dropna().unique())
                selected_compliance = st.multiselect("遵守状況", options=compliance_status, default=compliance_status)
                df_display = df_display[df_display['遵守状況'].isin(selected_compliance)]
            
            with col5:
                # 先行生産フィルター
                early_prod_options = [True, False]
                selected_early_prod = st.multiselect("先行生産", options=early_prod_options, default=early_prod_options)
                df_display = df_display[df_display['先行生産'].isin(selected_early_prod)]

        # 表示するカラムの順番を定義
        display_columns = [
            'No', '親指図番号', '親品目コード', '親品目テキスト', '子指図番号', '子品目コード',
            '子品目テキスト', '所要日', '基準所要日', '子指図計画開始日', '子指図計画終了日', '計画数量',
            '進捗', '完了日', '基準計画終了日', '子MRP管理者', '生産タイプ', '遵守状況', '先行生産', '所要日乖離日数'
        ]
        # 存在しないカラムを除外
        display_columns = [col for col in display_columns if col in df_display.columns]

        st.dataframe(
            df_display[display_columns],
            height=600,
            use_container_width=True,
            hide_index=True
        )
        st.info(f"全 {len(df_display)} 件中 {len(df_display[df_display['遵守状況'] == '遵守'])} 件を表示しています。")

    with tab2:
        display_compliance_dashboard(st.session_state.df)

    with tab3:
        st.header("📊 詳細分析")
        st.subheader("計画終了日と基準所要日の乖離日数分布")
        if not st.session_state.df.empty and '所要日乖離日数' in st.session_state.df.columns:
            fig = px.histogram(st.session_state.df, x='所要日乖離日数', nbins=50, title='所要日乖離日数分布')
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("乖離日数データがありません。")

    with tab4:
        st.header("📋 ZP51N遅延一覧")
        try:
            conn = get_db_connection()
            # テーブルが存在するか確認
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='zp51n_delayed'")
            table_exists = cursor.fetchone()
            
            if table_exists:
                df_zp51n_raw = pd.read_sql_query("SELECT * FROM zp51n_delayed", conn)
                if not df_zp51n_raw.empty:
                    # 日付カラムのフォーマットを統一
                    date_columns_zp51n = ['基準所要日', '所要日']
                    for col in date_columns_zp51n:
                        if col in df_zp51n_raw.columns:
                            df_zp51n_raw[col] = pd.to_datetime(df_zp51n_raw[col], errors='coerce')
                            df_zp51n_raw[col] = df_zp51n_raw[col].dt.strftime('%Y-%m-%d')
                            df_zp51n_raw[col] = df_zp51n_raw[col].replace({'NaT': None, 'None': None, 'nan': None})
                    
                    # 乖離日数を整数に変換
                    if '乖離日数' in df_zp51n_raw.columns:
                        df_zp51n_raw['乖離日数'] = pd.to_numeric(df_zp51n_raw['乖離日数'], errors='coerce').fillna(0).astype(int)
                    
                    st.dataframe(df_zp51n_raw, use_container_width=True)
                    st.info(f"全 {len(df_zp51n_raw)} 件の遅延データを表示しています。")
                else:
                    st.info("遅延データがありません。")
            else:
                st.info("ZP51N遅延データがまだ登録されていません。")
            conn.close()
        except Exception as e:
            st.error(f"ZP51N遅延データの読み込みに失敗しました: {e}")

# --- データ未読み込み時の表示 ---
else:
    st.info("👈 サイドバーから「データ更新」ボタンをクリックして、最新のデータを読み込んでください。")