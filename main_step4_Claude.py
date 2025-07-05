import csv
import io
import sys
import os
import logging
from datetime import datetime, timedelta

# ロギング設定
# 実行ファイルと同じディレクトリにログファイルを出力
log_dir = os.path.dirname(sys.argv[0]) if getattr(sys, 'frozen', False) else os.path.dirname(os.path.abspath(__file__))
log_file = os.path.join(log_dir, 'neo_roomcsv_converter.log')

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# CSVデータの入力開始ーーーーーーーーーーーーーーーーーーーーー
# 試すエンコーディングのリスト (優先順位順)
ENCODINGS_TO_TRY = ['utf-8', 'shift_jis', 'cp932']

def fix_time_format(datetime_str):
    """24:00を翌日00:00に変換"""
    try:
        if ' 24:00' in datetime_str:
            date_part = datetime_str.split(' ')[0]
            # 日付を1日進める処理
            date_obj = datetime.strptime(date_part, '%Y/%m/%d')
            next_day = date_obj + timedelta(days=1)
            result = next_day.strftime('%Y/%m/%d 00:00')
            logging.info(f"時刻修正: {datetime_str} → {result}")
            return result
        return datetime_str
    except Exception as e:
        logging.error(f"時刻修正エラー: {datetime_str}, エラー: {e}")
        return datetime_str

def format_for_supabase(datetime_str):
    """YYYY/MM/DD HH:MM形式をISO形式に変換（JST固定）"""
    try:
        # まず24:00の修正を適用
        fixed_datetime = fix_time_format(datetime_str)
        dt = datetime.strptime(fixed_datetime, '%Y/%m/%d %H:%M')
        result = dt.strftime('%Y-%m-%dT%H:%M:00+09:00')  # JST固定
        logging.info(f"SUPABASE形式変換: {datetime_str} → {result}")
        return result
    except Exception as e:
        logging.error(f"SUPABASE形式変換エラー: {datetime_str}, エラー: {e}")
        return datetime_str

def convert_csv(input_file_path, output_file_path):
    """
    会議室予約情報のCSVを変換する

    Args:
        input_file_path (str): 入力CSVファイルのパス (異なるエンコーディングを自動検出)
        output_file_path (str): 出力CSVファイルのパス (UTF-8, SUPABASE用)
    """
    try:
        logging.info(f"変換開始: 入力ファイル='{input_file_path}', 出力ファイル='{output_file_path}'")
        
        csv_c = None
        detected_encoding = None
        for enc in ENCODINGS_TO_TRY:
            try:
                with open(input_file_path, 'r', encoding=enc) as f_in:
                    raw_content = f_in.read()
                    # BOM (Byte Order Mark) を除去する
                    if raw_content.startswith('\ufeff'):
                        raw_content = raw_content.lstrip('\ufeff')
                        logging.info("UTF-8 BOMを除去しました。")
                    csv_c = raw_content
                    detected_encoding = enc
                logging.info(f"ファイルは '{detected_encoding}' エンコーディングで正常に読み込まれました。")
                break # 成功したらループを抜ける
            except UnicodeDecodeError:
                logging.warning(f"ファイルは '{enc}' エンコーディングで読み込めませんでした。次のエンコーディングを試します。")
                continue
            except Exception as e:
                # FileNotFoundErrorなどはメインのtry-exceptで処理されるため、
                # ここではUnicodeDecodeError以外の不明なエラーが発生した場合を想定し、
                # ログに記録し、さらに外側のExceptionブロックでキャッチされるように再raise
                logging.error(f"ファイル読み込み中に予期せぬエラーが発生しました ({enc}): {e}")
                # ここでraiseしないことで、他のエンコーディングも試行し続ける
        
        if csv_c is None:
            msg = f"エラー: 入力ファイル '{input_file_path}' をサポートされているどのエンコーディング ({', '.join(ENCODINGS_TO_TRY)}) でも読み込むことができませんでした。ファイルの形式を確認してください。"
            logging.error(msg)
            print(msg)
            return

        csv_file = io.StringIO(csv_c)
        
        reader = csv.reader(csv_file)
        
        lines = list(reader)
        
        # BOM除去後の処理に移動
        if not lines:
            msg = "エラー: CSVファイルが空です。"
            logging.warning(msg)
            print(msg)
            return

        result_rows = []
        current_kaigishitsu = None
# CSVデータの入力終了ーーーーーーーーーーーーーーーーーーーーー

# CSVデータの変換処理開始ーーーーーーーーーーーーーーーーーーーーー
        # ヘッダー行の処理
        header_row = lines[0]
        
        # 元のヘッダーをインデックス取得用に保持 (データ行処理で元のカラム名からインデックスを取得するため)
        original_header_for_indexing = list(lines[0])

        # SUPABASE用の新しいヘッダーを定義（PostgreSQLのテーブル構造に合わせる）
        new_csv_header = ['room_name', 'start_datetime', 'end_datetime', 'purpose_detail']
        logging.info(f"SUPABASE用CSVヘッダーを定義しました: {new_csv_header}")
        
        result_rows.append(new_csv_header)

        # 'purpose_detail' カラムの最終的なインデックスを特定
        purpose_detail_final_index = new_csv_header.index('purpose_detail')
        logging.info(f"最終ヘッダーで'purpose_detail'カラムをインデックス {purpose_detail_final_index} で検出しました。")

        # データ行の処理
        # 元のヘッダーから必要なカラムのインデックスを取得
        orig_header = lines[0]
        try:
            kaishi_nichi_idx = orig_header.index('開始日')
            kaishi_jikan_idx = orig_header.index('開始時刻')
            shuryo_nichi_idx = orig_header.index('終了日')
            shuryo_jikan_idx = orig_header.index('終了時刻')
            logging.info("開始日、開始時刻、終了日、終了時刻のインデックスを正常に取得しました。")
        except ValueError as e:
            logging.error(f"必須カラムのインデックスが見つかりません。エラー: {e}")
            print(f"エラー: 必須カラムが見つかりません。CSVファイルに '開始日', '開始時刻', '終了日', '終了時刻' が存在することを確認してください。")
            return

        for i, row in enumerate(lines[1:]):
            if not row:
                continue

            # 会議室名の行を判定 (app.jsのロジックを参考)
            # この判定は元の行データ（削除前）に対して行う
            if len(row) > 1 and '会議室' in row[1] and (len(row) <= 2 or not row[2].strip()):
                current_kaigishitsu = row[1].strip()
                logging.info(f"会議室名検出: '{current_kaigishitsu}'")
                continue

            # 予約情報の行に会議室名を追加
            if current_kaigishitsu and len(row) > 2 and row[2].strip():
                # 日時カラムの値を取得
                try:
                    start_datetime_str = f"{row[kaishi_nichi_idx]} {row[kaishi_jikan_idx]}"
                    end_datetime_str = f"{row[shuryo_nichi_idx]} {row[shuryo_jikan_idx]}"
                    
                    # SUPABASE用のISO形式に変換
                    start_datetime_iso = format_for_supabase(start_datetime_str)
                    end_datetime_iso = format_for_supabase(end_datetime_str)
                    
                    logging.info(f"開始日時: {start_datetime_str} → {start_datetime_iso}")
                    logging.info(f"終了日時: {end_datetime_str} → {end_datetime_iso}")

                except IndexError as e:
                    logging.warning(f"日時カラムのデータ取得中にインデックスエラーが発生しました: {e} 行データ: {row}")
                    continue # この行はスキップ

                # processed_rowをSUPABASE用の構造に基づいてゼロから構築
                processed_row = []
                
                # 会議室名の処理（不要な文字列を除去）
                clean_room_name = current_kaigishitsu.replace('仙台合同庁舎', '').replace('／仙台地方振興事務所', '').strip()
                
                # Shift_JISでエンコードできない文字の置換（丸数字など）
                clean_room_name = clean_room_name.replace('①', '1').replace('②', '2').replace('③', '3').replace('④', '4').replace('⑤', '5') \
                                 .replace('⑥', '6').replace('⑦', '7').replace('⑧', '8').replace('⑨', '9').replace('⑩', '10')
                
                # 新しいヘッダーの各要素に対応するデータを追加
                processed_row.append(clean_room_name)        # room_name
                processed_row.append(start_datetime_iso)     # start_datetime
                processed_row.append(end_datetime_iso)       # end_datetime
                processed_row.append('×')                    # purpose_detail（固定値）
                
                logging.info(f"処理済み行データ: {processed_row}")
                result_rows.append(processed_row)

        if len(result_rows) <= 1:
            msg = "エラー: 変換対象の予約データが見つかりませんでした。入力ファイルの形式を確認してください。"
            logging.warning(msg)
            print(msg)
            return
# CSVデータの変換処理終了ーーーーーーーーーーーーーーーーーーーーー

# CSVデータの出力ーーーーーーーーーーーーーーーーーーーーー
        # 変換後のデータをUTF-8で書き出し（SUPABASE用）
        with open(output_file_path, 'w', encoding='utf-8', newline='') as f_out:
            writer = csv.writer(f_out)
            writer.writerows(result_rows)
        
        # 処理結果の統計情報
        total_records = len(result_rows) - 1  # ヘッダー行を除く
        msg = f"変換が完了しました。出力ファイル: {output_file_path}"
        stats_msg = f"処理統計: 変換済みレコード数 = {total_records} 件"
        
        logging.info(msg)
        logging.info(stats_msg)
        print(msg)
        print(stats_msg)
        
        # SUPABASE用の追加情報
        print("\n=== SUPABASE用情報 ===")
        print(f"出力形式: UTF-8 CSV")
        print(f"日時形式: ISO 8601 (JST固定)")
        print(f"テーブル名推奨: meeting_room_reservations")
        print("======================")

    except FileNotFoundError:
        msg = f"エラー: 入力ファイルが見つかりません: {input_file_path}"
        logging.error(msg)
        print(msg)
    except Exception as e:
        msg = f"処理中に予期せぬエラーが発生しました: {e}"
        logging.exception(msg) # 詳細なトレースバックをログに記録
        print(msg)
        print("詳細はログファイル (neo_roomcsv_converter.log) をご確認ください。")

def validate_datetime_format(csv_file_path):
    """出力されたCSVファイルの日時形式を検証"""
    try:
        with open(csv_file_path, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader)
            
            if 'start_datetime' in header and 'end_datetime' in header:
                start_idx = header.index('start_datetime')
                end_idx = header.index('end_datetime')
                
                validation_errors = []
                row_count = 0
                
                for i, row in enumerate(reader, 1):
                    row_count += 1
                    if len(row) > max(start_idx, end_idx):
                        try:
                            # ISO形式の日時が正しくパースできるかチェック
                            start_dt = datetime.fromisoformat(row[start_idx].replace('+09:00', ''))
                            end_dt = datetime.fromisoformat(row[end_idx].replace('+09:00', ''))
                            
                            # 開始時刻が終了時刻より後でないかチェック
                            if start_dt >= end_dt:
                                validation_errors.append(f"行{i}: 開始時刻が終了時刻以降です ({row[start_idx]} >= {row[end_idx]})")
                                
                        except ValueError as e:
                            validation_errors.append(f"行{i}: 日時形式エラー - {e}")
                
                if validation_errors:
                    print(f"\n警告: {len(validation_errors)} 件の日時形式エラーが発見されました:")
                    for error in validation_errors[:5]:  # 最初の5件のみ表示
                        print(f"  - {error}")
                    if len(validation_errors) > 5:
                        print(f"  ... 他 {len(validation_errors) - 5} 件")
                    logging.warning(f"日時形式検証で {len(validation_errors)} 件のエラーが発見されました")
                else:
                    print(f"\n✓ 日時形式検証完了: {row_count} 件すべて正常です")
                    logging.info(f"日時形式検証完了: {row_count} 件すべて正常")
                    
    except Exception as e:
        logging.error(f"日時形式検証エラー: {e}")
        print(f"日時形式検証中にエラーが発生しました: {e}")

if __name__ == '__main__':
    try:
        if len(sys.argv) < 2:
            msg = "エラー: 入力ファイルが指定されていません。ファイルをドラッグ＆ドロップしてください。"
            logging.error(msg)
            print(msg)
            sys.exit(1)

        input_file_path = sys.argv[1]

        if not os.path.exists(input_file_path):
            msg = f"エラー: 指定された入力ファイルが見つかりません: {input_file_path}"
            logging.error(msg)
            print(msg)
            sys.exit(1)

        # 現在の日時を取得
        now = datetime.now()
        timestamp = now.strftime("%Y%m%d%H%M") # 西暦4桁+時間4桁

        # 元のファイル名と拡張子を取得
        base_name = os.path.basename(input_file_path)
        file_name_without_ext, file_ext = os.path.splitext(base_name)

        # 新しいファイル名を生成（SUPABASE用）
        output_base_name = f"{timestamp}_supabase_{file_name_without_ext}{file_ext}"

        # 出力パスを作成（元のファイルと同じディレクトリ）
        output_directory = os.path.dirname(input_file_path)
        output_file_path = os.path.join(output_directory, output_base_name)

        convert_csv(input_file_path, output_file_path)
        
        # 変換後のファイルの日時形式を検証
        if os.path.exists(output_file_path):
            validate_datetime_format(output_file_path)
            
    except Exception as e:
        msg = f"メイン処理中に予期せぬエラーが発生しました: {e}"
        logging.exception(msg)
        print(msg)
        print("詳細はログファイル (neo_roomcsv_converter.log) をご確認ください。")
    finally:
        # エラーメッセージやログを確認できるように、処理後にコンソールを閉じないようにする
        if sys.stdout.isatty(): # ターミナルから実行された場合のみ一時停止
            input("処理が完了しました。何かキーを押すと終了します。")