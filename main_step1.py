
import csv
import io
import sys
import os
import logging
from datetime import datetime

# ロギング設定
# 実行ファイルと同じディレクトリにログファイルを出力
log_dir = os.path.dirname(sys.argv[0]) if getattr(sys, 'frozen', False) else os.path.dirname(os.path.abspath(__file__))
log_file = os.path.join(log_dir, 'neo_roomcsv_converter.log')

logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# 試すエンコーディングのリスト (優先順位順)
ENCODINGS_TO_TRY = ['utf-8', 'shift_jis', 'cp932']

def convert_csv(input_file_path, output_file_path):
    """
    会議室予約情報のCSVを変換する

    Args:
        input_file_path (str): 入力CSVファイルのパス (異なるエンコーディングを自動検出)
        output_file_path (str): 出力CSVファイルのパス (Shift_JIS)
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
                    csv_c = raw_content # .replace('"', '') を削除
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

        # ヘッダー行の処理
        header_row = lines[0]
        header_row.insert(0, '会議室名')
        result_rows.append(header_row)

        # データ行の処理
        for i, row in enumerate(lines[1:]): # i を追加
            if not row:  # 空行をスキップ
                continue

            # 会議室名の行を判定 (app.jsのロジックを参考)
            if len(row) > 1 and '会議室' in row[1] and (len(row) <= 2 or not row[2].strip()):
                current_kaigishitsu = row[1].strip()
                logging.info(f"会議室名検出: '{current_kaigishitsu}'")
                continue

            # 予約情報の行に会議室名を追加
            if current_kaigishitsu and len(row) > 2 and row[2].strip():
                new_row = row[:] # オリジナルを保持するためスライスでコピー
                new_row.insert(0, current_kaigishitsu)
                
                # Shift_JISでエンコードできない文字の置換 (例: 丸数字)
                # 全てのセルに対して置換を適用
                for j in range(len(new_row)):
                    # 丸数字を通常数字に置換
                    new_row[j] = new_row[j].replace('①', '1').replace('②', '2').replace('③', '3').replace('④', '4').replace('⑤', '5') \
                                         .replace('⑥', '6').replace('⑦', '7').replace('⑧', '8').replace('⑨', '9').replace('⑩', '10')

                result_rows.append(new_row)

        if len(result_rows) <= 1:
            msg = "エラー: 変換対象の予約データが見つかりませんでした。入力ファイルの形式を確認してください。"
            logging.warning(msg)
            print(msg)
            return

        # 変換後のデータをShift_JISで書き出し
        # encoding='shift_jis'でエンコードできない文字は'?'に置換 (errors='replace')
        with open(output_file_path, 'w', encoding='shift_jis', errors='replace', newline='') as f_out:
            writer = csv.writer(f_out)
            writer.writerows(result_rows)
        
        msg = f"変換が完了しました。出力ファイル: {output_file_path}"
        logging.info(msg)
        print(msg)

    except FileNotFoundError:
        msg = f"エラー: 入力ファイルが見つかりません: {input_file_path}"
        logging.error(msg)
        print(msg)
    except Exception as e:
        msg = f"処理中に予期せぬエラーが発生しました: {e}"
        logging.exception(msg) # 詳細なトレースバックをログに記録
        print(msg)
        print("詳細はログファイル (neo_roomcsv_converter.log) をご確認ください。")


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

        # 新しいファイル名を生成
        output_base_name = f"{timestamp}_converted_{file_name_without_ext}{file_ext}"

        # 出力パスを作成（元のファイルと同じディレクトリ）
        output_directory = os.path.dirname(input_file_path)
        output_file_path = os.path.join(output_directory, output_base_name)

        convert_csv(input_file_path, output_file_path)
    except Exception as e:
        msg = f"メイン処理中に予期せぬエラーが発生しました: {e}"
        logging.exception(msg)
        print(msg)
        print("詳細はログファイル (neo_roomcsv_converter.log) をご確認ください。")
    finally:
        # エラーメッセージやログを確認できるように、処理後にコンソールを閉じないようにする
        if sys.stdout.isatty(): # ターミナルから実行された場合のみ一時停止
            input("処理が完了しました。何かキーを押すと終了します。")
