# R-Lake Job Samples (Python)

このフォルダには、R-Lake の Python 前処理ジョブのサンプルが含まれます。

前提
- PreprocessJob(job_type='python') を作成し、script_path か script_file でこの .py を指定
- entry_function は既定で `process` に設定（変更する場合はモデルの entry_function を更新）
- 戻り値は以下いずれか
  - None: ctx.make_output_path('output.csv') へCSVを書き出したとみなす
  - str / pathlib.Path: 生成したCSVのパス
  - pandas.DataFrame: そのままCSV化して取り込み
  - Iterable[dict]: ヘッダを推定してCSV化

コンテキスト `ctx`
- ctx.input_file: RawDataFile インスタンス
- ctx.input_path: 入力CSVの絶対パス
- ctx.parameters: 実行時のパラメータ(dict)。ジョブ既定+実行時がマージ
- ctx.tempdir: 一時ディレクトリのパス
- ctx.make_output_path(name='output.csv'): 一時ディレクトリ配下のパスを返す
- ctx.logger(msg): 実行ログを JobRun.log に追記（簡易）

実行例
- UI のアップロード画面で既存ジョブにこのスクリプトを紐付けて実行
- REST: POST /api/datasets/jobs/{job_id}/run
