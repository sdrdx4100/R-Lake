"""
R-Lake Python Job Sample: sample_cleaning.py

機能:
- 入力CSVを読み込み、必要列の空白トリム、列名正規化、任意のフィルタや計算列追加を行う
- 出力CSVを返す（戻り値はパス or DataFrame or None のどれでも可）

必要ライブラリ:
- pandas（インストール済み前提）
"""
from __future__ import annotations
from typing import Any, Iterable
import os
import pandas as pd


def process(ctx) -> str | None | pd.DataFrame | Iterable[dict[str, Any]]:
    """エントリ関数（既定名）

    期待:
    - ctx.input_path: 入力CSVの絶対パス
    - ctx.parameters: 実行パラメータ（例: {"drop_na": true, "min_speed": 5}）
    - ctx.make_output_path(name): 一時出力パス作成

    戻り値:
    - None の場合、ctx.make_output_path('output.csv') に出力したとみなされる
    - str/Path: 生成したCSVのパス
    - DataFrame: 自動でCSV化
    - Iterable[dict]: 自動でCSV化
    """
    ctx.logger("starting sample_cleaning")

    # パラメータの既定
    params = {
        "drop_na": True,
        "min_speed": None,  # 例: 速度列で閾値フィルタ
        "columns": None,    # 例: 出力したい列のリスト
        "lower_columns": True,  # 列名を小文字に
        "strip_whitespace": True,  # 文字列の前後空白をトリム
    }
    if isinstance(ctx.parameters, dict):
        params.update(ctx.parameters)

    # CSV読み込み
    df = pd.read_csv(ctx.input_path)
    ctx.logger(f"loaded input shape={df.shape}")

    # 列名正規化
    if params.get("lower_columns"):
        df.columns = [str(c).strip().lower().replace(" ", "_") for c in df.columns]

    # 文字列列の空白トリム
    if params.get("strip_whitespace"):
        for col in df.select_dtypes(include=["object", "string"]).columns:
            df[col] = df[col].astype("string").str.strip()

    # 任意のフィルタ（例: speed 列が存在し、min_speed 指定があればフィルタ）
    min_speed = params.get("min_speed")
    if min_speed is not None and "speed" in df.columns:
        before = len(df)
        df = df[df["speed"].astype(float) >= float(min_speed)]
        ctx.logger(f"filtered by min_speed>={min_speed}: {before}->{len(df)}")

    # 必要列の選択
    if params.get("columns"):
        cols = [c for c in params["columns"] if c in df.columns]
        if cols:
            df = df[cols]
            ctx.logger(f"selected columns: {cols}")

    # 計算列の例: if both lat & lon present, create latlon string
    if "lat" in df.columns and "lon" in df.columns and "latlon" not in df.columns:
        df["latlon"] = df["lat"].astype(str) + "," + df["lon"].astype(str)

    # 出力
    out_path = ctx.make_output_path("output.csv")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    df.to_csv(out_path, index=False)
    ctx.logger(f"wrote {out_path} shape={df.shape}")

    # None を返すとフレームワーク側で out_path を既定出力として扱う
    return None
