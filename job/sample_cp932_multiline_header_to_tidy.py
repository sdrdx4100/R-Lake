"""
R-Lake Python Job Sample: CP932 + 前置メタ行 + 複数ヘッダ風CSV → 整形ロング形式

想定データ例:
- 先頭にメタ情報行（文字化け含む）がある
- 次の数行が見出しっぽいが不定形
- 実データは 1列目が yyyy/m/d、以降は 3列ごとのグループが繰り返される
  例) 日付, G1値, G1補助1, G1補助2, G2値, G2補助1, G2補助2, G3値, G3補助1, G3補助2, ...

処理:
1) 文字コードを自動推定（優先: cp932）し、テキスト化
2) 先頭から日付行(yyyy/m/d)が現れるまでスキップ
3) 最初のデータ行から列数を決定し、ヘッダを合成
4) 3列ごとにグループ化し、各グループの「1列目」を値列として抽出
5) ロング形式(date, series, value)に変換して出力

パラメータ(ctx.parameters):
- encoding: 明示的に指定したい文字コード（未指定なら自動推定→cp932優先）
- sep: 区切り文字（既定は ','）
- date_format: 日付の明示フォーマット（例 '%Y/%m/%d'）。未指定時は自動推論
- group_size: グループサイズ（既定3）。m=(列数-1)が割り切れない場合は melt にフォールバック
- series_prefix: series名の接頭辞（既定 's'）
"""
from __future__ import annotations
from typing import Any, Iterable, List
import os
import re
import io
import csv

import pandas as pd

try:
    import chardet  # type: ignore
except Exception:  # pragma: no cover
    chardet = None  # type: ignore


def _detect_encoding(data: bytes) -> str:
    # cp932系を優先的に試す
    candidates: List[str] = []
    if chardet is not None:
        res = chardet.detect(data)
        enc = (res.get('encoding') or '').lower()
        conf = float(res.get('confidence') or 0)
        if enc:
            candidates.append(enc)
        # 日本語っぽい場合はcp932も候補に
        candidates.extend(['cp932', 'shift_jis', 'utf-8-sig', 'utf-8'])
    else:
        candidates = ['cp932', 'shift_jis', 'utf-8-sig', 'utf-8']

    for enc in candidates:
        try:
            data.decode(enc)
            return enc
        except Exception:
            continue
    return 'utf-8'


def process(ctx) -> str | None | pd.DataFrame | Iterable[dict[str, Any]]:
    ctx.logger('starting cp932 multiline header tidy job')

    params = {
        'encoding': None,
        'sep': ',',
        'date_format': None,
        'group_size': 3,
        'series_prefix': 's',
    }
    if isinstance(ctx.parameters, dict):
        params.update(ctx.parameters)

    # 読み込み（バイト→エンコード判定→テキスト）
    with open(ctx.input_path, 'rb') as f:
        raw = f.read()
    encoding = params['encoding'] or _detect_encoding(raw)
    text = raw.decode(encoding, errors='replace')
    lines = text.splitlines()

    # 日付行の開始位置を検出
    date_re = re.compile(r'^\s*\d{4}/\d{1,2}/\d{1,2}\b')
    start_idx = None
    for i, line in enumerate(lines):
        if date_re.search(line):
            start_idx = i
            break
    if start_idx is None:
        raise ValueError('データ開始行を検出できませんでした（yyyy/m/d パターン未検出）')

    data_lines = [ln for ln in lines[start_idx:] if ln.strip()]
    if not data_lines:
        raise ValueError('データ行が空です')

    # 区切り推定（簡易）
    sep = params['sep'] or ','

    # 最初のデータ行から列数を推定
    first = data_lines[0]
    # CSVとして安全に分割
    reader = csv.reader([first])
    first_fields = next(reader)
    n_fields = len(first_fields)
    if n_fields < 2:
        raise ValueError('列数が不足しています')

    # 合成ヘッダ
    names = ['date'] + [f'v{i}' for i in range(1, n_fields)]

    # ヘッダ+データで再構築
    buf = io.StringIO()
    buf.write(','.join(names) + '\n')
    # すべてをそのまま追記（列数がズレる行はスキップ）
    valid = 0
    for ln in data_lines:
        row = next(csv.reader([ln]))
        if len(row) == n_fields:
            buf.write(ln + '\n')
            valid += 1
    ctx.logger(f'kept {valid} rows (from {len(data_lines)})')
    buf.seek(0)

    # DataFrame化
    df = pd.read_csv(buf, parse_dates=['date'], dayfirst=False, infer_datetime_format=False)
    if params['date_format']:
        df['date'] = pd.to_datetime(df['date'], format=params['date_format'], errors='coerce')

    # グループ化してロング形式へ
    value_cols = names[1:]
    m = len(value_cols)
    gsize = int(params.get('group_size') or 3)

    if gsize > 0 and m % gsize == 0:
        groups = m // gsize
        parts = []
        for gi in range(groups):
            # 各グループの1列目を値とみなす
            val_col = value_cols[gi * gsize + 0]
            part = df[['date', val_col]].copy()
            part.rename(columns={val_col: 'value'}, inplace=True)
            part['series'] = f"{params['series_prefix']}{gi+1}"
            parts.append(part)
        out = pd.concat(parts, ignore_index=True)
    else:
        # gsizeで割り切れない場合は縦持ちにして全列を series として扱う
        out = df.melt(id_vars=['date'], value_vars=value_cols, var_name='series', value_name='value')

    # 数値化
    out['value'] = pd.to_numeric(out['value'], errors='coerce')
    out = out.dropna(subset=['date']).reset_index(drop=True)

    # 出力
    out_path = ctx.make_output_path('output.csv')
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    out[['date', 'series', 'value']].to_csv(out_path, index=False)
    ctx.logger(f'wrote {out_path} rows={len(out)}')

    return None
