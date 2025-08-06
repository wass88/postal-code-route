# File: zip_latlon_mapper.py
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# 依存パッケージ: requests, urllib3==2.5.0, tqdm

import csv
import time
import requests
import os
import json
import sys
from tqdm import tqdm

# 環境変数で設定可能
INPUT_CSV = os.getenv('INPUT_CSV', 'utf_ken_all.csv')
BATCH_DIR = os.getenv('BATCH_DIR', 'batches')
OUTPUT_CSV = os.getenv('OUTPUT_CSV', 'zip_latlon_mapping.csv')
CHECKPOINT_FILE = os.getenv('CHECKPOINT_FILE', 'checkpoint.json')
BATCH_SIZE = int(os.getenv('BATCH_SIZE', '100'))
RATE_LIMIT = float(os.getenv('RATE_LIMIT', '1'))
ENCODING = 'utf-8'
GSI_API_URL = 'https://msearch.gsi.go.jp/address-search/AddressSearch'


def geocode_address(address: str) -> tuple:
    """住所文字列をジオコーディングし、(lat, lon) を返す"""
    try:
        resp = requests.get(GSI_API_URL, params={'q': address}, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        results = data.get('results', []) if isinstance(data, dict) else data
        if results and isinstance(results[0], dict):
            coords = results[0].get('geometry', {}).get('coordinates', [])
            if len(coords) >= 2:
                return coords[1], coords[0]
    except Exception as e:
        print(f"Error geocoding '{address}': {e}", file=sys.stderr)
    return None, None


def parse_row(cols: list) -> list:
    """
    CSVの1レコード（リスト）から [postal_code, address, lat, lon] を生成
    変換元と結果を標準出力に表示
    """
    postal_code = cols[2]
    address = cols[6] + cols[7] + cols[8]
    lat, lon = geocode_address(address)
    lat_str = lat if lat is not None else ''
    lon_str = lon if lon is not None else ''
    result = [postal_code, address, lat_str, lon_str]
        # 変換元と結果を表示
    # フォーマット: 元レコード: [...], 変換結果: [...]
    print(f"元レコード: {cols}  ->  変換結果: {result}")
    return result


def load_checkpoint() -> int:
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, encoding=ENCODING) as f:
            return json.load(f).get('last_line', 0)
    return 0


def save_checkpoint(line_no: int):
    with open(CHECKPOINT_FILE, 'w', encoding=ENCODING) as f:
        json.dump({'last_line': line_no}, f)


def process_file():
    """入力CSVをバッチ単位で処理し、各バッチをファイル出力"""
    os.makedirs(BATCH_DIR, exist_ok=True)
    last = load_checkpoint()
    with open(INPUT_CSV, encoding=ENCODING) as f:
        total = sum(1 for _ in f)
    batch = []
    idx_batch = last // BATCH_SIZE

    with open(INPUT_CSV, encoding=ENCODING) as f:
        reader = csv.reader(f)
        for i, cols in enumerate(tqdm(reader, total=total, desc='Processing'), start=0):
            if i < last:
                continue
            row = parse_row(cols)
            batch.append(row)
            if (i + 1) % BATCH_SIZE == 0:
                fn = os.path.join(BATCH_DIR, f'batch_{idx_batch}.csv')
                with open(fn, 'w', encoding=ENCODING, newline='') as bf:
                    csv.writer(bf).writerows(batch)
                tqdm.write(f'Saved {fn}')
                batch = []
                idx_batch += 1
                save_checkpoint(i + 1)
            time.sleep(RATE_LIMIT)

    if batch:
        fn = os.path.join(BATCH_DIR, f'batch_{idx_batch}.csv')
        with open(fn, 'w', encoding=ENCODING, newline='') as bf:
            csv.writer(bf).writerows(batch)
        tqdm.write(f'Saved {fn}')
    save_checkpoint(total)


def merge_batches():
    """すべてのバッチをマージして単一のCSVを作成"""
    files = sorted([os.path.join(BATCH_DIR, f) for f in os.listdir(BATCH_DIR) if f.startswith('batch_')])
    with open(OUTPUT_CSV, 'w', encoding=ENCODING, newline='') as out:
        writer = csv.writer(out)
        for fn in files:
            with open(fn, encoding=ENCODING) as bf:
                for row in csv.reader(bf):
                    writer.writerow(row)
    print(f'Merged {len(files)} into {OUTPUT_CSV}')


def main():
    process_file()
    if load_checkpoint() >= sum(1 for _ in open(INPUT_CSV, encoding=ENCODING)):
        merge_batches()


if __name__ == '__main__':
    main()
