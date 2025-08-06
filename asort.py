import csv
import json
import os

BATCH_DIR = 'batches'
OUTPUT_CSV = 'input.csv'
def merge_batches():
    """すべてのバッチをマージして単一のCSVを作成"""
    files = sorted([os.path.join(BATCH_DIR, f) for f in os.listdir(BATCH_DIR) if f.startswith('batch_')])
    with open(OUTPUT_CSV, 'w', newline='') as out:
        writer = csv.writer(out)
        for fn in files:
            with open(fn) as bf:
                for row in csv.reader(bf):
                    writer.writerow(row)
    print(f'Merged {len(files)} into {OUTPUT_CSV}')

def read_csv(file_path):
    """CSVファイルを読み込み、各行を辞書形式で返す"""
    with open(file_path, encoding='utf-8') as f:
        reader = csv.reader(f)
        return [row for row in reader]
def assort(data):
    # [postal_code, address, lng, lat] の形式を
    # [lat, lng, postal_code, address] の形式でデータを整形にして
    # postal_codeをキーにしてソートする
    def format_row(row):
        postal_code, address, lng, lat = row
        if not lat or not lng:
            return None
        return [float(lat), float(lng), postal_code, address]
    return sorted(
        (row for row in (format_row(row) for row in data) if row is not None),
        key=lambda x: x[2]
    )
if __name__ == "__main__":
    output_json = 'postal_codes.json'
    merge_batches()
    data = read_csv(OUTPUT_CSV)
    sorted_data = assort(data)
    with open(output_json, 'w', encoding='utf-8') as f:
        json.dump(sorted_data, f, ensure_ascii=False, indent=4)
    