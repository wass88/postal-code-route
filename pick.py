import json
import math

# 定数
INPUT_JSON        = 'postal_codes.json'   # 入力：[[lon, lat, code, address], …]
OUTPUT_ALL        = 'all_segments.json'   # 全セグメント出力先
OUTPUT_MAJOR      = 'major_segments.json' # 閾値以上セグメント出力先
LENGTH_THRESHOLD  = 10.0                   # km単位の閾値（例：1km以上を「目立つ線」とみなす）

# 地球上の２点間距離を計算する大円距離（Haversine）  
def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0  # 地球半径 km
    φ1, φ2 = math.radians(lat1), math.radians(lat2)
    Δφ = math.radians(lat2 - lat1)
    Δλ = math.radians(lon2 - lon1)
    a = math.sin(Δφ/2)**2 + math.cos(φ1)*math.cos(φ2)*math.sin(Δλ/2)**2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))

# メイン処理
if __name__ == '__main__':
    # 1. 入力読み込み
    with open(INPUT_JSON, encoding='utf-8') as f:
        points = json.load(f)
        # points: [[lon, lat, code, address], …]

    all_feats = []
    major_feats = []

    # 2. 隣接点間をループ
    for i in range(len(points) - 1):
        lon1, lat1, code1, addr1 = points[i]
        lon2, lat2, code2, addr2 = points[i+1]
        dist_km = haversine(lat1, lon1, lat2, lon2)

        feat = {
            "type": "Feature",
            "geometry": {
                "type": "LineString",
                "coordinates": [
                    [lon1, lat1],
                    [lon2, lat2]
                ]
            },
            "properties": {
                "length_km": round(dist_km, 3),
                "from": code1,
                "to":   code2
            }
        }
        all_feats.append(feat)
        if dist_km >= LENGTH_THRESHOLD:
            major_feats.append(feat)

    # 3. GeoJSONファイル出力
    base = {"type": "FeatureCollection"}
    with open(OUTPUT_ALL,   'w', encoding='utf-8') as f:
        json.dump({**base, "features": all_feats}, f, ensure_ascii=False, indent=2)
    with open(OUTPUT_MAJOR, 'w', encoding='utf-8') as f:
        json.dump({**base, "features": major_feats}, f, ensure_ascii=False, indent=2)

    print(f"全セグメント: {len(all_feats)} 件、" +
          f"目立つセグメント: {len(major_feats)} 件を出力完了")
