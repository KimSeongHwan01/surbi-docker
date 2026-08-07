-- scripts/map_queries.sql
-- 지도 화면용 쿼리 참고본
-- app/pipelines/dong_geom.py 로 districts.geom 적재 후 사용
--
-- 라우터 구현 시 app/routers/districts.py, businesses.py 에 옮겨 붙이는 용도.
-- 컬럼명은 실제 스키마(scripts/init.sql) 기준으로 맞춰져 있다.


-- =============================================================
-- 0. 적재 결과 검증
-- =============================================================

-- 경계가 안 채워진 행정동 확인
SELECT count(*) AS total,
       count(geom) AS with_geom,
       count(*) - count(geom) AS missing
FROM districts;

-- 좌표 범위 확인 (좌표계 사고 방지)
-- 서울은 경도 126.7~127.2 / 위도 37.4~37.7 안에 들어와야 한다.
-- 수십만 단위로 나오면 EPSG:5179 가 변환 없이 들어간 것이다.
SELECT ST_XMin(e) AS min_lng, ST_YMin(e) AS min_lat,
       ST_XMax(e) AS max_lng, ST_YMax(e) AS max_lat
FROM (SELECT ST_Extent(geom) AS e FROM districts) t;

-- 업소 좌표가 실제로 해당 행정동 경계 안에 찍히는지 교차 검증
-- 경계안 비율이 크게 낮으면 businesses.district_id 매핑이 잘못된 것이다.
SELECT d.district_name,
       count(*) AS 업소수,
       count(*) FILTER (
           WHERE ST_Contains(
               d.geom,
               ST_SetSRID(ST_MakePoint(b.lng::float8, b.lat::float8), 4326)
           )
       ) AS 경계안
FROM districts d
JOIN businesses b ON b.district_id = d.id
WHERE d.gu = '성동구'
  AND b.lat IS NOT NULL
  AND b.lng IS NOT NULL
GROUP BY d.district_name
ORDER BY d.district_name;


-- =============================================================
-- 1. 면 레이어 — 행정동 경계 + 창업 점수
--    GET /api/map/districts?gu=&category=&period=
-- =============================================================
-- ST_SimplifyPreserveTopology 로 좌표 개수를 줄인다.
-- 0.0001도는 약 10m 수준이라 화면상 차이가 없는데 응답 크기는 크게 준다.
-- 일반 ST_Simplify 를 쓰면 인접 동 사이에 틈이나 겹침이 생겨
-- 지도에 실금처럼 보이므로 PreserveTopology 를 쓴다.
SELECT d.district_code,
       d.district_name,
       s.total_score,
       ST_AsGeoJSON(ST_SimplifyPreserveTopology(d.geom, 0.0001))::json AS geometry
FROM districts d
LEFT JOIN scores s
       ON s.district_id = d.id
      AND s.category = :category
      AND s.period_code = :period_code
WHERE d.gu = :gu
  AND d.geom IS NOT NULL;


-- =============================================================
-- 2. 점 레이어 — 화면 영역(bbox) 안의 업소 마커
--    GET /api/map/businesses?min_lat=&min_lng=&max_lat=&max_lng=&category=&limit=
-- =============================================================
-- 지도 API 는 항상 "지금 보이는 사각형"을 넘겨주므로 반경(r)보다 bbox 가 자연스럽다.
-- limit 는 반드시 건다. 성동구 외식업만 수천 건이라 전부 내리면 앱이 멈춘다.
SELECT b.biz_name,
       b.building_name,
       b.category_code,
       b.category_name,
       b.lat,
       b.lng
FROM businesses b
WHERE b.lat BETWEEN :min_lat AND :max_lat
  AND b.lng BETWEEN :min_lng AND :max_lng
  AND b.category_code LIKE :category_prefix   -- 외식업 전체면 'CS1%'
  AND b.open_status = '영업중'
LIMIT :limit;                                  -- 권장 300

-- 위 쿼리용 인덱스 (PostGIS 없이 B-tree 만으로 충분히 빠르다)
CREATE INDEX IF NOT EXISTS idx_businesses_latlng
    ON businesses (lat, lng);
CREATE INDEX IF NOT EXISTS idx_businesses_category_code
    ON businesses (category_code);


-- =============================================================
-- 3. 좌표 → 행정동 역조회
--    화면 플로우를 '지도 먼저 → 분석'으로 만들 때 사용
-- =============================================================
SELECT d.id, d.district_code, d.district_name, d.gu
FROM districts d
WHERE ST_Contains(d.geom, ST_SetSRID(ST_MakePoint(:lng, :lat), 4326))
LIMIT 1;


-- =============================================================
-- 4. 인근 행정동 비교 (노션 문서 [전체] 7번 대응)
--    기준 동에서 반경 N미터 안의 동을 가까운 순으로
-- =============================================================
-- geography 로 캐스팅하면 미터 단위로 계산된다.
-- geometry 그대로 두면 단위가 '도'라 거리 값이 엉뚱하게 나온다.
WITH base AS (
    SELECT geom FROM districts WHERE district_code = :district_code
)
SELECT d.district_code,
       d.district_name,
       s.total_score,
       round(ST_Distance(d.geom::geography, b.geom::geography)) AS 거리_m
FROM districts d
CROSS JOIN base b
LEFT JOIN scores s
       ON s.district_id = d.id
      AND s.category = :category
      AND s.period_code = :period_code
WHERE ST_DWithin(d.geom::geography, b.geom::geography, :radius_m)  -- 예: 2000
  AND d.district_code <> :district_code
ORDER BY 거리_m
LIMIT 5;


-- =============================================================
-- 5. 폴백 — 경계 대신 동별 대표 좌표
--    geom 적재가 일정상 어려울 때 원 마커로 대체하는 용도
-- =============================================================
ALTER TABLE districts
    ADD COLUMN IF NOT EXISTS center_lat DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS center_lng DOUBLE PRECISION;

-- geom 이 있으면 정확한 대표점을 쓴다.
-- ST_PointOnSurface 는 ST_Centroid 와 달리 결과가 반드시 폴리곤 내부에 찍힌다.
-- 동 모양이 U자로 휘어 있으면 centroid 는 경계 바깥으로 튀어나간다.
UPDATE districts
SET center_lng = ST_X(ST_PointOnSurface(geom)),
    center_lat = ST_Y(ST_PointOnSurface(geom))
WHERE geom IS NOT NULL;

-- geom 이 아예 없을 때는 업소 좌표 평균으로 근사한다.
UPDATE districts d
SET center_lat = s.lat,
    center_lng = s.lng
FROM (
    SELECT district_id,
           avg(lat) AS lat,
           avg(lng) AS lng
    FROM businesses
    WHERE lat IS NOT NULL AND lng IS NOT NULL
    GROUP BY district_id
) s
WHERE d.id = s.district_id
  AND d.geom IS NULL;
