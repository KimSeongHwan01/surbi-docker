-- scripts/init.sql
-- PostgreSQL + PostGIS 초기화 스크립트
-- Docker 최초 기동 시 자동 실행됨

-- PostGIS 확장 활성화
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 공간 참조 시스템 확인 (EPSG:4326 = WGS84 / GeoAlchemy2 기본값)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM spatial_ref_sys WHERE srid = 4326
  ) THEN
    RAISE EXCEPTION 'EPSG:4326 not found in spatial_ref_sys. PostGIS may not be installed correctly.';
  END IF;
END
$$;

-- ============================================================
-- Surbi 테이블 생성 DDL
-- 실제 API 검증 기반 최종본 (2026-06-22)
-- 실행 순서: 반드시 위에서 아래로 (FK 참조 순서)
-- ============================================================


-- ── 1. districts ────────────────────────────────────────────
-- 행정동 기준 허브 테이블. 모든 테이블의 연결 기준점.
-- 출처: 소상공인시장진흥공단 상가(상권)정보 API (adongCd 추출)
CREATE TABLE IF NOT EXISTS districts (
    id              SERIAL          PRIMARY KEY,
    district_code   VARCHAR(10)     NOT NULL UNIQUE,  -- 행정동 코드 8자리 (adongCd)
    district_name   VARCHAR(30)     NOT NULL,           -- 행정동명 (adongNm)
    gu_code         VARCHAR(10)     NOT NULL,           -- 시군구 코드 (signguCd)
    gu              VARCHAR(30)     NOT NULL,           -- 구명 (signguNm). rent_stats JOIN 기준
    si_code         VARCHAR(10)     NOT NULL,           -- 시도 코드 (ctprvnCd). 항상 '11'
    si              VARCHAR(30)     NOT NULL,           -- 시도명 (ctprvnNm). 항상 '서울특별시'
    geom            GEOMETRY,                           -- MVP 제외. 히트맵 기능 추가 시 SHP 적재
    created_at      TIMESTAMP       NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE districts IS '행정동 기준 허브 테이블. 소상공인 API 25개 구 반복 호출로 약 424개 행정동 적재.';
COMMENT ON COLUMN districts.district_code IS '서울시/소상공인 API 기준 행정동 코드 8자리 (adongCd / ADSTRD_CD)';
COMMENT ON COLUMN districts.geom IS 'MVP 제외. PostGIS GEOMETRY. 히트맵 기능 추가 시 국토교통부 SHP 파일로 적재.';


-- ── 2. businesses ───────────────────────────────────────────
-- 업소 정보. 소상공인 API 39개 컬럼 전체 적재.
-- 출처: 소상공인시장진흥공단 상가(상권)정보 API
CREATE TABLE IF NOT EXISTS businesses (
    id                      SERIAL          PRIMARY KEY,
    district_id             INT             NOT NULL REFERENCES districts(id),
    business_code           VARCHAR(30)     NOT NULL UNIQUE,  -- 상가업소번호 (bizesId)
    biz_name                    VARCHAR(100)    NOT NULL,          -- 상호명 (bizesNm)
    branch_name             VARCHAR(100),                      -- 지점명 (brchNm). 빈값→NULL
    category_large_code     VARCHAR(10),                       -- 상권업종대분류코드 (indsLclsCd)
    category_large_name     VARCHAR(30),                       -- 상권업종대분류명 (indsLclsNm)
    category_medium_code    VARCHAR(20),                       -- 상권업종중분류코드 (indsMclsCd)
    category_medium_name    VARCHAR(50),                       -- 상권업종중분류명 (indsMclsNm)
    category_code           VARCHAR(20),                       -- 상권업종소분류코드 (indsSclsCd)
    category_name           VARCHAR(50),                       -- 상권업종소분류명 (indsSclsNm)
    ksic_code               VARCHAR(20),                       -- 표준산업분류코드 (ksicCd)
    ksic_name               VARCHAR(50),                       -- 표준산업분류명 (ksicNm)
    legal_dong_code         VARCHAR(20),                       -- 법정동코드 (ldongCd)
    legal_dong_name         VARCHAR(30),                       -- 법정동명 (ldongNm)
    pnu_code                VARCHAR(30),                       -- PNU코드 (lnoCd)
    plot_sct_code           VARCHAR(5),                        -- 대지구분코드 (plotSctCd). 1=대지,2=산
    plot_sct_name           VARCHAR(20),                       -- 대지구분명 (plotSctNm)
    land_main_no            INT,                               -- 지번본번지 (lnoMnno)
    land_sub_no             VARCHAR(10),                       -- 지번부번지 (lnoSlno). 빈값→NULL
    land_address            VARCHAR(200),                      -- 지번주소 (lnoAdr)
    road_name_code          VARCHAR(20),                       -- 도로명코드 (rdnmCd)
    road_name               VARCHAR(100),                      -- 도로명 (rdnm)
    bld_main_no             INT,                               -- 건물본번지 (bldMnno)
    bld_sub_no              VARCHAR(10),                       -- 건물부번지 (bldSlno). 빈값→NULL
    building_mgmt_no        VARCHAR(30),                       -- 건물관리번호 (bldMngNo)
    building_name           VARCHAR(100),                      -- 건물명 (bldNm). 빈값→NULL
    road_address            VARCHAR(200),                      -- 도로명주소 (rdnmAdr). 메인 주소
    old_zipcode             VARCHAR(10),                       -- 구우편번호 (oldZipcd)
    new_zipcode             VARCHAR(10),                       -- 신우편번호 (newZipcd)
    dong_no                 VARCHAR(20),                       -- 동정보 (dongNo). 빈값→NULL
    floor_no                VARCHAR(10),                       -- 층정보 (flrNo). B1 등 문자 가능
    unit_no                 VARCHAR(20),                       -- 호정보 (hoNo). 빈값→NULL
    lng                     DECIMAL(17,14),                    -- 경도 (lon). API명 lon→lng 변환
    lat                     DECIMAL(17,14),                    -- 위도 (lat). 좌표 없는 업소→NULL
    open_status             VARCHAR(10)     NOT NULL DEFAULT '영업중',  -- API 미제공. 월별 수집 시 폐업 UPDATE
    created_at              TIMESTAMP       NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMP       NOT NULL DEFAULT NOW()   -- 월별 수집 시 갱신. 폐업 추적용
);

COMMENT ON TABLE businesses IS '소상공인 상가(상권)정보 API. 39개 컬럼 전체 적재. 월별 수집 시 사라진 open_status=폐업 UPDATE.';
COMMENT ON COLUMN businesses.open_status IS '기본값 영업중. 월별 수집 시 미조회 업소→폐업 UPDATE.';
COMMENT ON COLUMN businesses.lng IS 'API 응답 컬럼명 lon → DB 저장명 lng. 파이프라인에서 변환.';


-- ── 3. populations ──────────────────────────────────────────
-- 서울시 생활인구(내국인). 가로형(Wide) 구조.
-- 출처: 서울 열린데이터광장 SPOP_LOCAL_RESD_DONG
-- 모든 수치값이 문자열로 내려옴 → float() 변환 후 DECIMAL 저장
CREATE TABLE IF NOT EXISTS populations (
    id              SERIAL          PRIMARY KEY,
    district_id     INT             NOT NULL REFERENCES districts(id),
    std_date        DATE            NOT NULL,  -- 기준일자 (STDR_DE_ID). 20260610→2026-06-10
    time_zone       VARCHAR(5)      NOT NULL,  -- 시간대구분 (TMZON_PD_SE). 00=해당 시간대
    total_pop       DECIMAL(12,4),             -- 총생활인구수 (TOT_LVPOP_CO)
    -- 남성 연령대별 (5세 단위, 70세 이상이 최고령 — API 확인됨)
    male_0t9        DECIMAL(10,4),  -- MALE_F0T9_LVPOP_CO
    male_10t14      DECIMAL(10,4),  -- MALE_F10T14_LVPOP_CO
    male_15t19      DECIMAL(10,4),  -- MALE_F15T19_LVPOP_CO
    male_20t24      DECIMAL(10,4),  -- MALE_F20T24_LVPOP_CO
    male_25t29      DECIMAL(10,4),  -- MALE_F25T29_LVPOP_CO
    male_30t34      DECIMAL(10,4),  -- MALE_F30T34_LVPOP_CO
    male_35t39      DECIMAL(10,4),  -- MALE_F35T39_LVPOP_CO
    male_40t44      DECIMAL(10,4),  -- MALE_F40T44_LVPOP_CO
    male_45t49      DECIMAL(10,4),  -- MALE_F45T49_LVPOP_CO
    male_50t54      DECIMAL(10,4),  -- MALE_F50T54_LVPOP_CO
    male_55t59      DECIMAL(10,4),  -- MALE_F55T59_LVPOP_CO
    male_60t64      DECIMAL(10,4),  -- MALE_F60T64_LVPOP_CO
    male_65t69      DECIMAL(10,4),  -- MALE_F65T69_LVPOP_CO
    male_70o        DECIMAL(10,4),  -- MALE_F70T74_LVPOP_CO (70세 이상, 최고령 구간)
    -- 여성 연령대별 (5세 단위, 70세 이상이 최고령 — API 확인됨)
    female_0t9      DECIMAL(10,4),  -- FEMALE_F0T9_LVPOP_CO
    female_10t14    DECIMAL(10,4),  -- FEMALE_F10T14_LVPOP_CO
    female_15t19    DECIMAL(10,4),  -- FEMALE_F15T19_LVPOP_CO
    female_20t24    DECIMAL(10,4),  -- FEMALE_F20T24_LVPOP_CO
    female_25t29    DECIMAL(10,4),  -- FEMALE_F25T29_LVPOP_CO
    female_30t34    DECIMAL(10,4),  -- FEMALE_F30T34_LVPOP_CO
    female_35t39    DECIMAL(10,4),  -- FEMALE_F35T39_LVPOP_CO
    female_40t44    DECIMAL(10,4),  -- FEMALE_F40T44_LVPOP_CO
    female_45t49    DECIMAL(10,4),  -- FEMALE_F45T49_LVPOP_CO
    female_50t54    DECIMAL(10,4),  -- FEMALE_F50T54_LVPOP_CO
    female_55t59    DECIMAL(10,4),  -- FEMALE_F55T59_LVPOP_CO
    female_60t64    DECIMAL(10,4),  -- FEMALE_F60T64_LVPOP_CO
    female_65t69    DECIMAL(10,4),  -- FEMALE_F65T69_LVPOP_CO
    female_70o      DECIMAL(10,4),  -- FEMALE_F70T74_LVPOP_CO (70세 이상, 최고령 구간)
    recorded_at     TIMESTAMP       NOT NULL DEFAULT NOW(),
    UNIQUE (district_id, std_date, time_zone)
);

COMMENT ON TABLE populations IS '서울시 생활인구(내국인) API. 가로형(Wide) 구조. 75세 이상 컬럼 없음 API 확인됨.';
COMMENT ON COLUMN populations.time_zone IS 'TMZON_PD_SE. 팀원 확인: 00=해당 시간대 값.';


-- ── 4. sales_stats ──────────────────────────────────────────
-- 서울시 추정매출(행정동). API 응답 순서 기준으로 컬럼 정렬.
-- 출처: 서울 열린데이터광장 VwsmAdstrdSelngW
-- ⚠️ 모든 금액/건수 컬럼: API float로 내려옴 → 파이프라인에서 int() 캐스팅 후 BIGINT 저장
CREATE TABLE IF NOT EXISTS sales_stats (
    id                      SERIAL          PRIMARY KEY,
    district_id             INT             NOT NULL REFERENCES districts(id),
    period_code             VARCHAR(10)     NOT NULL,  -- 기준년분기 (STDR_YYQU_CD). 예: 20211
    category_code           VARCHAR(20)     NOT NULL,  -- 서비스업종코드 (SVC_INDUTY_CD). UNIQUE 기준키
    category                VARCHAR(50)     NOT NULL,  -- 서비스업종명 (SVC_INDUTY_CD_NM)
    -- 매출 금액 (API 응답 순서: 당월→주중/주말→요일→시간대→성별→연령대)
    monthly_sales           BIGINT,   -- 당월매출금액 (THSMON_SELNG_AMT)
    monthly_sales_count     BIGINT,   -- 당월매출건수 (THSMON_SELNG_CO)
    weekday_sales           BIGINT,   -- 주중매출금액 (MDWK_SELNG_AMT)
    weekend_sales           BIGINT,   -- 주말매출금액 (WKEND_SELNG_AMT)
    mon_sales               BIGINT,   -- 월요일매출금액 (MON_SELNG_AMT)
    tue_sales               BIGINT,   -- 화요일매출금액 (TUES_SELNG_AMT)
    wed_sales               BIGINT,   -- 수요일매출금액 (WED_SELNG_AMT)
    thu_sales               BIGINT,   -- 목요일매출금액 (THUR_SELNG_AMT)
    fri_sales               BIGINT,   -- 금요일매출금액 (FRI_SELNG_AMT)
    sat_sales               BIGINT,   -- 토요일매출금액 (SAT_SELNG_AMT)
    sun_sales               BIGINT,   -- 일요일매출금액 (SUN_SELNG_AMT)
    tmzon_00_06_sales       BIGINT,   -- 00~06시매출금액 (TMZON_00_06_SELNG_AMT)
    tmzon_06_11_sales       BIGINT,   -- 06~11시매출금액 (TMZON_06_11_SELNG_AMT)
    tmzon_11_14_sales       BIGINT,   -- 11~14시매출금액 (TMZON_11_14_SELNG_AMT)
    tmzon_14_17_sales       BIGINT,   -- 14~17시매출금액 (TMZON_14_17_SELNG_AMT)
    tmzon_17_21_sales       BIGINT,   -- 17~21시매출금액 (TMZON_17_21_SELNG_AMT)
    tmzon_21_24_sales       BIGINT,   -- 21~24시매출금액 (TMZON_21_24_SELNG_AMT)
    male_sales              BIGINT,   -- 남성매출금액 (ML_SELNG_AMT)
    female_sales            BIGINT,   -- 여성매출금액 (FML_SELNG_AMT)
    age10_sales             BIGINT,   -- 10대매출금액 (AGRDE_10_SELNG_AMT)
    age20_sales             BIGINT,   -- 20대매출금액 (AGRDE_20_SELNG_AMT)
    age30_sales             BIGINT,   -- 30대매출금액 (AGRDE_30_SELNG_AMT)
    age40_sales             BIGINT,   -- 40대매출금액 (AGRDE_40_SELNG_AMT)
    age50_sales             BIGINT,   -- 50대매출금액 (AGRDE_50_SELNG_AMT)
    age60_sales             BIGINT,   -- 60대이상매출금액 (AGRDE_60_ABOVE_SELNG_AMT)
    -- 매출 건수 (API 응답 순서: 주중/주말→요일→시간대→성별→연령대)
    weekday_sales_count     BIGINT,   -- 주중매출건수 (MDWK_SELNG_CO)
    weekend_sales_count     BIGINT,   -- 주말매출건수 (WKEND_SELNG_CO)
    mon_sales_count         BIGINT,   -- 월요일매출건수 (MON_SELNG_CO)
    tue_sales_count         BIGINT,   -- 화요일매출건수 (TUES_SELNG_CO)
    wed_sales_count         BIGINT,   -- 수요일매출건수 (WED_SELNG_CO)
    thu_sales_count         BIGINT,   -- 목요일매출건수 (THUR_SELNG_CO)
    fri_sales_count         BIGINT,   -- 금요일매출건수 (FRI_SELNG_CO)
    sat_sales_count         BIGINT,   -- 토요일매출건수 (SAT_SELNG_CO)
    sun_sales_count         BIGINT,   -- 일요일매출건수 (SUN_SELNG_CO)
    tmzon_00_06_count       BIGINT,   -- 00~06시매출건수 (TMZON_00_06_SELNG_CO)
    tmzon_06_11_count       BIGINT,   -- 06~11시매출건수 (TMZON_06_11_SELNG_CO)
    tmzon_11_14_count       BIGINT,   -- 11~14시매출건수 (TMZON_11_14_SELNG_CO)
    tmzon_14_17_count       BIGINT,   -- 14~17시매출건수 (TMZON_14_17_SELNG_CO)
    tmzon_17_21_count       BIGINT,   -- 17~21시매출건수 (TMZON_17_21_SELNG_CO)
    tmzon_21_24_count       BIGINT,   -- 21~24시매출건수 (TMZON_21_24_SELNG_CO)
    male_sales_count        BIGINT,   -- 남성매출건수 (ML_SELNG_CO)
    female_sales_count      BIGINT,   -- 여성매출건수 (FML_SELNG_CO)
    age10_sales_count       BIGINT,   -- 10대매출건수 (AGRDE_10_SELNG_CO)
    age20_sales_count       BIGINT,   -- 20대매출건수 (AGRDE_20_SELNG_CO)
    age30_sales_count       BIGINT,   -- 30대매출건수 (AGRDE_30_SELNG_CO)
    age40_sales_count       BIGINT,   -- 40대매출건수 (AGRDE_40_SELNG_CO)
    age50_sales_count       BIGINT,   -- 50대매출건수 (AGRDE_50_SELNG_CO)
    age60_sales_count       BIGINT,   -- 60대이상매출건수 (AGRDE_60_ABOVE_SELNG_CO)
    recorded_at             TIMESTAMP NOT NULL DEFAULT NOW(),
    UNIQUE (district_id, category_code, period_code)
);

COMMENT ON TABLE sales_stats IS '서울시 추정매출(행정동) API VwsmAdstrdSelngW. 금액/건수 컬럼 API float→int 캐스팅 필수.';
COMMENT ON COLUMN sales_stats.category_code IS 'SVC_INDUTY_CD. UNIQUE 기준키 — 업종명 변경에 안전.';


-- ── 5. market_trends ────────────────────────────────────────
-- 서울시 상권변화지표(행정동).
-- 출처: 서울 열린데이터광장 VwsmAdstrdIxQq
-- ⚠️ 서비스명 주의: VwsmAdstrdIxQq (기존 VwsmAdstrdIdisCnsmp 아님 — 팀원 확인)
CREATE TABLE IF NOT EXISTS market_trends (
    id                      SERIAL          PRIMARY KEY,
    district_id             INT             NOT NULL REFERENCES districts(id),
    period_code             VARCHAR(10)     NOT NULL,  -- 기준년분기 (STDR_YYQU_CD)
    trend_grade             VARCHAR(10),               -- 상권변화지표 (TRDAR_CHNGE_IX). HH/HL/LH/LL
    trend_grade_nm          VARCHAR(10),               -- 상권변화지표명 (TRDAR_CHNGE_IX_NM)
    opr_sale_mt_avrg        INT,                       -- 행정동 평균 영업기간 개월 (OPR_SALE_MT_AVRG)
    cls_sale_mt_avrg        INT,                       -- 행정동 평균 폐업기간 개월 (CLS_SALE_MT_AVRG)
    su_opr_sale_mt_avrg     INT,                       -- 서울시 전체 평균 영업기간 (SU_OPR_SALE_MT_AVRG)
    su_cls_sale_mt_avrg     INT,                       -- 서울시 전체 평균 폐업기간 (SU_CLS_SALE_MT_AVRG)
    recorded_at             TIMESTAMP       NOT NULL DEFAULT NOW(),
    UNIQUE (district_id, period_code)
);

COMMENT ON TABLE market_trends IS '서울시 상권변화지표(행정동) API. 서비스명: VwsmAdstrdIxQq. HH=정체 HL=상권축소 LH=상권확장 LL=다이나믹.';


-- ── 6. subway_stats ─────────────────────────────────────────
-- 서울시 지하철 호선별 역별 승하차 인원.
-- 출처: 서울 열린데이터광장 CardSubwayStatsNew
-- MVP 후순위 — populations로 교통 접근성 대체 가능
-- district_id: NULL 허용 — 역 좌표 확보 후 PostGIS ST_Within() 매핑 예정
CREATE TABLE IF NOT EXISTS subway_stats (
    id              SERIAL          PRIMARY KEY,
    district_id     INT             REFERENCES districts(id),  -- NULL 허용
    line_name       VARCHAR(20)     NOT NULL,  -- 호선명 (SBWY_ROUT_LN_NM)
    station_name    VARCHAR(50)     NOT NULL,  -- 역명 (SBWY_STNS_NM)
    lat             DECIMAL(14,10),            -- 역 위도. 카카오 Local API 별도 수집
    lng             DECIMAL(14,10),            -- 역 경도. 카카오 Local API 별도 수집
    use_date        DATE            NOT NULL,  -- 실제 이용일자 (USE_YMD). REG_YMD 아님
    boarding        INT,                       -- 승차 인원 (GTON_TNOPE)
    alighting       INT,                       -- 하차 인원 (GTOFF_TNOPE)
    recorded_at     TIMESTAMP       NOT NULL DEFAULT NOW(),
    UNIQUE (line_name, station_name, use_date)
);

COMMENT ON TABLE subway_stats IS 'MVP 후순위. district_id NULL 허용 — 역 좌표 확보 후 PostGIS ST_Within()으로 행정동 매핑 예정.';


-- ── 7. rent_stats ───────────────────────────────────────────
-- 한국부동산원 소규모상가 분기별 임대료.
-- 출처: 공공데이터포털 CSV 파일 (분기 수동 다운로드)
-- 독립 테이블 — 행정동 직접 매핑 불가. gu 컬럼으로 간접 연결.
CREATE TABLE IF NOT EXISTS rent_stats (
    id                  SERIAL          PRIMARY KEY,
    rent_area_name      VARCHAR(100)    NOT NULL,  -- 한국부동산원 자체 상권명 (CSV 지역3)
    gu                  VARCHAR(50),               -- 자치구명. 상권명→구 수동 매핑 딕셔너리 처리
    period_code         VARCHAR(10)     NOT NULL,  -- 기준 분기. 예: 20261
    avg_rent_per_sqm    DECIMAL(8,2),              -- ㎡당 평균 임대료 (단위: 천원/㎡)
    recorded_at         TIMESTAMP       NOT NULL DEFAULT NOW(),
    UNIQUE (rent_area_name, period_code)
);

COMMENT ON TABLE rent_stats IS '한국부동산원 임대료 CSV. 독립 테이블 — districts FK 없음. gu 컬럼으로 districts.gu JOIN하여 간접 연결.';
COMMENT ON COLUMN rent_stats.gu IS '상권명→자치구 수동 매핑. 파이프라인 코드 딕셔너리에서 처리.';
COMMENT ON COLUMN rent_stats.avg_rent_per_sqm IS '단위: 천원/㎡. 예: 62.9 = 62,900원/㎡.';


-- ── 8. scores ───────────────────────────────────────────────
-- ML 계산 결과 저장 테이블.
-- 출처: ML Microservice (XGBoost / Random Forest / SHAP)
-- score_reason: LLM 창업 보고서 생성 핵심 입력값
CREATE TABLE IF NOT EXISTS scores (
    id                  SERIAL          PRIMARY KEY,
    district_id         INT             NOT NULL REFERENCES districts(id),
    category            VARCHAR(50)     NOT NULL,  -- 업종명
    period_code         VARCHAR(10)     NOT NULL,  -- 계산 기준 분기
    total_score         DECIMAL(5,2),              -- 종합 점수 0~100
    population_score    DECIMAL(5,2),              -- 유동인구 점수 (20점 만점)
    sales_score         DECIMAL(5,2),              -- 소비금액 점수 (20점 만점)
    age_target_score    DECIMAL(5,2),              -- 타겟 연령대 점수 (15점 만점)
    competitor_score    DECIMAL(5,2),              -- 경쟁업소 점수 (15점 만점)
    ml_sales_score      DECIMAL(5,2),              -- AI 예상매출 점수 (15점 만점)
    transport_score     DECIMAL(5,2),              -- 교통 접근성 점수 (10점 만점)
    rent_score          DECIMAL(5,2),              -- 임대 부담도 점수 (5점 만점)
    model_version       VARCHAR(50),               -- ML 모델 버전. 예: v1.0
    score_reason        TEXT,                      -- 점수 산출 근거. LLM 보고서 생성 핵심 입력값
    calculated_at       TIMESTAMP       NOT NULL DEFAULT NOW(),
    UNIQUE (district_id, category, period_code)
);

COMMENT ON TABLE scores IS 'ML Microservice 계산 결과 저장. ML 팀원 협의 필요: 결과 전달 방식 (POST/CSV/직접INSERT).';
COMMENT ON COLUMN scores.score_reason IS 'LLM 창업 보고서 자동 생성 핵심 입력값. 예: 유동인구 높음, 주말 매출 낮음.';


-- ── 9. government_supports ──────────────────────────────────
-- 정부 지원 사업. 독립 테이블 — 행정동과 무관.
-- 출처: 기업마당 지원사업정보 API (bizinfo.go.kr)
CREATE TABLE IF NOT EXISTS government_supports (
    id              SERIAL          PRIMARY KEY,
    pblanc_id       VARCHAR(100)    NOT NULL UNIQUE,  -- 공고ID (pblancId). 중복 방지 기준키
    title           VARCHAR(200)    NOT NULL,           -- 지원사업명 (pblancNm)
    category        VARCHAR(50),                        -- 지원 분야 (pldirSportRealmLclasCodeNm)
    support_type    VARCHAR(50),                        -- 지원 유형 (pldirSportRealmMlsfcCodeNm)
    support_target  VARCHAR(100),                       -- 신청 대상 (trgetNm)
    agency          VARCHAR(100),                       -- 실행기관 (excInsttNm)
    jrsd_instt_nm   VARCHAR(100),                       -- 주관기관 (jrsdInsttNm)
    summary         TEXT,                               -- 사업요약 (bsnsSumryCn). HTML 제거 후 저장
    sprt_start_date DATE,                               -- 신청 시작일. reqstBeginEndDe 앞부분 파싱
    end_date        DATE,                               -- 신청 종료일. reqstBeginEndDe 뒷부분 파싱
    support_url     VARCHAR(500),                       -- 공고 상세 링크 (pblancUrl)
    created_at      TIMESTAMP       NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE government_supports IS '기업마당 지원사업 API. 독립 테이블 — districts FK 없음. 소상공인/창업벤처 필터링 수집 권장.';


-- ── 10. users ───────────────────────────────────────────────
-- 회원 테이블. Firebase Auth 연동.
CREATE TABLE IF NOT EXISTS users (
    id              SERIAL          PRIMARY KEY,
    firebase_uid    VARCHAR(100)    NOT NULL UNIQUE,  -- Firebase Auth UID
    email           VARCHAR(100),                      -- Firebase Auth 이메일
    created_at      TIMESTAMP       NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE users IS 'Firebase Auth 연동 회원. 로그인 성공 시 firebase_uid 백엔드 전달 → 자동 저장.';


-- ── 11. favorites ───────────────────────────────────────────
-- 즐겨찾기. 사용자가 앱에서 버튼 클릭 시 저장.
CREATE TABLE IF NOT EXISTS favorites (
    id              SERIAL          PRIMARY KEY,
    user_id         INT             NOT NULL REFERENCES users(id),
    district_id     INT             NOT NULL REFERENCES districts(id),
    created_at      TIMESTAMP       NOT NULL DEFAULT NOW(),
    UNIQUE (user_id, district_id)  -- 같은 행정동 중복 즐겨찾기 방지
);

COMMENT ON TABLE favorites IS '사용자 즐겨찾기. (user_id, district_id) UNIQUE — 중복 방지.';


-- ============================================================
-- 인덱스 생성 — 자주 쓰는 조회 패턴 최적화
-- ============================================================

-- districts: 구명 조회 (rent_stats JOIN 기준)
CREATE INDEX IF NOT EXISTS idx_districts_gu ON districts(gu);

-- businesses: 행정동별 업종 조회
CREATE INDEX IF NOT EXISTS idx_businesses_district_id ON businesses(district_id);
CREATE INDEX IF NOT EXISTS idx_businesses_category_code ON businesses(category_code);
CREATE INDEX IF NOT EXISTS idx_businesses_category_large_code ON businesses(category_large_code);

-- populations: 날짜 기준 최신 데이터 조회
CREATE INDEX IF NOT EXISTS idx_populations_district_date ON populations(district_id, std_date);

-- sales_stats: 행정동+분기 기준 조회
CREATE INDEX IF NOT EXISTS idx_sales_stats_district_period ON sales_stats(district_id, period_code);

-- market_trends: 행정동+분기 기준 조회
CREATE INDEX IF NOT EXISTS idx_market_trends_district_period ON market_trends(district_id, period_code);

-- scores: 행정동+업종 기준 조회 (핵심 조회 패턴)
CREATE INDEX IF NOT EXISTS idx_scores_district_category ON scores(district_id, category);

-- government_supports: 대상+분야 기준 필터링
CREATE INDEX IF NOT EXISTS idx_gov_supports_target ON government_supports(target);
CREATE INDEX IF NOT EXISTS idx_gov_supports_category ON government_supports(category);
CREATE INDEX IF NOT EXISTS idx_gov_supports_end_date ON government_supports(end_date);

-- rent_stats: 구 기준 임대료 조회
CREATE INDEX IF NOT EXISTS idx_rent_stats_gu ON rent_stats(gu);
