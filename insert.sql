SELECT * FROM mst_raw_inst
WHERE info_type  IN ('6')--,'5')
AND KOR_NAME LIKE '%202509%'
/*종목 정보 타입 코드: 
 * 1:지수선물, 
 * 2:지수SP, 
 * 3:스타선물, 
 * 4:스타SP, 
 * 5:지수콜옵션, 
 * 6:지수풋옵션, 
 * 7:변동성선물, 
 * 8:변동성SP, 
 * 9:섹터선물, 
 * A:섹터SP, 
 * B:미니선물, 
 * C:미니SP, 
 * D:미니콜옵션, 
 * E:미니풋옵션, 
 * J:코스닥150콜옵션, 
 * K:코스닥150풋옵션, 
 * L:위클리콜옵션, 
 * M:위클리풋옵션
 */
;

SELECT * 
FROM map_mrktdiv_infotype
;
SELECT * 
FROM mst_inst
WHERE use_yn ='Y'
;
SELECT * FROM column_comments
ORDER BY column_order

;

SELECT *   FROM rst_raw_api
--WHERE  rowid < '53'
--WHERE symbol  like '%01W09412'
--AND response_type ='FHPIF05030000'
--AND response_type ='FHPIF05030000'


;

INSERT INTO RST_FHMIF10000000_01 (
    raw_api_id,
    req_cond_mrkt_div_code, req_input_iscd,
    base_date, -- rawdata call 시점 컬럼 추가
    hts_kor_isnm, futs_prpr, futs_prdy_vrss, prdy_vrss_sign, futs_prdy_clpr,
    futs_prdy_ctrt, acml_vol, acml_tr_pbmn, hts_otst_stpl_qty, otst_stpl_qty_icdc,
    futs_oprc, futs_hgpr, futs_lwpr, futs_mxpr, futs_llam, basis, futs_sdpr,
    hts_thpr, dprt, crbr_aply_mxpr, crbr_aply_llam, futs_last_tr_date, hts_rmnn_dynu,
    futs_lstn_medm_hgpr, futs_lstn_medm_lwpr, delta_val, gama, theta, vega, rho,
    hist_vltl, hts_ints_vltl, mrkt_basis, acpr, inserted_at
)
SELECT
    r.id AS raw_api_id,
    json_extract(r.param, '$.FID_COND_MRKT_DIV_CODE'),
    json_extract(r.param, '$.FID_INPUT_ISCD'),
    r.created_at AS base_date, -- rst_raw_api.created_at 값 사용
    json_extract(r.data, '$.output1.hts_kor_isnm'),
    CAST(json_extract(r.data, '$.output1.futs_prpr') AS REAL),
    CAST(json_extract(r.data, '$.output1.futs_prdy_vrss') AS REAL),
    json_extract(r.data, '$.output1.prdy_vrss_sign'),
    CAST(json_extract(r.data, '$.output1.futs_prdy_clpr') AS REAL),
    CAST(json_extract(r.data, '$.output1.futs_prdy_ctrt') AS REAL),
    CAST(json_extract(r.data, '$.output1.acml_vol') AS INTEGER),
    CAST(json_extract(r.data, '$.output1.acml_tr_pbmn') AS INTEGER),
    CAST(json_extract(r.data, '$.output1.hts_otst_stpl_qty') AS INTEGER),
    CAST(json_extract(r.data, '$.output1.otst_stpl_qty_icdc') AS INTEGER),
    CAST(json_extract(r.data, '$.output1.futs_oprc') AS REAL),
    CAST(json_extract(r.data, '$.output1.futs_hgpr') AS REAL),
    CAST(json_extract(r.data, '$.output1.futs_lwpr') AS REAL),
    CAST(json_extract(r.data, '$.output1.futs_mxpr') AS REAL),
    CAST(json_extract(r.data, '$.output1.futs_llam') AS REAL),
    CAST(json_extract(r.data, '$.output1.basis') AS REAL),
    CAST(json_extract(r.data, '$.output1.futs_sdpr') AS REAL),
    CAST(json_extract(r.data, '$.output1.hts_thpr') AS REAL),
    CAST(json_extract(r.data, '$.output1.dprt') AS REAL),
    CAST(json_extract(r.data, '$.output1.crbr_aply_mxpr') AS REAL),
    CAST(json_extract(r.data, '$.output1.crbr_aply_llam') AS REAL),
    json_extract(r.data, '$.output1.futs_last_tr_date'),
    CAST(json_extract(r.data, '$.output1.hts_rmnn_dynu') AS INTEGER),
    CAST(json_extract(r.data, '$.output1.futs_lstn_medm_hgpr') AS REAL),
    CAST(json_extract(r.data, '$.output1.futs_lstn_medm_lwpr') AS REAL),
    CAST(json_extract(r.data, '$.output1.delta_val') AS REAL),
    CAST(json_extract(r.data, '$.output1.gama') AS REAL),
    CAST(json_extract(r.data, '$.output1.theta') AS REAL),
    CAST(json_extract(r.data, '$.output1.vega') AS REAL),
    CAST(json_extract(r.data, '$.output1.rho') AS REAL),
    CAST(json_extract(r.data, '$.output1.hist_vltl') AS REAL),
    CAST(json_extract(r.data, '$.output1.hts_ints_vltl') AS REAL),
    CAST(json_extract(r.data, '$.output1.mrkt_basis') AS REAL),
    CAST(json_extract(r.data, '$.output1.acpr') AS REAL),
    STRFTIME('%Y-%m-%dT%H:%M:%f', 'now')
FROM
    rst_raw_api AS r
WHERE
    r.response_type = 'FHMIF10000000' AND json_extract(r.data, '$.output1') IS NOT NULL;



;


INSERT INTO RST_FHMIF10000000_02 (
    raw_api_id,
    req_cond_mrkt_div_code, req_input_iscd,
    base_date, -- rawdata call 시점 컬럼 추가
    bstp_cls_code, hts_kor_isnm, bstp_nmix_prpr, prdy_vrss_sign,
    bstp_nmix_prdy_vrss, bstp_nmix_prdy_ctrt, inserted_at
)SELECT
    r.id AS raw_api_id,
    json_extract(r.param, '$.FID_COND_MRKT_DIV_CODE'),
    json_extract(r.param, '$.FID_INPUT_ISCD'),
    r.created_at AS base_date, -- rst_raw_api.created_at 값 사용
    json_extract(r.data, '$.output2.bstp_cls_code'),
    json_extract(r.data, '$.output2.hts_kor_isnm'),
    CAST(json_extract(r.data, '$.output2.bstp_nmix_prpr') AS REAL),
    json_extract(r.data, '$.output2.prdy_vrss_sign'),
    CAST(json_extract(r.data, '$.output2.bstp_nmix_prdy_vrss') AS REAL),
    CAST(json_extract(r.data, '$.output2.bstp_nmix_prdy_ctrt') AS REAL),
    STRFTIME('%Y-%m-%dT%H:%M:%f', 'now')
FROM
    rst_raw_api AS r
WHERE
    r.response_type = 'FHMIF10000000' AND json_extract(r.data, '$.output2') IS NOT NULL;



;


INSERT INTO RST_FHMIF10000000_03 (
    raw_api_id,
    req_cond_mrkt_div_code, req_input_iscd,
    base_date, -- rawdata call 시점 컬럼 추가
    bstp_cls_code, hts_kor_isnm, bstp_nmix_prpr, prdy_vrss_sign,
    bstp_nmix_prdy_vrss, bstp_nmix_prdy_ctrt, inserted_at
)
SELECT
    r.id AS raw_api_id,
    json_extract(r.param, '$.FID_COND_MRKT_DIV_CODE'),
    json_extract(r.param, '$.FID_INPUT_ISCD'),
    r.created_at AS base_date, -- rst_raw_api.created_at 값 사용
    json_extract(r.data, '$.output3.bstp_cls_code'),
    json_extract(r.data, '$.output3.hts_kor_isnm'),
    CAST(json_extract(r.data, '$.output3.bstp_nmix_prpr') AS REAL),
    json_extract(r.data, '$.output3.prdy_vrss_sign'),
    CAST(json_extract(r.data, '$.output3.bstp_nmix_prdy_vrss') AS REAL),
    CAST(json_extract(r.data, '$.output3.bstp_nmix_prdy_ctrt') AS REAL),
    STRFTIME('%Y-%m-%dT%H:%M:%f', 'now')
FROM
    rst_raw_api AS r
WHERE
    r.response_type = 'FHMIF10000000' AND json_extract(r.data, '$.output3') IS NOT NULL;



INSERT INTO RST_FHKIF03020100_01 (
    raw_api_id,
    req_cond_mrkt_div_code, req_input_iscd, req_input_date_1, req_input_date_2, req_period_div_code,
    base_date,
    futs_prdy_vrss, prdy_vrss_sign, futs_prdy_ctrt, futs_prdy_clpr,
    acml_vol, acml_tr_pbmn, hts_kor_isnm, futs_prpr, futs_shrn_iscd, prdy_vol,
    futs_mxpr, futs_llam, futs_oprc, futs_hgpr, futs_lwpr,
    futs_prdy_oprc, futs_prdy_hgpr, futs_prdy_lwpr, futs_askp, futs_bidp,
    basis, kospi200_nmix, kospi200_prdy_vrss, kospi200_prdy_ctrt, kospi200_prdy_vrss_sign,
    hts_otst_stpl_qty, otst_stpl_qty_icdc, tday_rltv, hts_thpr, dprt,
    inserted_at
)
SELECT
    r.id,
    json_extract(r.param, '$.FID_COND_MRKT_DIV_CODE'),
    json_extract(r.param, '$.FID_INPUT_ISCD'),
    json_extract(r.param, '$.FID_INPUT_DATE_1'),
    json_extract(r.param, '$.FID_INPUT_DATE_2'),
    json_extract(r.param, '$.FID_PERIOD_DIV_CODE'), -- 예시 param에 존재하여 추가
    r.created_at,
    CAST(json_extract(r.data, '$.output1.futs_prdy_vrss') AS REAL),
    json_extract(r.data, '$.output1.prdy_vrss_sign'),
    CAST(json_extract(r.data, '$.output1.futs_prdy_ctrt') AS REAL),
    CAST(json_extract(r.data, '$.output1.futs_prdy_clpr') AS REAL),
    CAST(json_extract(r.data, '$.output1.acml_vol') AS INTEGER),
    CAST(json_extract(r.data, '$.output1.acml_tr_pbmn') AS INTEGER),
    json_extract(r.data, '$.output1.hts_kor_isnm'),
    CAST(json_extract(r.data, '$.output1.futs_prpr') AS REAL),
    json_extract(r.data, '$.output1.futs_shrn_iscd'),
    CAST(json_extract(r.data, '$.output1.prdy_vol') AS INTEGER),
    CAST(json_extract(r.data, '$.output1.futs_mxpr') AS REAL),
    CAST(json_extract(r.data, '$.output1.futs_llam') AS REAL),
    CAST(json_extract(r.data, '$.output1.futs_oprc') AS REAL),
    CAST(json_extract(r.data, '$.output1.futs_hgpr') AS REAL),
    CAST(json_extract(r.data, '$.output1.futs_lwpr') AS REAL),
    CAST(json_extract(r.data, '$.output1.futs_prdy_oprc') AS REAL),
    CAST(json_extract(r.data, '$.output1.futs_prdy_hgpr') AS REAL),
    CAST(json_extract(r.data, '$.output1.futs_prdy_lwpr') AS REAL),
    CAST(json_extract(r.data, '$.output1.futs_askp') AS REAL),
    CAST(json_extract(r.data, '$.output1.futs_bidp') AS REAL),
    CAST(json_extract(r.data, '$.output1.basis') AS REAL),
    CAST(json_extract(r.data, '$.output1.kospi200_nmix') AS REAL),
    CAST(json_extract(r.data, '$.output1.kospi200_prdy_vrss') AS REAL),
    CAST(json_extract(r.data, '$.output1.kospi200_prdy_ctrt') AS REAL),
    json_extract(r.data, '$.output1.kospi200_prdy_vrss_sign'),
    CAST(json_extract(r.data, '$.output1.hts_otst_stpl_qty') AS INTEGER),
    CAST(json_extract(r.data, '$.output1.otst_stpl_qty_icdc') AS INTEGER),
    CAST(json_extract(r.data, '$.output1.tday_rltv') AS REAL),
    CAST(json_extract(r.data, '$.output1.hts_thpr') AS REAL),
    CAST(json_extract(r.data, '$.output1.dprt') AS REAL),
    STRFTIME('%Y-%m-%dT%H:%M:%f', 'now')
FROM
    rst_raw_api AS r
WHERE
    r.response_type = 'FHKIF03020100' AND json_extract(r.data, '$.output1') IS NOT NULL;





INSERT OR REPLACE INTO RST_FHKIF03020100_02 (
    raw_api_id,
    req_cond_mrkt_div_code, req_input_iscd, req_period_div_code, -- 조회 시작/종료일자 제거
    base_date,
    stck_bsop_date, futs_prpr, futs_oprc, futs_hgpr, futs_lwpr,
    acml_vol, acml_tr_pbmn, mod_yn,
    inserted_at
)
SELECT
    r.id,
    json_extract(r.param, '$.FID_COND_MRKT_DIV_CODE'),
    json_extract(r.param, '$.FID_INPUT_ISCD'),
    json_extract(r.param, '$.FID_PERIOD_DIV_CODE'),
    r.created_at,
    json_extract(json_each.value, '$.stck_bsop_date'),
    CAST(json_extract(json_each.value, '$.futs_prpr') AS REAL),
    CAST(json_extract(json_each.value, '$.futs_oprc') AS REAL),
    CAST(json_extract(json_each.value, '$.futs_hgpr') AS REAL),
    CAST(json_extract(json_each.value, '$.futs_lwpr') AS REAL),
    CAST(json_extract(json_each.value, '$.acml_vol') AS INTEGER),
    CAST(json_extract(json_each.value, '$.acml_tr_pbmn') AS INTEGER),
    json_extract(json_each.value, '$.mod_yn'),
    STRFTIME('%Y-%m-%dT%H:%M:%f', 'now')
FROM
    rst_raw_api AS r,
    json_each(r.data, '$.output2') AS json_each
WHERE
    r.response_type = 'FHKIF03020100' AND json_type(r.data, '$.output2') = 'array';


;

INSERT INTO RST_FHKIF03020200_01 (
    raw_api_id,
    req_cond_mrkt_div_code, req_input_iscd, req_hour_cls_code, req_pw_data_incu_yn, req_fake_tick_incu_yn, req_input_date_1, req_input_hour_1,
    base_date,
    futs_prdy_vrss, prdy_vrss_sign, futs_prdy_ctrt, futs_prdy_clpr, prdy_nmix,
    acml_vol, acml_tr_pbmn, hts_kor_isnm, futs_prpr, futs_shrn_iscd, prdy_vol,
    futs_mxpr, futs_llam, futs_oprc, futs_hgpr, futs_lwpr,
    futs_prdy_oprc, futs_prdy_hgpr, futs_prdy_lwpr, futs_askp, futs_bidp,
    basis, kospi200_nmix, kospi200_prdy_vrss, kospi200_prdy_ctrt, kospi200_prdy_vrss_sign,
    hts_otst_stpl_qty, otst_stpl_qty_icdc, tday_rltv, hts_thpr, dprt,
    inserted_at
)
SELECT
    r.id,
    json_extract(r.param, '$.FID_COND_MRKT_DIV_CODE'),
    json_extract(r.param, '$.FID_INPUT_ISCD'),
    json_extract(r.param, '$.FID_HOUR_CLS_CODE'),
    json_extract(r.param, '$.FID_PW_DATA_INCU_YN'),
    json_extract(r.param, '$.FID_FAKE_TICK_INCU_YN'),
    json_extract(r.param, '$.FID_INPUT_DATE_1'),
    json_extract(r.param, '$.FID_INPUT_HOUR_1'),
    r.created_at,
    CAST(json_extract(r.data, '$.output1.futs_prdy_vrss') AS REAL),
    json_extract(r.data, '$.output1.prdy_vrss_sign'),
    CAST(json_extract(r.data, '$.output1.futs_prdy_ctrt') AS REAL),
    CAST(json_extract(r.data, '$.output1.futs_prdy_clpr') AS REAL),
    CAST(json_extract(r.data, '$.output1.prdy_nmix') AS REAL),
    CAST(json_extract(r.data, '$.output1.acml_vol') AS INTEGER),
    CAST(json_extract(r.data, '$.output1.acml_tr_pbmn') AS INTEGER),
    json_extract(r.data, '$.output1.hts_kor_isnm'),
    CAST(json_extract(r.data, '$.output1.futs_prpr') AS REAL),
    json_extract(r.data, '$.output1.futs_shrn_iscd'),
    CAST(json_extract(r.data, '$.output1.prdy_vol') AS INTEGER),
    CAST(json_extract(r.data, '$.output1.futs_mxpr') AS REAL),
    CAST(json_extract(r.data, '$.output1.futs_llam') AS REAL),
    CAST(json_extract(r.data, '$.output1.futs_oprc') AS REAL),
    CAST(json_extract(r.data, '$.output1.futs_hgpr') AS REAL),
    CAST(json_extract(r.data, '$.output1.futs_lwpr') AS REAL),
    CAST(json_extract(r.data, '$.output1.futs_prdy_oprc') AS REAL),
    CAST(json_extract(r.data, '$.output1.futs_prdy_hgpr') AS REAL),
    CAST(json_extract(r.data, '$.output1.futs_prdy_lwpr') AS REAL),
    CAST(json_extract(r.data, '$.output1.futs_askp') AS REAL),
    CAST(json_extract(r.data, '$.output1.futs_bidp') AS REAL),
    CAST(json_extract(r.data, '$.output1.basis') AS REAL),
    CAST(json_extract(r.data, '$.output1.kospi200_nmix') AS REAL),
    CAST(json_extract(r.data, '$.output1.kospi200_prdy_vrss') AS REAL),
    CAST(json_extract(r.data, '$.output1.kospi200_prdy_ctrt') AS REAL),
    json_extract(r.data, '$.output1.kospi200_prdy_vrss_sign'),
    CAST(json_extract(r.data, '$.output1.hts_otst_stpl_qty') AS INTEGER),
    CAST(json_extract(r.data, '$.output1.otst_stpl_qty_icdc') AS INTEGER),
    CAST(json_extract(r.data, '$.output1.tday_rltv') AS REAL),
    CAST(json_extract(r.data, '$.output1.hts_thpr') AS REAL),
    CAST(json_extract(r.data, '$.output1.dprt') AS REAL),
    STRFTIME('%Y-%m-%dT%H:%M:%f', 'now')
FROM
    rst_raw_api AS r
WHERE
    r.response_type = 'FHKIF03020200' AND json_extract(r.data, '$.output1') IS NOT NULL;
;



INSERT OR REPLACE INTO RST_FHKIF03020200_02 (
    raw_api_id,
    req_cond_mrkt_div_code, req_input_iscd, req_hour_cls_code,
    base_date,
    stck_bsop_date, stck_cntg_hour, futs_prpr, futs_oprc, futs_hgpr, futs_lwpr,
    cntg_vol, acml_tr_pbmn,
    inserted_at
)
SELECT
    r.id,
    json_extract(r.param, '$.FID_COND_MRKT_DIV_CODE'),
    json_extract(r.param, '$.FID_INPUT_ISCD'),
    json_extract(r.param, '$.FID_HOUR_CLS_CODE'),
    r.created_at,
    json_extract(json_each.value, '$.stck_bsop_date'),
    json_extract(json_each.value, '$.stck_cntg_hour'),
    CAST(json_extract(json_each.value, '$.futs_prpr') AS REAL),
    CAST(json_extract(json_each.value, '$.futs_oprc') AS REAL),
    CAST(json_extract(json_each.value, '$.futs_hgpr') AS REAL),
    CAST(json_extract(json_each.value, '$.futs_lwpr') AS REAL),
    CAST(json_extract(json_each.value, '$.cntg_vol') AS INTEGER),
    CAST(json_extract(json_each.value, '$.acml_tr_pbmn') AS INTEGER),
    STRFTIME('%Y-%m-%dT%H:%M:%f', 'now')
FROM
    rst_raw_api AS r,
    json_each(r.data, '$.output2') AS json_each
WHERE
    r.response_type = 'FHKIF03020200' AND json_type(r.data, '$.output2') = 'array';
;
INSERT INTO RST_FHPIF05030000_01 (
    raw_api_id,
    req_cond_mrkt_div_code, req_input_iscd, 
    base_date,
    unas_prpr, unas_prdy_vrss, unas_prdy_vrss_sign, unas_prdy_ctrt, unas_acml_vol,
    hts_kor_isnm, futs_prpr, futs_prdy_vrss, prdy_vrss_sign, futs_prdy_ctrt,
    inserted_at
)
SELECT
    r.id,
    json_extract(r.param, '$.FID_COND_MRKT_DIV_CODE'),
    json_extract(r.param, '$.FID_INPUT_ISCD'),
    r.created_at,
    CAST(json_extract(r.data, '$.output1.unas_prpr') AS REAL),
    CAST(json_extract(r.data, '$.output1.unas_prdy_vrss') AS REAL),
    json_extract(r.data, '$.output1.unas_prdy_vrss_sign'),
    CAST(json_extract(r.data, '$.output1.unas_prdy_ctrt') AS REAL),
    CAST(json_extract(r.data, '$.output1.unas_acml_vol') AS INTEGER),
    json_extract(r.data, '$.output1.hts_kor_isnm'),
    CAST(json_extract(r.data, '$.output1.futs_prpr') AS REAL),
    CAST(json_extract(r.data, '$.output1.futs_prdy_vrss') AS REAL),
    json_extract(r.data, '$.output1.prdy_vrss_sign'),
    CAST(json_extract(r.data, '$.output1.futs_prdy_ctrt') AS REAL),
    STRFTIME('%Y-%m-%dT%H:%M:%f', 'now')
FROM
    rst_raw_api AS r
WHERE
    r.response_type = 'FHPIF05030000' AND json_extract(r.data, '$.output1') IS NOT NULL;

SELECT * FROM RST_FHMIF10000000_01
;
SELECT * FROM RST_FHMIF10000000_02
;
SELECT * FROM RST_FHMIF10000000_03

SELECT * FROM RST_FHKIF03020100_01 ;

SELECT * FROM RST_FHKIF03020100_02 ;

SELECT * FROM RST_FHKIF03020200_01;

SELECT * FROM RST_FHKIF03020200_02 ;

SELECT * FROM RST_FHPIF05030000_01 ;


SELECT * FROM meta_table_comments
--WHERE table_name  ='RST_FHPIF05030000_01'
--ORDER BY 1,4
;
