INSERT OR REPLACE INTO RST_FHMIF10000000_02 (
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