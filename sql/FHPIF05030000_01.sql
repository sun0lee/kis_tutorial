INSERT OR REPLACE INTO RST_FHPIF05030000_01 (
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