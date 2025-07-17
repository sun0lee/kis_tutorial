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