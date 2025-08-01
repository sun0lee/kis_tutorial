
INSERT OR REPLACE INTO RST_FHPIF05030100 (
    raw_api_id, mat_date, option_type, base_date,
    strike_prc, conv_prpr, inst_cd, prpr, prdy_vs, prdy_vs_sign, prdy_ctrt,
    bidp, askp, time_val, idx_sdpr, acml_vol, sell_rem_qty, buy_rem_qty, acml_trade_amt,
    otst_stpl_qty, otst_stpl_qty_chg, delta, gamma, vega, theta, rho, ints_vltl,
    intrinsic_val, disparity, disparity_rate, hist_volatility, thpr, oprc, hgpr, lwpr, mxpr, llam,
    atm_class_name, prev_diff_chg, total_ask_qty, total_bid_qty,
    antc_cnpr, antc_cntg_vs, antc_cntg_vs_sign, antc_cntg_prdy_ctrt,
    inserted_at
)
SELECT
    r.id,
    json_extract(r.param, '$.FID_MTRT_CNT'), 
    'CALL' AS option_type, -- 콜옵션임을 명시
    STRFTIME('%Y-%m-%dT%H:%M:%f', r.created_at) AS base_date, -- 원본 API 호출 시간
    CAST(json_extract(json_each.value, '$.acpr') AS REAL),
    CAST(json_extract(json_each.value, '$.unch_prpr') AS REAL),
    json_extract(json_each.value, '$.optn_shrn_iscd'),
    CAST(json_extract(json_each.value, '$.optn_prpr') AS REAL),
    CAST(json_extract(json_each.value, '$.optn_prdy_vrss') AS REAL),
    json_extract(json_each.value, '$.prdy_vrss_sign'),
    CAST(json_extract(json_each.value, '$.optn_prdy_ctrt') AS REAL),
    CAST(json_extract(json_each.value, '$.optn_bidp') AS REAL),
    CAST(json_extract(json_each.value, '$.optn_askp') AS REAL),
    CAST(json_extract(json_each.value, '$.tmvl_val') AS REAL),
    CAST(json_extract(json_each.value, '$.nmix_sdpr') AS REAL),
    CAST(json_extract(json_each.value, '$.acml_vol') AS INTEGER),
    CAST(json_extract(json_each.value, '$.seln_rsqn') AS INTEGER),
    CAST(json_extract(json_each.value, '$.shnu_rsqn') AS INTEGER),
    CAST(json_extract(json_each.value, '$.acml_tr_pbmn') AS REAL),
    CAST(json_extract(json_each.value, '$.hts_otst_stpl_qty') AS INTEGER),
    CAST(json_extract(json_each.value, '$.otst_stpl_qty_icdc') AS INTEGER),
    CAST(json_extract(json_each.value, '$.delta_val') AS REAL),
    CAST(json_extract(json_each.value, '$.gama') AS REAL),
    CAST(json_extract(json_each.value, '$.vega') AS REAL),
    CAST(json_extract(json_each.value, '$.theta') AS REAL),
    CAST(json_extract(json_each.value, '$.rho') AS REAL),
    CAST(json_extract(json_each.value, '$.hts_ints_vltl') AS REAL),
    CAST(json_extract(json_each.value, '$.invl_val') AS REAL),
    CAST(json_extract(json_each.value, '$.esdg') AS REAL),
    CAST(json_extract(json_each.value, '$.dprt') AS REAL),
    CAST(json_extract(json_each.value, '$.hist_vltl') AS REAL),
    CAST(json_extract(json_each.value, '$.hts_thpr') AS REAL),
    CAST(json_extract(json_each.value, '$.optn_oprc') AS REAL),
    CAST(json_extract(json_each.value, '$.optn_hgpr') AS REAL),
    CAST(json_extract(json_each.value, '$.optn_lwpr') AS REAL),
    CAST(json_extract(json_each.value, '$.optn_mxpr') AS REAL),
    CAST(json_extract(json_each.value, '$.optn_llam') AS REAL),
    json_extract(json_each.value, '$.atm_cls_name'),
    CAST(json_extract(json_each.value, '$.rgbf_vrss_icdc') AS INTEGER),
    CAST(json_extract(json_each.value, '$.total_askp_rsqn') AS INTEGER),
    CAST(json_extract(json_each.value, '$.total_bidp_rsqn') AS INTEGER),
    CAST(json_extract(json_each.value, '$.futs_antc_cnpr') AS REAL),
    CAST(json_extract(json_each.value, '$.futs_antc_cntg_vrss') AS REAL),
    json_extract(json_each.value, '$.antc_cntg_vrss_sign'),
    CAST(json_extract(json_each.value, '$.antc_cntg_prdy_ctrt') AS REAL),
    STRFTIME('%Y-%m-%dT%H:%M:%f', 'now')
FROM
    rst_raw_api AS r,
    json_each(r.data, '$.output1') AS json_each -- 콜옵션은 output1
WHERE
    r.response_type = 'FHPIF05030100'
    AND r.symbol ='ALL'
    AND r.created_at  LIKE '2025-08-01%';
