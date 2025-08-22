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
    T1.raw_api_id, T1.mat_date, T1.option_type, T1.base_date,
    T1.strike_prc, T1.conv_prpr, T1.inst_cd, T1.prpr, T1.prdy_vs, T1.prdy_vs_sign, T1.prdy_ctrt,
    T1.bidp, T1.askp, T1.time_val, T1.idx_sdpr, T1.acml_vol, T1.sell_rem_qty, T1.buy_rem_qty, T1.acml_trade_amt,
    T1.otst_stpl_qty, T1.otst_stpl_qty_chg, T1.delta, T1.gamma, T1.vega, T1.theta, T1.rho, T1.ints_vltl,
    T1.intrinsic_val, T1.disparity, T1.disparity_rate, T1.hist_volatility, T1.thpr, T1.oprc, T1.hgpr, T1.lwpr, T1.mxpr, T1.llam,
    T1.atm_class_name, T1.prev_diff_chg, T1.total_ask_qty, T1.total_bid_qty,
    T1.antc_cnpr, T1.antc_cntg_vs, T1.antc_cntg_vs_sign, T1.antc_cntg_prdy_ctrt,
    T1.inserted_at
FROM (
    SELECT
        r.id AS raw_api_id,
        json_extract(r.param, '$.FID_MTRT_CNT') AS mat_date,
        'CALL' AS option_type,
        STRFTIME('%Y-%m-%dT%H:%M:%f', r.created_at) AS base_date,
        CAST(json_extract(json_each.value, '$.acpr') AS REAL) AS strike_prc,
        CAST(json_extract(json_each.value, '$.unch_prpr') AS REAL) AS conv_prpr,
        json_extract(json_each.value, '$.optn_shrn_iscd') AS inst_cd,
        CAST(json_extract(json_each.value, '$.optn_prpr') AS REAL) AS prpr,
        CAST(json_extract(json_each.value, '$.optn_prdy_vrss') AS REAL) AS prdy_vs,
        json_extract(json_each.value, '$.prdy_vrss_sign') AS prdy_vs_sign,
        CAST(json_extract(json_each.value, '$.optn_prdy_ctrt') AS REAL) AS prdy_ctrt,
        CAST(json_extract(json_each.value, '$.optn_bidp') AS REAL) AS bidp,
        CAST(json_extract(json_each.value, '$.optn_askp') AS REAL) AS askp,
        CAST(json_extract(json_each.value, '$.tmvl_val') AS REAL) AS time_val,
        CAST(json_extract(json_each.value, '$.nmix_sdpr') AS REAL) AS idx_sdpr,
        CAST(json_extract(json_each.value, '$.acml_vol') AS INTEGER) AS acml_vol,
        CAST(json_extract(json_each.value, '$.seln_rsqn') AS INTEGER) AS sell_rem_qty,
        CAST(json_extract(json_each.value, '$.shnu_rsqn') AS INTEGER) AS buy_rem_qty,
        CAST(json_extract(json_each.value, '$.acml_tr_pbmn') AS REAL) AS acml_trade_amt,
        CAST(json_extract(json_each.value, '$.hts_otst_stpl_qty') AS INTEGER) AS otst_stpl_qty,
        CAST(json_extract(json_each.value, '$.otst_stpl_qty_icdc') AS INTEGER) AS otst_stpl_qty_chg,
        CAST(json_extract(json_each.value, '$.delta_val') AS REAL) AS delta,
        CAST(json_extract(json_each.value, '$.gama') AS REAL) AS gamma,
        CAST(json_extract(json_each.value, '$.vega') AS REAL) AS vega,
        CAST(json_extract(json_each.value, '$.theta') AS REAL) AS theta,
        CAST(json_extract(json_each.value, '$.rho') AS REAL) AS rho,
        CAST(json_extract(json_each.value, '$.hts_ints_vltl') AS REAL) AS ints_vltl,
        CAST(json_extract(json_each.value, '$.invl_val') AS REAL) AS intrinsic_val,
        CAST(json_extract(json_each.value, '$.esdg') AS REAL) AS disparity,
        CAST(json_extract(json_each.value, '$.dprt') AS REAL) AS disparity_rate,
        CAST(json_extract(json_each.value, '$.hist_vltl') AS REAL) AS hist_volatility,
        CAST(json_extract(json_each.value, '$.hts_thpr') AS REAL) AS thpr,
        CAST(json_extract(json_each.value, '$.optn_oprc') AS REAL) AS oprc,
        CAST(json_extract(json_each.value, '$.optn_hgpr') AS REAL) AS hgpr,
        CAST(json_extract(json_each.value, '$.optn_lwpr') AS REAL) AS lwpr,
        CAST(json_extract(json_each.value, '$.optn_mxpr') AS REAL) AS mxpr,
        CAST(json_extract(json_each.value, '$.optn_llam') AS REAL) AS llam,
        json_extract(json_each.value, '$.atm_cls_name') AS atm_class_name,
        CAST(json_extract(json_each.value, '$.rgbf_vrss_icdc') AS INTEGER) AS prev_diff_chg,
        CAST(json_extract(json_each.value, '$.total_askp_rsqn') AS INTEGER) AS total_ask_qty,
        CAST(json_extract(json_each.value, '$.total_bidp_rsqn') AS INTEGER) AS total_bid_qty,
        CAST(json_extract(json_each.value, '$.futs_antc_cnpr') AS REAL) AS antc_cnpr,
        CAST(json_extract(json_each.value, '$.futs_antc_cntg_vrss') AS REAL) AS antc_cntg_vs,
        json_extract(json_each.value, '$.antc_cntg_vrss_sign') AS antc_cntg_vs_sign,
        CAST(json_extract(json_each.value, '$.antc_cntg_prdy_ctrt') AS REAL) AS antc_cntg_prdy_ctrt,
        STRFTIME('%Y-%m-%dT%H:%M:%f', 'now') AS inserted_at
    FROM
        rst_raw_api AS r,
        json_each(r.data, '$.output1') AS json_each
    WHERE
        r.response_type = 'FHPIF05030100'
        AND r.symbol = 'ALL'
        and r.created_at like '2025-08-2%'
) AS T1
LEFT JOIN RST_FHPIF05030100 AS T2
    ON T1.inst_cd = T2.inst_cd
    AND T1.base_date = T2.base_date
WHERE
    T2.inst_cd IS NULL;