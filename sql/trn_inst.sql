INSERT INTO trn_inst (
    inst_cd, base_date,
    strike_prc, index_std_prc,
    cur_prc, open_prc, high_prc, low_prc, bid_prc, ask_prc, volume, trade_amt,
    bid_qty, ask_qty, prdy_vs, prdy_ctrt, impl_vol,
    delta, gamma, vega, theta, rho,
    impl_val, time_val, theory_prc,
    inserted_at
)
SELECT
    T1.inst_cd,
    T1.base_date,
    T1.strike_prc,
    T1.idx_sdpr AS index_std_prc,
    T1.prpr AS cur_prc,
    T1.oprc AS open_prc,
    T1.hgpr AS high_prc,
    T1.lwpr AS low_prc,
    T1.bidp AS bid_prc,
    T1.askp AS ask_prc,
    T1.acml_vol AS volume,
    T1.acml_trade_amt AS trade_amt,
    T1.buy_rem_qty AS bid_qty,
    T1.sell_rem_qty AS ask_qty,
    T1.prdy_vs,
    T1.prdy_ctrt,
    T1.ints_vltl AS impl_vol,
    T1.delta,
    T1.gamma,
    T1.vega,
    T1.theta,
    T1.rho,
    T1.intrinsic_val AS impl_val,
    T1.time_val,
    T1.thpr AS theory_prc,
    strftime('%Y-%m-%dT%H:%M:%S', 'now') AS inserted_at
FROM
    RST_FHPIF05030100 T1
LEFT JOIN
    trn_inst T2 ON T1.inst_cd = T2.inst_cd AND T1.base_date = T2.base_date
WHERE
    T2.inst_cd IS NULL;
