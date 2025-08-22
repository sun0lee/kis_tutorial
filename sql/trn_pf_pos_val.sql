INSERT INTO trn_pf_pos_val (
    pf_id,
    pos_id,
    inst_cd,
    base_date,
    entry_at,
    pos_type,
    entry_prc,
    cur_prc,
    volume,
    trade_amt,
    strike_prc,
    index_std_prc,
    impl_val,
    time_val,
    theory_prc,
    market_val,
    pnl,
    impl_vol,
    delta,
    gamma,
    vega,
    theta,
    rho,
    evaluated_at
)
SELECT
    T4.pf_id,
    T4.pos_id,
    T4.inst_cd,
    T4.base_date,
    T4.entry_at,
    T4.pos_type,
    T4.signed_entry_prc as entry_prc,
    T4.signed_cur_prc as cur_prc,
    T4.volume,
    T4.trade_amt,
    T4.strike_prc,
    T4.index_std_prc,
    T4.impl_val,
    T4.time_val,
    T4.theory_prc,
    T4.market_val,
    T4.pnl,
    T4.impl_vol,
    T4.delta,
    T4.gamma,
    T4.vega,
    T4.theta,
    T4.rho,
    T4.evaluated_at
FROM (
    SELECT
        pf_id,
        pos_id,
        inst_cd,
        base_date,
        entry_at,
        pos_type,
        qty AS volume,
        (qty * cur_prc) AS trade_amt,
        strike_prc,
        index_std_prc,
        impl_val,
        time_val,
        theory_prc,
        (qty * cur_prc) AS market_val,
        (cur_prc - entry_prc) * qty * pnl_sign AS pnl,
        impl_vol,
        delta * pnl_sign AS delta,
        gamma * pnl_sign AS gamma,
        vega * pnl_sign AS vega,
        theta * pnl_sign AS theta,
        rho * pnl_sign AS rho,
        (qty * cur_prc) * pnl_sign AS signed_cur_prc,
        (qty * entry_prc) * pnl_sign AS signed_entry_prc,
        strftime('%Y-%m-%dT%H:%M:%S', 'now') AS evaluated_at
    FROM (
        SELECT
            T1.pf_id,
            T1.pos_id,
            T2.inst_cd,
            T1.qty,
            T1.entry_prc,
            T1.entry_at,
            T1.pos_type,
            T2.strike_prc,
            T2.index_std_prc,
            T2.impl_val,
            T2.time_val,
            T2.theory_prc,
            T2.impl_vol,
            T2.delta,
            T2.gamma,
            T2.vega,
            T2.theta,
            T2.rho,
            strftime('%Y-%m-%d %H:%M', T2.base_date) AS base_date,
            T2.cur_prc,
            CASE T1.pos_type
                WHEN 'SELL' THEN -1
                WHEN 'BUY' THEN 1
                ELSE 0
            END AS pnl_sign
        FROM mst_pf_pos AS T1
        JOIN trn_inst AS T2
            ON T1.inst_cd = T2.inst_cd
        WHERE
            T1.is_active = 'Y'
            AND T2.base_date > T1.entry_at
    ) AS T3
) AS T4
LEFT JOIN trn_pf_pos_val AS T5
    ON T4.pf_id = T5.pf_id
    AND T4.pos_id = T5.pos_id
    AND T4.base_date = strftime('%Y-%m-%d %H:%M', T5.base_date)
WHERE
    T5.pos_id IS NULL
order by t4.pf_id, t4.base_date, t4.pos_id ;