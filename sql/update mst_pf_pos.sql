UPDATE mst_pf_pos
SET
    entry_prc = (
        SELECT cur_prc
        FROM trn_inst
        WHERE
            trn_inst.inst_cd = mst_pf_pos.inst_cd
            AND strftime('%Y-%m-%d %H:%M', trn_inst.base_date) = strftime('%Y-%m-%d %H:%M', mst_pf_pos.entry_at)
        LIMIT 1
    ),
    updated_at = strftime('%Y-%m-%dT%H:%M:%S', 'now')
WHERE EXISTS (
    SELECT 1
    FROM trn_inst
    WHERE
        trn_inst.inst_cd = mst_pf_pos.inst_cd
        AND strftime('%Y-%m-%d %H:%M', trn_inst.base_date) = strftime('%Y-%m-%d %H:%M', mst_pf_pos.entry_at)
);