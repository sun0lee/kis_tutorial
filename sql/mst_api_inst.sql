INSERT INTO mst_api_inst (
    shrn_iscd,
    stnd_iscd,
    kor_name,
    info_type,
    atm_cls_code,
    mmsc_cls_code,
    acpr,
    unas_shrn_iscd,
    unas_kor_name,
    mrkt_div,
    use_yn,
    loaded_at
)
SELECT
    mri.shrn_iscd,
    mri.stnd_iscd,
    mri.kor_name,
    mri.info_type,
    mri.atm_cls_code,
    mri.mmsc_cls_code,
    mri.acpr,
    mri.unas_shrn_iscd,
    mri.unas_kor_name,
    mm.mrkt_div,
    'N',
    mri.loaded_at
FROM
    mst_raw_inst AS mri
JOIN
    map_mrktdiv_infotype AS mm
ON
    mri.info_type = mm.info_type
WHERE
    mri.info_type != '1' -- INSERT 시 필터링을 원한다면 여기에 추가
ON CONFLICT(shrn_iscd) DO UPDATE SET
    stnd_iscd = EXCLUDED.stnd_iscd,
    kor_name = EXCLUDED.kor_name,
    info_type = EXCLUDED.info_type,
    atm_cls_code = EXCLUDED.atm_cls_code,
    mmsc_cls_code = EXCLUDED.mmsc_cls_code,
    acpr = EXCLUDED.acpr,
    unas_shrn_iscd = EXCLUDED.unas_shrn_iscd,
    unas_kor_name = EXCLUDED.unas_kor_name,
    mrkt_div = EXCLUDED.mrkt_div,
    loaded_at = STRFTIME('%Y-%m-%dT%H:%M:%f', 'now')
