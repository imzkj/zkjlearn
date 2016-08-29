-- 近90天最近访问时间
-- 开发人： 郑可佳/全伟
-- 开发日期：2016-08-25
-fdhshg反对韩国护肤呸呸呸根深蒂固
refresh dw.bdl_com_recommend;
refresh dw.bdl_com_headlines;
refresh dw.bdl_ssj_feidee_user;
INSERT INTO tag.ssj_device_user_tagging (udidmd5,sq_latestvisit_90d)
SELECT
    udfs.UDFStringMD5(CONCAT(pool.deviceid,'_',pool.sid)) AS udidmd5,
    bcrh.ymd AS sq_latestvisit_90d
FROM (
    SELECT 
        CAST(bcrbch.uid AS BIGINT) AS uid,
        bcrbch.ymd
    FROM (
        SELECT 
            ruh.uid,
            ruh.ymd,
            row_number() OVER(PARTITION BY ruh.uid ORDER BY ruh.ymd DESC) rank
        FROM
            (
            SELECT
                uid,
                ymd
            FROM
                dw.bdl_com_recommend
            WHERE 
                ymd>=from_unixtime(unix_timestamp()-86400*90,'yyyy-MM-dd') AND from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd')>=ymd AND uid IS NOT NULL AND uid!='' AND uid!='null'
        UNION ALL
            SELECT 
                uid,ymd
            FROM
                dw.bdl_com_headlines
            WHERE 
                ymd>=from_unixtime(unix_timestamp()-86400*90,'yyyy-MM-dd') AND from_unixtime(unix_timestamp()-86400,'yyyy-MM-dd')>=ymd AND uid IS NOT NULL AND uid!='' AND uid!='null'
            ) ruh
    ) bcrbch
    WHERE
        rank=1
    ) bcrh
INNER JOIN (
    SELECT DISTINCT fid,fname
    FROM
    dw.bdl_ssj_feidee_user
    WHERE
    fstatus=0 AND ffrom NOT LIKE '%cardniu%' AND ffrom NOT LIKE 'market-%' AND ffrom NOT LIKE '%mycredit%'
    ) bsfu
ON bcrh.uid=bsfu.fid
INNER JOIN (
    SELECT 
        deviceid, sid 
    FROM 
        basic.device_user_pool 
    WHERE
        sid IS NOT NULL AND sid!='' AND sid!='null'  
    ) pool
ON pool.sid=bsfu.fname;
