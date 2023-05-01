drop table if exists dw_risk.first_pcl_final;

create table dw_risk.firstpcl(
ssn_hash                        varchar
,id                                int
,product_type                    varchar
,create_date                      date
,cnt_tot_post_accts1_orig         float
,cnt_pcl_post_accts1_orig         float
,cnt_pl_post_accts1_orig          float
,cnt_dep_post_accts1_orig         float
,cnt_hm_post_accts1_orig          float
,cnt_tot_post_accts1_3month       float
,cnt_pcl_post_accts1_3month       float
,cnt_pl_post_accts1_3month        float
,cnt_dep_post_accts1_3month       float
,cnt_hm_post_accts1_3month        float
,cnt_tot_post_accts1_6month       float
,cnt_pcl_post_accts1_6month       float
,cnt_pl_post_accts1_6month        float
,cnt_dep_post_accts1_6month       float
,cnt_hm_post_accts1_6month        float
,cnt_tot_post_accts1_12month      float
,cnt_pcl_post_accts1_12month      float
,cnt_pl_post_accts1_12month       float
,cnt_dep_post_accts1_12month      float
,cnt_hm_post_accts1_12month       float
,cnt_tot_post_accts1_today        float
,cnt_pcl_post_accts1_today        float
,cnt_pl_post_accts1_today         float
,cnt_dep_post_accts1_today        float
,cnt_hm_post_accts1_today         float
);


COPY dw_risk.firstpcl
from  's3://upg-redshift-dropbox-usw2-prod/AROAX5KSFFV7UKNQLNYDA:rjangada@upgrade.com/final_output_onlypcl.csv'
iam_role 'arn:aws:iam::544022539647:role/redshift-edwreplica-assume-role-usw2-prod,arn:aws:iam::544022539647:role/s3-assume-role-dropbox-edwreplica-usw2-prod'
format as csv
IGNOREHEADER 1;


SELECT  *
FROM stl_load_errors
order by starttime desc

select top 10 *
from dw_risk.firstpcl

grant select on dw_risk.firstpcl to public;