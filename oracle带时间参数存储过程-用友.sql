CREATE OR REPLACE 
procedure proc_m_map_data(
  i_date in varchar2,
	o_status out NUMBER,
	o_msg out VARCHAR2)Authid Current_User is
/***********************************************************
 * 过程名称: proc_m_map_data
 * 源码版本: V1.0
 * 主要功能1: map_data :地图数据过程表
 * 主要功能2: map_data_end :地图生成地图数据
 * 主要功能3:
 * 参数说明:
 * 开发人员: 陈强
 * 开发时间: 2015-08-28
 * 开发地点: 泰康金融
 * 装数模式: 增量
 * 装数频率: 1天
 * 修改人员:
 * 修改内容:
 * 修改时间:
 ***********************************************************/
  v_sql         varchar2(4000);
begin
  /* -----创建map_temp临时表----- */
	v_sql := 'create table map_temp as
select a.STR_NO,b.SALER_NO,a.STR_NAME,a.is_black_lst,A.STR_PROV,A.STR_AREA,
CASE WHEN A.STR_CITY=''重庆'' then A.STR_AREA ELSE A.STR_CITY END
STR_CITY from 
T_R_MAR_STORE a 
LEFT JOIN  T_R_SALER_STORE b on a.str_no=b.str_no 
ORDER BY  a.STR_NO,B.SALER_NO';
   execute immediate v_sql;
   commit;
 

 /*插入map_data表*/
  
  v_sql := 'insert into map_data

select aaa.str_prov,aaa.str_city,bbb.SALER_NUM,aaa.loan_amt,aaa.str_num,SYSDATE TIMESTRAMP from (
---门店数据
SELECT aaa.*,loan_amt  from(
select str_prov,str_city,sum(str_num) str_num,SYSDATE from (
select str_prov,case when str_prov=''重庆'' then str_AREA else str_city end str_city
,count(DISTINCT(str_no)) str_num from map_temp
where is_black_lst=''1''
GROUP BY  str_prov,str_city,str_AREA)
GROUP BY str_prov,str_city
ORDER BY str_prov,str_city
) aaa
LEFT JOIN
( 
select STR_PROV,str_city,round(sum(loan_amt)/10000,2) loan_amt from (
 select  cc.str_prov,case when cc.str_prov=''重庆'' then cc.str_AREA else cc.str_city end str_city,cc.str_area,
sum(loan_amt) loan_amt  from t_loan_base_info aa
  LEFT JOIN TEMP_SALES_INFO bb on aa.loan_no=bb.loan_no
  LEFT JOIN T_R_MAR_STORE cc on bb.str_no=cc.str_no
  where aa.apply_date<=sysdate and  TRUNC(aa.APPLY_DATE)>=trunc(sysdate, ''mm'') and aa.aprov_result=''013005''
  GROUP BY cc.str_prov,str_city,CC.str_AREA
  ORDER BY cc.str_prov,cc.str_city,cc.str_area)
  GROUP BY STR_PROV,str_city
  ORDER BY str_prov,str_city
) bbb on aaa.str_prov=bbb.str_prov and aaa.str_city=bbb.str_city)aaa
left JOIN
(
--销售数据 
select ORG_ARE STR_CITY ,COUNT(DISTINCT(SALER_NO)) SALER_NUM from (
SELECT
	I.SALER_NO,
	F.STAFF_ID,
	F.STAFF_NAME,
	S.ORG_NAME,
	S.ORG_ARE
FROM
	SYS_STAFF F
JOIN SYS_ORG S ON F.STAFF_NET_ID = S.ORG_ID
JOIN T_TEMP_SALER_INFO I ON F.STAFF_ID = I.STAFF_ID
WHERE
	F.STAFF_ID IN (
		SELECT
			STAFF_ID
		FROM
			T_TEMP_SALER_INFO
		WHERE
			STATE = ''1''
	)
AND f.STAFF_ID IN (
	SELECT
		staff_id
	FROM
		SYS_STAFF_ROLE
	WHERE
		ROLE_ID = ''9AD8CDAE7F00000137494ADADF13BF09''
)
ORDER BY org_are)
GROUP BY ORG_ARE
) bbb on aaa.STR_CITY=bbb.STR_CITY
ORDER BY aaa.STR_PROV';
   execute immediate v_sql;
   commit;


/*删除map_temp表*/
  v_sql := 'drop table map_temp';
  execute immediate v_sql;
  commit;

/*获取最终数据*/

v_sql := 'insert into map_data_end
select str_prov, STR_CITY,str_num,SALER_NUM,LOAN_AMT,STR_NUM_MINUS,SALER_NUM_MINUS,LOAN_AMT_MINUS, TIMESTREAMP
 from 
(SELECT
	A .str_prov,

case when A .str_city=''黔东南'' then ''黔东南苗族侗族自治州''
when  A .str_city=''毕节'' then ''毕节地区'' 
when  A .str_city=''毕节'' then ''毕节地区''
when  A .str_city=''毕节'' then ''毕节地区''
when  A .str_city in (''江北'',''沙坪坝'',''渝中'',''九龙坡'',''合川'',''永川'',''万州'',''涪陵'',''江津'',''南岸'') then  A .str_city||''区''
when  A .str_city =''开县'' then ''开县''
when   A .str_city =''綦江'' then ''綦江县''
else  A .str_city||''市'' end str_city,
	A .str_num,
	A .saler_num,
	A .loan_amt,
	b.str_num - A .str_num str_num_minus,
	b.saler_num - A .saler_num saler_num_minus,
	case when a.loan_amt=0 then b.loan_amt else round(b.loan_amt/a.loan_amt,2)  end  loan_amt_minus,

--b.loan_amt - A .loan_amt loan_amt_minus,
	SYSDATE TIMESTREAMP
FROM
	(
---
		SELECT
		T .*
	FROM
		(
			SELECT
				TRUNC (MAX(TIMESTREAMP)) dt
			FROM
				map_data
		) A,
		map_data T
	WHERE
		A .dt = TRUNC (T .TIMESTREAMP)
	) A
LEFT JOIN (
	SELECT
			T .*
		FROM
			(
				SELECT
					TRUNC (MAX(TIMESTREAMP),''mm'')-1 dt
				FROM
					map_data
			) A,
			map_data T
		WHERE
			A .dt = TRUNC (T .TIMESTREAMP)
) b ON A .str_city = b.str_city)
';
   execute immediate v_sql;
   commit;

	o_status := 0 ;
	o_msg := 'successful' ;
end;
