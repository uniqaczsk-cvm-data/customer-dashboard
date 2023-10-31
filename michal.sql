DROP TABLE IF EXISTS #FST;

SELECT  s.idsubject
       ,CAST(MIN(occ.RecValidFrom) AS DATE) fst_dt
       ,RANK() OVER (PARTITION BY s.idsubject ORDER BY MIN(occ.RecValidFrom) asc) ranked INTO #FST
FROM DW_C360_Hist.dbo.Object_ClientContract occ
JOIN [DW_C360_Hist].dbo.Object o
ON o.id_Object_ClientContract = occ.IdObject_ClientContract AND o.id_ENU_ObjectType = 1 -- smlouva 
 AND o.IsLastVersion = 1
JOIN [DW_C360_Hist].dbo.Role r
ON o.idObject = r.Id_Object AND r.Id_ENU_RoleType = 2 -- klient 
 AND r.IsLastVersion = 1
JOIN DW_C360_Hist.dbo.Subject s
ON s.IdSubject = r.Id_Subject AND s.IsLastVersion = 1
WHERE occ.IsActive = 1 --and s.idsubject = '17852515' 
GROUP BY  s.idsubject; WHILE ( (

SELECT  CAST(MAX(month_id) AS INT)
FROM dw_cvm.dbo.mn_nni_all
WHERE flow_type = 1) < (
SELECT  CAST((month(current_timestamp)+year(current_timestamp)*100) AS INT))) BEGIN
WITH t1 AS
(
	SELECT  s.idsubject
	       ,o.idobject
	       ,occ.isActive
	       ,occ.id_enu_product
	       ,occ.id_ENU_DataSource
	       ,occ.Id_ENU_Country
	       ,ContractNumber
	       ,CAST(MIN(occ.RecValidFrom)        AS DATE) start_dt
	       ,CAST(MAX(occ.RecValidTo) AS DATE) AS end_dt
	FROM DW_C360_Hist.dbo.Object_ClientContract occ
	JOIN [DW_C360_Hist].dbo.Object o
	ON o.id_Object_ClientContract = occ.IdObject_ClientContract AND o.id_ENU_ObjectType = 1 -- smlouva 
 AND o.IsLastVersion = 1
	JOIN [DW_C360_Hist].dbo.Role r
	ON o.idObject = r.Id_Object AND r.Id_ENU_RoleType = 2 -- klient 
 AND r.IsLastVersion = 1
	JOIN DW_C360_Hist.dbo.Subject s
	ON s.IdSubject = r.Id_Subject AND s.IsLastVersion = 1
	WHERE occ.IsActive = 1 --and s.idsubject = '17852515' 
	GROUP BY  s.idsubject
	         ,o.idobject
	         ,occ.isActive
	         ,occ.id_enu_product
	         ,occ.id_ENU_DataSource
	         ,occ.Id_ENU_Country
	         ,ContractNumber
	HAVING YEAR(MIN(occ.RecValidFrom)) = --'2022' 
 (
	SELECT  CASE WHEN MONTH(MAX(start_dt)) = 12 THEN YEAR(dateadd(year,1,MAX(start_dt)))  ELSE YEAR(MAX(start_dt)) END
	FROM dw_cvm.dbo.mn_nni_all
	WHERE flow_type = 1) AND MONTH(MIN (occ.RecValidFrom)) = --'1' 
 (
	SELECT  CASE WHEN MONTH(MAX(start_dt)) = 12 THEN 1  ELSE MONTH(dateadd(MONTH,1,MAX(start_dt))) END
	FROM dw_cvm.dbo.mn_nni_all
	WHERE flow_type = 1) 
) , t2 AS
(
	SELECT  ContractNumber
	       ,idSubject
	       ,idObject
	       ,start_dt
	       ,(month(t1.start_dt)+year(t1.start_dt)*100)                                                   AS month_id
	       ,pg1.ProductGroup_lvl1
	       ,pg2.ProductGroup_lvl2
	       ,CASE WHEN t1.id_enu_country = 1 THEN 'CZ'
	             WHEN t1.id_enu_country = 2 THEN 'SK'  ELSE null END                                     AS Country
	       ,CASE WHEN t1.id_ENU_DataSource IN (1,9,200,203) THEN 'Life'
	             WHEN t1.id_ENU_DataSource IN (2,27,10,11) THEN 'Pension'
	             WHEN t1.id_ENU_DataSource IN (4,5,12,13,201,202,204,205,206,207,208,209) THEN 'PaC'
	             WHEN t1.id_ENU_DataSource IN (3,18,21,25,111,19,20,22,26) THEN 'MF'  ELSE 'Unknown' END AS LoB
	FROM t1
	JOIN DW_C360_Hist.dbo.ENU_Product p
	ON t1.Id_ENU_Product = p.idENU_Product
	JOIN DW_C360_Hist.dbo.ENU_ProductGroup_lvl2 pg2
	ON pg2.idENU_ProductGroup_lvl2 = p.[id_ENU_ProductGroup_lvl2]
	JOIN DW_C360_Hist.dbo.ENU_ProductGroup_lvl1 pg1
	ON pg1.idENU_ProductGroup_lvl1 = pg2.[id_ENU_ProductGroup_lvl1]
) , t3 AS
(
	SELECT  distinct t2.*
	       ,ose.OrganizationalEntityNameL1
	       ,CASE WHEN ose.SalesDistributionChannelCode IN ('SK16','SK17') THEN 'Direct Sales'  ELSE ose.OrganizationalEntityNameL2 END OrganizationalEntityNameL2
	       ,ose.OrganizationalEntityNameL3
	       ,ose.OrganizationalEntityNameL4
	       ,ose.OrganizationalEntityNameL5
	       ,ose.OrganizationalEntityNameL6
	       ,RANK() OVER (PARTITION BY contractnumber ORDER BY OrganizationalEntityNameL6 DESC) ranked
	FROM t2
	JOIN DW_C360_Hist.dbo.Object_AgentContract_AA_PO aapo
	ON t2.idObject = aapo.Id_Object AND aapo.IsLastVersion = 1
	JOIN DW_C360_Hist.dbo.Object_AgentContract oac
	ON aapo.id_Object_AgentContract_PO = oac.IdObject_AgentContract AND oac.IsLastVersion = 1
	JOIN DW_C360_Hist.dbo.Object_AgentContract_Agent oaca
	ON oac.Id_Object_AgentContract_Agent = oaca.IdObject_AgentContract_Agent AND oaca.IsLastVersion = 1
	JOIN DP_TIM.edw.Agent ag
	ON oaca.AgentNumber = ag.AgentNumber AND ag.IsLastVersion = 1
	JOIN dp_tim.dm.v_OrganizationalStructureExtended ose
	ON ag.agentid = ose.agentid
)
INSERT INTO dw_cvm.dbo.mn_nni_all
SELECT  distinct a.*
       ,1 AS flow_type
--into dw_cvm.dbo.mn_nni_all 
FROM t3 a
WHERE ranked = 1 end;

SELECT  month_id
       ,flow_type
       ,COUNT(*)
FROM dw_cvm.dbo.mn_nni_all
GROUP BY  month_id
         ,flow_type
ORDER BY month_id asc
SELECT  *
FROM dw_cvm.dbo.mn_nni_all
SELECT  *
FROM #FST
WHERE idsubject = 36402123
AND ranked = 1
SELECT  *
FROM dw_cvm.dbo.mn_nni_all;

SELECT  month_id
       ,flow_type
       ,COUNT(*)
FROM dw_cvm.dbo.mn_nni_all
GROUP BY  month_id
         ,flow_type
ORDER BY month_id
         ,flow_type asc
--- 
 DROP TABLE IF EXISTS #LAST;

SELECT  s.idsubject
       ,CAST(MAX(occ.RecValidTo) AS DATE) Lst_dt
       ,RANK() OVER (PARTITION BY s.idsubject ORDER BY MAX(occ.RecValidTo) asc) ranked INTO #LAST
FROM DW_C360_Hist.dbo.Object_ClientContract occ
JOIN [DW_C360_Hist].dbo.Object o
ON o.id_Object_ClientContract = occ.IdObject_ClientContract AND o.id_ENU_ObjectType = 1 -- smlouva 
 AND o.IsLastVersion = 1
JOIN [DW_C360_Hist].dbo.Role r
ON o.idObject = r.Id_Object AND r.Id_ENU_RoleType = 2 -- klient 
 AND r.IsLastVersion = 1
JOIN DW_C360_Hist.dbo.Subject s
ON s.IdSubject = r.Id_Subject AND s.IsLastVersion = 1
WHERE occ.IsActive = 1 --and s.idsubject = '17852515' 
GROUP BY  s.idsubject;

SELECT  *
FROM #LAST
SELECT  a.*
--l.lstt_dt,
--f.fstt_dt,
--l.lstt_month_id,
--f.fstt_month_id, 
       ,CASE WHEN a.flow_type = 1 THEN f.fstt_dt  ELSE l.lstt_dt END             AS act_dt
       ,CASE WHEN a.flow_type = 1 THEN f.fstt_month_id  ELSE l.lstt_month_id END AS act_month_id
       ,CASE WHEN a.flow_type = 1 THEN f.actt_type  ELSE l.actt_type END         AS act_type into dw_cvm.dbo.mn_nni_all_1
FROM dw_cvm.dbo.mn_nni_all a
LEFT JOIN
(
	SELECT  distinct a.ContractNumber
	       ,a.idObject
	       ,a.month_id
	       ,a.flow_type
	       ,f.fst_dt                                                             AS fstt_dt
	       ,f.fst_month_id                                                       AS fstt_month_id
	       ,CASE WHEN f.fst_dt = a.start_dt THEN 'New Client'  ELSE 'X-sell' END AS actt_type
	FROM dw_cvm.dbo.mn_nni_all A
	LEFT JOIN
	(
		SELECT  f.IdSubject
		       ,fst_dt
		       ,(month(fst_dt)+year(fst_dt)*100) AS fst_month_id
		FROM #FST f
		WHERE ranked = 1 
	) f
	ON a.idsubject = f.idsubject
	WHERE a.flow_type = 1 
) f
ON f.contractnumber = a.ContractNumber AND a.month_id = f.month_id AND a.flow_type = f.flow_type
LEFT JOIN
(
	SELECT  distinct a.ContractNumber
	       ,a.idObject
	       ,a.month_id
	       ,a.flow_type
	       ,l.lst_dt                                                              AS lstt_dt
	       ,l.lst_month_id                                                        AS lstt_month_id
	       ,CASE WHEN l.lst_dt = a.start_dt THEN 'Lost client'  ELSE 'X-lost' END AS actt_type
	FROM dw_cvm.dbo.mn_nni_all A
	LEFT JOIN
	(
		SELECT  f.IdSubject
		       ,lst_dt
		       ,(month(lst_dt)+year(lst_dt)*100) AS lst_month_id
		FROM #LAST f
		WHERE ranked = 1 
	) l
	ON a.idsubject = l.idsubject
	WHERE a.flow_type = 2 
) l
ON l.contractnumber = a.ContractNumber AND a.month_id = l.month_id AND a.flow_type = l.flow_type
SELECT  *
FROM dw_cvm.dbo.mn_nni_all
WHERE ContractNumber = '9460063189' DELETE
FROM dw_cvm.dbo.mn_nni_all_1
SELECT  *
FROM dw_cvm.dbo.mn_nni_all_1

ALTER TABLE dw_cvm.dbo.mn_nni_all_1 DROP COLUMN lstt_month_id
SELECT  *
FROM dw_cvm.dbo.mn_nni_all
WHERE month_id = 202201
AND flow_type = 1
SELECT  distinct a.*
       ,f.fst_dt
       ,f.fst_month_id
       ,CASE WHEN fst_dt = a.start_dt THEN 'New Client'  ELSE 'X-sell' END AS act_type
FROM dw_cvm.dbo.mn_nni_all A
LEFT JOIN
(
	SELECT  f.IdSubject
	       ,fst_dt
	       ,(month(fst_dt)+year(fst_dt)*100) AS fst_month_id
	FROM #FST f
	WHERE ranked = 1 
) f
ON a.idsubject = f.idsubject
WHERE a.flow_type = 1
AND a.ContractNumber = '9460063189'
SELECT  *
FROM #fst
SELECT  *
FROM dw_cvm.dbo.mn_nni_all
WHERE contractnumber = '9460063189'
SELECT  distinct x.IdSubject
       ,COUNT(*)
FROM
(
	SELECT  f.IdSubject
	       ,f.ContractNumber                 AS fst_contractnumber
	       ,fst_dt
	       ,(month(fst_dt)+year(fst_dt)*100) AS fst_month_id
	FROM #FST f
	WHERE ranked = 1 
) x
GROUP BY  x.IdSubject
HAVING COUNT(*) > 1
SELECT  *
FROM #fst
WHERE idsubject = 49555748
SELECT  *
FROM DW_CVM.dbo.TV_Portfolio
SELECT  COUNT(*)
FROM dw_cvm.dbo.mn_nni_all_1
WHERE month_id
SELECT  f.*
       ,(month(fst_dt)+year(fst_dt)*100) AS month_id
FROM #FST f
WHERE ranked = 1
SELECT  s.idsubject
       ,occ.ContractNumber
       ,CAST(MIN(beginDATE) AS DATE) fst_dt
       ,RANK() OVER (PARTITION BY s.idsubject ORDER BY MIN(beginDATE) asc) ranked
FROM DW_C360_Hist.dbo.Object_ClientContract occ
JOIN [DW_C360_Hist].dbo.Object o
ON o.id_Object_ClientContract = occ.IdObject_ClientContract AND o.id_ENU_ObjectType = 1 -- smlouva 
 AND o.IsLastVersion = 1
JOIN [DW_C360_Hist].dbo.Role r
ON o.idObject = r.Id_Object AND r.Id_ENU_RoleType = 2 -- klient 
 AND r.IsLastVersion = 1
JOIN DW_C360_Hist.dbo.Subject s
ON s.IdSubject = r.Id_Subject AND s.IsLastVersion = 1
WHERE occ.IsActive = 1 --and s.idsubject = '17852515' 
GROUP BY  s.idsubject
         ,occ.ContractNumber
SELECT  *
FROM DW_C360_Hist.dbo.Object_ClientContract
WHERE contractnumber = '8501787670'
SELECT  a.*
--l.lstt_dt,
--f.fstt_dt,
--l.lstt_month_id,
--f.fstt_month_id, 
       ,CASE WHEN a.flow_type = 1 THEN f.fstt_dt  ELSE l.lstt_dt END             AS act_dt
       ,CASE WHEN a.flow_type = 1 THEN f.fstt_month_id  ELSE l.lstt_month_id END AS act_month_id
       ,CASE WHEN a.flow_type = 1 THEN f.actt_type  ELSE l.actt_type END         AS act_type
--into dw_cvm.dbo.mn_nni_all_1 
FROM dw_cvm.dbo.mn_nni_all a
LEFT JOIN
(
	SELECT  distinct a.ContractNumber
	       ,a.idObject
	       ,a.month_id
	       ,a.flow_type
	       ,f.fst_dt                                                             AS fstt_dt
	       ,f.fst_month_id                                                       AS fstt_month_id
	       ,CASE WHEN f.fst_dt = a.start_dt THEN 'New Client'  ELSE 'X-sell' END AS actt_type
	FROM dw_cvm.dbo.mn_nni_all A
	LEFT JOIN
	(
		SELECT  f.IdSubject
		       ,fst_dt
		       ,(month(fst_dt)+year(fst_dt)*100) AS fst_month_id
		FROM #FST f
		WHERE ranked = 1 
	) f
	ON a.idsubject = f.idsubject
	WHERE a.flow_type = 1
	AND ContractNumber = '9460063189' 
) f
ON f.contractnumber = a.ContractNumber AND a.month_id = f.month_id AND a.flow_type = f.flow_type
LEFT JOIN
(
	SELECT  distinct a.ContractNumber
	       ,a.idObject
	       ,a.month_id
	       ,a.flow_type
	       ,l.lst_dt                                                              AS lstt_dt
	       ,l.lst_month_id                                                        AS lstt_month_id
	       ,CASE WHEN l.lst_dt = a.start_dt THEN 'Lost client'  ELSE 'X-lost' END AS actt_type
	FROM dw_cvm.dbo.mn_nni_all A
	LEFT JOIN
	(
		SELECT  f.IdSubject
		       ,lst_dt
		       ,(month(lst_dt)+year(lst_dt)*100) AS lst_month_id
		FROM #LAST f
		WHERE ranked = 1 
	) l
	ON a.idsubject = l.idsubject
	WHERE a.flow_type = 2
	AND ContractNumber = '9460063189' 
) l
ON l.contractnumber = a.ContractNumber AND a.month_id = l.month_id AND a.flow_type = l.flow_type
