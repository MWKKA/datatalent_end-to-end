-- Team working - quality checks matching
-- A executer sur team_working/db/team_working.db

-- 1) KPI global matching
SELECT
    COUNT(*) AS total_offres,
    SUM(CASE WHEN sirene_matched = 1 THEN 1 ELSE 0 END) AS matched_offres,
    ROUND(
        100.0 * SUM(CASE WHEN sirene_matched = 1 THEN 1 ELSE 0 END) / COUNT(*),
        2
    ) AS matched_pct
FROM offres_enriched_team;

total_offres	matched_offres	matched_pct
150	62	41,33

-- 2) KPI NAF coverage
SELECT
    COUNT(*) AS total_offres,
    SUM(CASE WHEN code_naf_offre IS NOT NULL AND TRIM(code_naf_offre) <> '' THEN 1 ELSE 0 END) AS offers_with_naf,
    SUM(CASE WHEN code_naf_sirene IS NOT NULL AND TRIM(code_naf_sirene) <> '' THEN 1 ELSE 0 END) AS matched_with_sirene_naf,
    SUM(CASE WHEN naf_match = 1 THEN 1 ELSE 0 END) AS naf_exact_match
FROM offres_enriched_team;

total_offres	offers_with_naf	matched_with_sirene_naf	naf_exact_match
150	34	0	0

-- 3) Top entreprises non match (priorite amelioration)
SELECT
    company_name,
    COUNT(*) AS nb_offres
FROM offres_enriched_team
WHERE sirene_matched = 0
  AND company_name IS NOT NULL
  AND TRIM(company_name) <> ''
GROUP BY company_name
ORDER BY nb_offres DESC, company_name
LIMIT 20;
company_name	nb_offres
3.EUROTUNNEL SERVICES GIE	1
ADEQUAT INTERIM ET RECRUTEMENT	1
BASSETTI GROUP	1
CEDEO - CLIM+ - CDL ELEC - DISPART - ENV	1
CEDIS BI DEAPDATA	1
DATAGALAXY	1

-- 4) Top matches faibles (risque faux positifs)
SELECT
    offer_id,
    company_name,
    match_method,
    match_score,
    code_naf_offre,
    code_naf_sirene,
    siren,
    sirene_name
FROM offres_enriched_team
WHERE sirene_matched = 1
  AND COALESCE(match_score, 0) <= 80
ORDER BY match_score ASC, offer_id
LIMIT 30;
offer_id	company_name	match_method	match_score	code_naf_offre	code_naf_sirene	siren	sirene_name
1143765	Mercato de l'emploi	contains_normalized	75	[NULL]	[NULL]	831267968	LE MERCATO DE L EMPLOI
1143766	Mercato de l'emploi	contains_normalized	75	[NULL]	[NULL]	831267968	LE MERCATO DE L EMPLOI
204QDGQ	LECASUD	contains_normalized	75	[NULL]	[NULL]	479403578	SOCIETE IMMOBILIERE LECASUD

-- 5) Repartition des methodes de matching
SELECT
    match_method,
    COUNT(*) AS nb
FROM offres_enriched_team
GROUP BY match_method
ORDER BY nb DESC;
match_method	nb
no_match	88
exact_normalized	57
contains_normalized	3
prefix_normalized	2

-- 6) Entreprises presentes sous variantes proches (ex DATAGALAXY vs DATA GALAXY)
SELECT
    UPPER(REPLACE(company_name, ' ', '')) AS company_compact,
    COUNT(DISTINCT company_name) AS nb_variantes,
    GROUP_CONCAT(DISTINCT company_name) AS variantes
FROM offres_enriched_team
WHERE company_name IS NOT NULL
  AND TRIM(company_name) <> ''
GROUP BY UPPER(REPLACE(company_name, ' ', ''))
HAVING COUNT(DISTINCT company_name) > 1
ORDER BY nb_variantes DESC, company_compact
LIMIT 20;
company_compact	nb_variantes	variantes
DATAGALAXY	2	DATA GALAXY,DATAGALAXY