PRAGMA table_info(raw_geo_regions);
cid	name	type	notnull	dflt_value	pk
0	code	TEXT	0	[NULL]	1
1	nom	TEXT	0	[NULL]	0


PRAGMA table_info(raw_geo_departements);
cid	name	type	notnull	dflt_value	pk
0	code	TEXT	0	[NULL]	1
1	nom	TEXT	0	[NULL]	0
2	code_region	TEXT	0	[NULL]	0

PRAGMA table_info(raw_geo_communes);
cid	name	type	notnull	dflt_value	pk
0	code	TEXT	0	[NULL]	1
1	nom	TEXT	0	[NULL]	0
2	code_departement	TEXT	0	[NULL]	0
3	code_region	TEXT	0	[NULL]	0
4	codes_postaux	TEXT	0	[NULL]	0
5	population	INTEGER	0	[NULL]	0


PRAGMA table_info(raw_france_travail_offres);
cid	name	type	notnull	dflt_value	pk
0	id	TEXT	0	[NULL]	1
1	intitule	TEXT	0	[NULL]	0
2	rome_code	TEXT	0	[NULL]	0
3	rome_libelle	TEXT	0	[NULL]	0
4	type_contrat	TEXT	0	[NULL]	0
5	date_creation	TEXT	0	[NULL]	0
6	lieu_libelle	TEXT	0	[NULL]	0
7	code_postal	TEXT	0	[NULL]	0
8	commune_code	TEXT	0	[NULL]	0
9	entreprise_nom	TEXT	0	[NULL]	0
10	salaire_libelle	TEXT	0	[NULL]	0
11	code_naf	TEXT	0	[NULL]	0
12	secteur_activite_libelle	TEXT	0	[NULL]	0
13	description_extrait	TEXT	0	[NULL]	0


PRAGMA table_info(raw_sirene_etablissement);
cid	name	type	notnull	dflt_value	pk
0	siren	TEXT	0	[NULL]	0
1	nic	TEXT	0	[NULL]	0
2	siret	TEXT	0	[NULL]	0
3	statutDiffusionEtablissement	TEXT	0	[NULL]	0
4	dateCreationEtablissement	DATE	0	[NULL]	0
5	trancheEffectifsEtablissement	TEXT	0	[NULL]	0
6	anneeEffectifsEtablissement	REAL	0	[NULL]	0
7	activitePrincipaleRegistreMetiersEtablissement	TEXT	0	[NULL]	0
8	dateDernierTraitementEtablissement	TIMESTAMP	0	[NULL]	0
9	etablissementSiege	INTEGER	0	[NULL]	0
10	nombrePeriodesEtablissement	INTEGER	0	[NULL]	0
11	complementAdresseEtablissement	TEXT	0	[NULL]	0
12	numeroVoieEtablissement	TEXT	0	[NULL]	0
13	indiceRepetitionEtablissement	TEXT	0	[NULL]	0
14	dernierNumeroVoieEtablissement	TEXT	0	[NULL]	0
15	indiceRepetitionDernierNumeroVoieEtablissement	TEXT	0	[NULL]	0
16	typeVoieEtablissement	TEXT	0	[NULL]	0
17	libelleVoieEtablissement	TEXT	0	[NULL]	0
18	codePostalEtablissement	TEXT	0	[NULL]	0
19	libelleCommuneEtablissement	TEXT	0	[NULL]	0
20	libelleCommuneEtrangerEtablissement	TEXT	0	[NULL]	0
21	distributionSpecialeEtablissement	TEXT	0	[NULL]	0
22	codeCommuneEtablissement	TEXT	0	[NULL]	0
23	codeCedexEtablissement	TEXT	0	[NULL]	0
24	libelleCedexEtablissement	TEXT	0	[NULL]	0
25	codePaysEtrangerEtablissement	TEXT	0	[NULL]	0
26	libellePaysEtrangerEtablissement	TEXT	0	[NULL]	0
27	identifiantAdresseEtablissement	TEXT	0	[NULL]	0
28	coordonneeLambertAbscisseEtablissement	TEXT	0	[NULL]	0
29	coordonneeLambertOrdonneeEtablissement	TEXT	0	[NULL]	0
30	complementAdresse2Etablissement	TEXT	0	[NULL]	0
31	numeroVoie2Etablissement	TEXT	0	[NULL]	0
32	indiceRepetition2Etablissement	TEXT	0	[NULL]	0
33	typeVoie2Etablissement	TEXT	0	[NULL]	0
34	libelleVoie2Etablissement	TEXT	0	[NULL]	0
35	codePostal2Etablissement	TEXT	0	[NULL]	0
36	libelleCommune2Etablissement	TEXT	0	[NULL]	0
37	libelleCommuneEtranger2Etablissement	TEXT	0	[NULL]	0
38	distributionSpeciale2Etablissement	TEXT	0	[NULL]	0
39	codeCommune2Etablissement	TEXT	0	[NULL]	0
40	codeCedex2Etablissement	TEXT	0	[NULL]	0
41	libelleCedex2Etablissement	TEXT	0	[NULL]	0
42	codePaysEtranger2Etablissement	TEXT	0	[NULL]	0
43	libellePaysEtranger2Etablissement	TEXT	0	[NULL]	0
44	dateDebut	DATE	0	[NULL]	0
45	etatAdministratifEtablissement	TEXT	0	[NULL]	0
46	enseigne1Etablissement	TEXT	0	[NULL]	0
47	enseigne2Etablissement	TEXT	0	[NULL]	0
48	enseigne3Etablissement	TEXT	0	[NULL]	0
49	denominationUsuelleEtablissement	TEXT	0	[NULL]	0
50	activitePrincipaleEtablissement	TEXT	0	[NULL]	0
51	nomenclatureActivitePrincipaleEtablissement	TEXT	0	[NULL]	0
52	caractereEmployeurEtablissement	TEXT	0	[NULL]	0
53	activitePrincipaleNAF25Etablissement	TEXT	0	[NULL]	0


PRAGMA table_info(raw_sirene_unite_legale);
cid	name	type	notnull	dflt_value	pk
0	siren	TEXT	0	[NULL]	0
1	statutDiffusionUniteLegale	TEXT	0	[NULL]	0
2	unitePurgeeUniteLegale	INTEGER	0	[NULL]	0
3	dateCreationUniteLegale	DATE	0	[NULL]	0
4	sigleUniteLegale	TEXT	0	[NULL]	0
5	sexeUniteLegale	TEXT	0	[NULL]	0
6	prenom1UniteLegale	TEXT	0	[NULL]	0
7	prenom2UniteLegale	TEXT	0	[NULL]	0
8	prenom3UniteLegale	TEXT	0	[NULL]	0
9	prenom4UniteLegale	TEXT	0	[NULL]	0
10	prenomUsuelUniteLegale	TEXT	0	[NULL]	0
11	pseudonymeUniteLegale	TEXT	0	[NULL]	0
12	identifiantAssociationUniteLegale	TEXT	0	[NULL]	0
13	trancheEffectifsUniteLegale	TEXT	0	[NULL]	0
14	anneeEffectifsUniteLegale	REAL	0	[NULL]	0
15	dateDernierTraitementUniteLegale	TIMESTAMP	0	[NULL]	0
16	nombrePeriodesUniteLegale	INTEGER	0	[NULL]	0
17	categorieEntreprise	TEXT	0	[NULL]	0
18	anneeCategorieEntreprise	REAL	0	[NULL]	0
19	dateDebut	DATE	0	[NULL]	0
20	etatAdministratifUniteLegale	TEXT	0	[NULL]	0
21	nomUniteLegale	TEXT	0	[NULL]	0
22	nomUsageUniteLegale	TEXT	0	[NULL]	0
23	denominationUniteLegale	TEXT	0	[NULL]	0
24	denominationUsuelle1UniteLegale	TEXT	0	[NULL]	0
25	denominationUsuelle2UniteLegale	TEXT	0	[NULL]	0
26	denominationUsuelle3UniteLegale	TEXT	0	[NULL]	0
27	categorieJuridiqueUniteLegale	INTEGER	0	[NULL]	0
28	activitePrincipaleUniteLegale	TEXT	0	[NULL]	0
29	nomenclatureActivitePrincipaleUniteLegale	TEXT	0	[NULL]	0
30	nicSiegeUniteLegale	TEXT	0	[NULL]	0
31	economieSocialeSolidaireUniteLegale	TEXT	0	[NULL]	0
32	societeMissionUniteLegale	TEXT	0	[NULL]	0
33	caractereEmployeurUniteLegale	TEXT	0	[NULL]	0
34	activitePrincipaleNAF25UniteLegale	TEXT	0	[NULL]	0


SELECT 'regions' AS table_name, COUNT(*) AS nb_lignes, COUNT(DISTINCT code) AS nb_distinct_code
FROM raw_geo_regions;
table_name	nb_lignes	nb_distinct_code
regions	18	18


SELECT 'departements' AS table_name, COUNT(*) AS nb_lignes, COUNT(DISTINCT code) AS nb_distinct_code
FROM raw_geo_departements;
table_name	nb_lignes	nb_distinct_code
departements	101	101


SELECT 'offres' AS table_name, COUNT(*) AS nb_lignes, COUNT(DISTINCT id) AS nb_distinct_id
FROM raw_france_travail_offres;
table_name	nb_lignes	nb_distinct_id
offres	150	150


SELECT 'sirene_etablissement' AS table_name, COUNT(*) AS nb_lignes, COUNT(DISTINCT siret) AS nb_distinct_siret
FROM raw_sirene_etablissement;
table_name	nb_lignes	nb_distinct_siret
sirene_etablissement	42 909 379	42 909 379


SELECT 'sirene_unite_legale' AS table_name, COUNT(*) AS nb_lignes, COUNT(DISTINCT siren) AS nb_distinct_siren
FROM raw_sirene_unite_legale;
table_name	nb_lignes	nb_distinct_siren
sirene_unite_legale	29 331 094	29 331 094


SELECT 'offres avec commune_code' AS lib, COUNT(*) AS nb
FROM raw_france_travail_offres WHERE commune_code IS NOT NULL AND commune_code != '';
lib	nb
offres avec commune_code	136


SELECT 'offres dont commune_code dans geo_communes' AS lib, COUNT(*) AS nb
FROM raw_france_travail_offres o
WHERE o.commune_code IS NOT NULL AND o.commune_code != ''
  AND EXISTS (SELECT 1 FROM raw_geo_communes c WHERE c.code = o.commune_code);
lib	nb
offres dont commune_code dans geo_communes	118


SELECT o.id, o.intitule, o.commune_code, o.code_postal, c.nom AS commune_nom, c.code_departement, c.code_region
FROM raw_france_travail_offres o
LEFT JOIN raw_geo_communes c ON c.code = o.commune_code
LIMIT 20;
id	intitule	commune_code	code_postal	commune_nom	code_departement	code_region
205KZMV	Data Engineer - Data Solutions F/H (H/F)	92051	92200	Neuilly-sur-Seine	92	11
205JXPJ	Data Engineer IA Copilot Studio & Automatisation IA (H/F)	[NULL]	[NULL]	[NULL]	[NULL]	[NULL]
205JMZV	Data Engineer (H/F)	59512	59100	Roubaix	59	32
205JGDF	Data Engineer GCP H/F	31555	31000	Toulouse	31	76
205HZRP	Data Engineer (H/F)	13212	13012	[NULL]	[NULL]	[NULL]
205HNXZ	#ForumOGCNice Lead DATA Engineer F/H	06152	06560	Valbonne	06	93
205CQWS	Data engineer (H/F)	92062	92800	Puteaux	92	11
204XPYS	Data Analyst / Data Engineer Confirmé(e) F/H (H/F)	75109	75009	[NULL]	[NULL]	[NULL]
204XHFX	DEVELOPPEUR REACT NATIVE / DATA ENGINEER H /F (H/F)	75102	75002	[NULL]	[NULL]	[NULL]
204QWDD	Data Engineer Confirmé F/H	91477	91120	Palaiseau	91	11
204PPSK	Data Engineer Confirmé F/H (H/F)	67482	67000	Strasbourg	67	44
204LRTC	Expert TALEND-DATA Engineer (F/H) (H/F)	29019	29200	Brest	29	53
204JCST	Data Engineer / Scientist (H/F)	94069	94410	Saint-Maurice	94	11
203ZSRB	Data Engineer Pole BI (H/F)	69266	69100	Villeurbanne	69	84
203WVFC	Data engineer F/H (H/F)	59350	59000	Lille	59	32
203CKSG	Senior Software Engineer (H/F)	34172	34000	Montpellier	34	76
205KYXD	Analytics Engineer (H/F)	69387	69007	[NULL]	[NULL]	[NULL]
205KFHH	INGENIEUR DATA (H/F)	59009	59491	Villeneuve-d'Ascq	59	32
204MYWS	Data ingénieur (H/F)	69383	69003	[NULL]	[NULL]	[NULL]
204LFJH	Ingénieur Data Sénior (H/F)	29103	29800	Landerneau	29	53



SELECT 'offres avec code_postal' AS lib, COUNT(*) AS nb
FROM raw_france_travail_offres WHERE code_postal IS NOT NULL AND code_postal != '';
lib	nb
offres avec code_postal	124

SELECT SUBSTR(code_postal, 1, 2) AS dep_approx, COUNT(*) AS nb_offres
FROM raw_france_travail_offres
WHERE code_postal IS NOT NULL AND LENGTH(code_postal) >= 2
GROUP BY SUBSTR(code_postal, 1, 2)
ORDER BY nb_offres DESC
LIMIT 15;
dep_approx	nb_offres
92	17
75	10
59	10
69	9
44	9
79	6
31	6
29	5
93	4
67	4
94	3
91	3
76	3
21	3
13	3


SELECT 'etablissements dont siren dans unite_legale' AS lib, COUNT(*) AS nb
FROM raw_sirene_etablissement e
WHERE EXISTS (SELECT 1 FROM raw_sirene_unite_legale u WHERE u.siren = e.siren);
lib	nb
etablissements dont siren dans unite_legale	42 909 379



SELECT
  o.id AS offre_id,
  o.intitule,
  o.rome_code,
  o.type_contrat,
  o.date_creation,
  o.commune_code,
  o.code_postal,
  o.entreprise_nom,
  o.salaire_libelle,
  o.code_naf,
  c.code AS commune_code_geo,
  c.nom AS commune_nom,
  c.code_departement,
  c.code_region,
  d.nom AS departement_nom,
  r.nom AS region_nom
FROM raw_france_travail_offres o
LEFT JOIN raw_geo_communes c ON c.code = o.commune_code
LEFT JOIN raw_geo_departements d ON d.code = c.code_departement
LEFT JOIN raw_geo_regions r ON r.code = c.code_region
LIMIT 50;
offre_id	intitule	rome_code	type_contrat	date_creation	commune_code	code_postal	entreprise_nom	salaire_libelle	code_naf	commune_code_geo	commune_nom	code_departement	code_region	departement_nom	region_nom
205KZMV	Data Engineer - Data Solutions F/H (H/F)	M1811	CDI	2026-03-13T17:45:05.249Z	92051	92200	JCDECAUX SE	Annuel de 40000.0 Euros à 60000.0 Euros sur 12.0 mois	70.10Z	92051	Neuilly-sur-Seine	92	11	Hauts-de-Seine	Île-de-France
205JXPJ	Data Engineer IA Copilot Studio & Automatisation IA (H/F)	M1811	CDI	2026-03-13T09:20:12.468Z	[NULL]	[NULL]	BASSETTI GROUP	Annuel de 45000.0 Euros à 65000.0 Euros sur 12.0 mois	64.20Z	[NULL]	[NULL]	[NULL]	[NULL]	[NULL]	[NULL]
205JMZV	Data Engineer (H/F)	M1811	CDD	2026-03-12T16:35:34.521Z	59512	59100	[NULL]	Annuel de 35000.0 Euros à 45000.0 Euros sur 12.0 mois	70.22Z	59512	Roubaix	59	32	Nord	Hauts-de-France
205JGDF	Data Engineer GCP H/F	M1811	CDI	2026-03-12T14:36:24.107Z	31555	31000	ACEO TECH	Annuel de 35000.00 Euros à 50000.00 Euros sur 12 mois	62.02A	31555	Toulouse	31	76	Haute-Garonne	Occitanie
205HZRP	Data Engineer (H/F)	M1811	CDI	2026-03-12T12:18:07.319Z	13212	13012	OLYMPIQUE DE MARSEILLE	Annuel de 50000.0 Euros à 60000.0 Euros sur 13.0 mois	93.12Z	[NULL]	[NULL]	[NULL]	[NULL]	[NULL]	[NULL]
205HNXZ	#ForumOGCNice Lead DATA Engineer F/H	M1811	CDI	2026-03-12T08:46:39.264Z	06152	06560	FORUM OGC NICE / Stand SEPTEO	Annuel de 60000.00 Euros à 90000.00 Euros sur 12 mois	58.29C	06152	Valbonne	06	93	Alpes-Maritimes	Provence-Alpes-Côte d'Azur
205CQWS	Data engineer (H/F)	M1811	CDI	2026-03-09T13:54:12.855Z	92062	92800	INVIVOO	Annuel de 50000.0 Euros à 60000.0 Euros sur 12.0 mois	62.02A	92062	Puteaux	92	11	Hauts-de-Seine	Île-de-France
204XPYS	Data Analyst / Data Engineer Confirmé(e) F/H (H/F)	M1419	CDI	2026-03-04T19:06:09.099Z	75109	75009	TELYS	Annuel de 40000.0 Euros à 50000.0 Euros sur 12.0 mois	62.02A	[NULL]	[NULL]	[NULL]	[NULL]	[NULL]	[NULL]
204XHFX	DEVELOPPEUR REACT NATIVE / DATA ENGINEER H /F (H/F)	M1811	CDI	2026-03-04T15:38:53.102Z	75102	75002	SYNOPSIA INGENIERIE	Annuel de 40000.0 Euros à 50000.0 Euros sur 12.0 mois	62.02A	[NULL]	[NULL]	[NULL]	[NULL]	[NULL]	[NULL]
204QWDD	Data Engineer Confirmé F/H	M1811	CDI	2026-02-26T20:00:43.316Z	91477	91120	SOCOTEC TECHNICAL CONSULTING	Annuel de 48000.0 Euros à 58000.0 Euros sur 12.0 mois	71.12B	91477	Palaiseau	91	11	Essonne	Île-de-France
204PPSK	Data Engineer Confirmé F/H (H/F)	M1811	CDI	2026-02-25T17:58:30.439Z	67482	67000	SOCIETE POUR L'INFORMATIQUE INDUSTRIELLE	[NULL]	62.02A	67482	Strasbourg	67	44	Bas-Rhin	Grand Est
204LRTC	Expert TALEND-DATA Engineer (F/H) (H/F)	M1811	CDI	2026-02-23T15:54:18.574Z	29019	29200	LAITA	[NULL]	46.33Z	29019	Brest	29	53	Finistère	Bretagne
204JCST	Data Engineer / Scientist (H/F)	M1811	CDI	2026-02-19T16:58:32.428Z	94069	94410	AGENCE NATIONALE DE SANTE PUBLIQUE	Annuel de 34617.0 Euros à 69765.0 Euros sur 12.0 mois	84.12Z	94069	Saint-Maurice	94	11	Val-de-Marne	Île-de-France
203ZSRB	Data Engineer Pole BI (H/F)	M1811	CDI	2026-02-13T09:42:43.620Z	69266	69100	OPTEVEN ASSURANCES	Annuel de 45000.0 Euros sur 12.0 mois	65.12Z	69266	Villeurbanne	69	84	Rhône	Auvergne-Rhône-Alpes
203WVFC	Data engineer F/H (H/F)	M1811	CDI	2026-02-10T16:15:05.027Z	59350	59000	XPEHO	Annuel de 37000.0 Euros à 45000.0 Euros sur 12.0 mois	62.02A	59350	Lille	59	32	Nord	Hauts-de-France
203CKSG	Senior Software Engineer (H/F)	M1811	CDI	2026-01-23T17:24:28.733Z	34172	34000	e-Science Data Factory	Annuel de 55000.0 Euros à 70000.0 Euros sur 12.0 mois	62.01Z	34172	Montpellier	34	76	Hérault	Occitanie
205KYXD	Analytics Engineer (H/F)	M1811	CDI	2026-03-13T17:25:04.374Z	69387	69007	RUBIX FRANCE	Annuel de 36000.0 Euros à 38000.0 Euros sur 12.0 mois	46.69B	[NULL]	[NULL]	[NULL]	[NULL]	[NULL]	[NULL]
205KFHH	INGENIEUR DATA (H/F)	M1811	CDI	2026-03-13T11:10:25.932Z	59009	59491	PROJEX	Annuel de 35000.0 Euros à 45000.0 Euros sur 12.0 mois	71.12B	59009	Villeneuve-d'Ascq	59	32	Nord	Hauts-de-France
204MYWS	Data ingénieur (H/F)	M1811	CDI	2026-02-24T14:59:53.037Z	69383	69003	CONSORT FRANCE	Annuel de 40000.0 Euros à 50000.0 Euros sur 12.0 mois	62.02A	[NULL]	[NULL]	[NULL]	[NULL]	[NULL]	[NULL]
204LFJH	Ingénieur Data Sénior (H/F)	M1811	CDI	2026-02-23T11:52:11.307Z	29103	29800	[NULL]	Annuel de 45000.0 Euros à 50000.0 Euros sur 12.0 mois	70.22Z	29103	Landerneau	29	53	Finistère	Bretagne
204HZBP	Data ingénieur (H/F)	M1811	CDD	2026-02-19T15:58:25.822Z	75112	75012	COALLIA	Annuel de 37000.0 Euros à 42000.0 Euros sur 13.0 mois	87.90B	[NULL]	[NULL]	[NULL]	[NULL]	[NULL]	[NULL]
204GHCG	Data Scientist (H/F)	M1811	CDI	2026-02-18T12:18:10.548Z	[NULL]	[NULL]	SKARLETT ASSURANCES	[NULL]	66.22Z	[NULL]	[NULL]	[NULL]	[NULL]	[NULL]	[NULL]
203RFTJ	Chef de projets transverses - Data management (H/F)	M1811	CDI	2026-02-05T14:40:29.652Z	67028	67230	SOCOMEC	[NULL]	27.12Z	67028	Benfeld	67	44	Bas-Rhin	Grand Est
203HGJG	Ingénieur Data / ETL (H/F)	M1811	CDI	2026-01-28T14:24:03.748Z	21231	21000	ROSARA	Annuel de 34000.0 Euros à 44000.0 Euros sur 12.0 mois	70.22Z	21231	Dijon	21	27	Côte-d'Or	Bourgogne-Franche-Comté
203BVWN	Ingénieur(e) Data & IA (H/F)	M1811	CDI	2026-01-23T12:31:51.259Z	[NULL]	[NULL]	SMARTPUSH	Mensuel de 3333.33 Euros à 4166.67 Euros sur 12.0 mois	62.01Z	[NULL]	[NULL]	[NULL]	[NULL]	[NULL]	[NULL]
202SDYR	Ingénieur.e de recherche en Data Science Recherche Opérationnelle (H/F)	M1811	CDI	2026-01-16T10:33:00.624Z	93001	93300	SAINT GOBAIN RECHERCHE	Mensuel de 3576.93 Euros à 4230.77 Euros sur 13.0 mois	72.19Z	93001	Aubervilliers	93	11	Seine-Saint-Denis	Île-de-France
205HTQR	Responsable Donnees et Business Intelligence H/F	M1811	CDI	2026-03-12T10:35:47.434Z	38193	38080	MENWAY EMPLOI	Annuel de 40000.0 Euros à 50000.0 Euros sur 12.0 mois	78.20Z	38193	L'Isle-d'Abeau	38	84	Isère	Auvergne-Rhône-Alpes
205KDJY	Ingénieur / Ingénieure en développement big data, Data Scientist (H/F)	M1811	CDI	2026-03-13T10:54:10.524Z	38229	38240	B. LOTUS	Annuel de 38000.0 Euros à 40000.0 Euros sur 12.0 mois	62.01Z	38229	Meylan	38	84	Isère	Auvergne-Rhône-Alpes
205HGCY	Consultant Talend - Data Integration (H/F)	M1811	CDI	2026-03-11T17:00:07.049Z	13201	13001	SYNANTO AIX EN PROVENCE	Annuel de 40000.0 Euros à 45000.0 Euros sur 12.0 mois	62.02A	[NULL]	[NULL]	[NULL]	[NULL]	[NULL]	[NULL]
205GKZG	Architecte Expert Data PLM (H/F)	M1811	CDI	2026-03-11T11:05:11.422Z	92064	92210	PB SOLUTIONS	Annuel de 45000.0 Euros à 65000.0 Euros sur 12.0 mois	71.12B	92064	Saint-Cloud	92	11	Hauts-de-Seine	Île-de-France
205FRWR	Architecte GCP - Data & IA Hybride (H/F)	M1811	CDI	2026-03-10T16:50:59.416Z	[NULL]	[NULL]	INFORMATIS TECHNOLOGY SYSTM	Annuel de 70000.0 Euros à 90000.0 Euros sur 12.0 mois	62.02A	[NULL]	[NULL]	[NULL]	[NULL]	[NULL]	[NULL]
205DMVN	DATA-MANAGER (H/F)	M1811	CDD	2026-03-10T08:11:50.437Z	97411	97400	SITE FELIX GUYON	[NULL]	86.10Z	97411	Saint-Denis	974	04	La Réunion	La Réunion
205DJVY	Data manager (H/F)	M1811	MIS	2026-03-09T19:10:51.002Z	29019	29200	MANPOWER FRANCE	Annuel de 40000.0 Euros à 43850.0 Euros sur 12.0 mois	78.20Z	29019	Brest	29	53	Finistère	Bretagne
205CYRF	Développeur / Développeuse Big Data (H/F)	M1811	CDI	2026-03-09T15:47:56.645Z	93045	93260	AUBER TECH	Mensuel de 32000.0 Euros à 50000.0 Euros sur 12.0 mois	62.02A	93045	Les Lilas	93	11	Seine-Saint-Denis	Île-de-France
204ZBJV	Data Manager Junior (F/H) (H/F)	M1811	CDD	2026-03-05T18:13:02.518Z	49007	49000	ESSCA	Annuel de 30000.0 Euros à 35000.0 Euros sur 12.0 mois	85.42Z	49007	Angers	49	52	Maine-et-Loire	Pays de la Loire
204XGVF	Data Ingénieur (H/F)	M1811	CDI	2026-03-04T15:31:47.101Z	94071	94370	MARAGA	Mensuel de 2750.0 Euros sur 12.0 mois	93.11Z	94071	Sucy-en-Brie	94	11	Val-de-Marne	Île-de-France
204WDFZ	Ingénieur IA générative (H/F)	M1811	CDI	2026-03-03T17:18:11.058Z	06152	06560	SOCIETE POUR L'INFORMATIQUE INDUSTRIELLE	Annuel de 37000.0 Euros à 42000.0 Euros sur 12.0 mois	62.02A	06152	Valbonne	06	93	Alpes-Maritimes	Provence-Alpes-Côte d'Azur
204VSXG	Domaine Data Architecte Azure - Databricks (H/F)	M1811	CDI	2026-03-03T14:47:06.224Z	69290	69800	ALGOVIA	Annuel de 60000.0 Euros à 70000.0 Euros sur 12.0 mois	62.02A	69290	Saint-Priest	69	84	Rhône	Auvergne-Rhône-Alpes
204TZWH	Technicien Data Lead F/H (H/F)	M1811	CDI	2026-03-03T09:28:49.737Z	75114	75014	NOMADIA GROUP.	Annuel de 55000.0 Euros à 60000.0 Euros sur 12.0 mois	82.11Z	[NULL]	[NULL]	[NULL]	[NULL]	[NULL]	[NULL]
204QDGQ	Expert DATA (H/F)	M1811	CDI	2026-02-26T11:28:31.164Z	83073	83340	LECASUD	Annuel de 42000.00 Euros à 46000.00 Euros sur 12 mois	46.17A	83073	Le Luc	83	93	Var	Provence-Alpes-Côte d'Azur
204NQCJ	CONSULTANT SEMARCHY XDI - LYON (H/F)	M1811	CDI	2026-02-25T09:03:14.059Z	69382	69002	NEXT DECISION	[NULL]	62.02A	[NULL]	[NULL]	[NULL]	[NULL]	[NULL]	[NULL]
204JJTP	Ingénieur.e de recherche en Data Science & Recherche Opérationnel (H/F)	M1811	CDI	2026-02-20T08:05:18.832Z	93001	93300	COMPAGNIE DE SAINT-GOBAIN	Annuel de 46500.0 Euros à 55000.0 Euros sur 12.0 mois	70.10Z	93001	Aubervilliers	93	11	Seine-Saint-Denis	Île-de-France
204DTZQ	Architecte Logiciel Data (H/F)	M1811	CDI	2026-02-17T11:35:07.143Z	78208	78990	PB SOLUTIONS	Mensuel de 55000.0 Euros à 65000.0 Euros sur 12.0 mois	71.12B	78208	Élancourt	78	11	Yvelines	Île-de-France
204BLBX	Ingénieur DATA (H/F)	M1811	CDI	2026-02-13T15:05:55.321Z	69383	69003	 LEPTIS CONSULTING	[NULL]	62.02A	[NULL]	[NULL]	[NULL]	[NULL]	[NULL]	[NULL]
202XXBQ	Ingénieur data expertise et qualité des données (H/F)	M1811	CDD	2026-01-21T08:44:23.229Z	72181	72000	GIE SESAM VITALE	[NULL]	63.11Z	72181	Le Mans	72	52	Sarthe	Pays de la Loire
205KVSC	BI Developer (H/F)	M1811	CDI	2026-03-13T16:06:08.542Z	79137	79360	POUJOULAT	[NULL]	24.20Z	79137	Granzay-Gript	79	75	Deux-Sèvres	Nouvelle-Aquitaine
205BSNT	TAF 2026 Développeur Odoo / Python H/F (H/F)	M1811	CDI	2026-03-07T10:59:55.839Z	30259	30560	PESAGE MB	Mensuel de 2500.0 Euros à 3500.0 Euros sur 12.0 mois	47.91A	30259	Saint-Hilaire-de-Brethmas	30	76	Gard	Occitanie
204ZSST	Développeur .Net (F/H) - CDI (H/F)	M1811	CDI	2026-03-06T11:57:37.764Z	75117	75017	[NULL]	[NULL]	65.12Z	[NULL]	[NULL]	[NULL]	[NULL]	[NULL]	[NULL]
204WDJJ	Développeur ETL (H/F)	M1811	CDI	2026-03-03T17:21:28.513Z	79191	79000	AMILTONE	Annuel de 30000.0 Euros à 40000.0 Euros sur 12.0 mois	62.01Z	79191	Niort	79	75	Deux-Sèvres	Nouvelle-Aquitaine
204TRTN	Alternance - Développeur Power BI / Power Platform (H/F)	M1811	CDD	2026-03-02T17:52:05.526Z	76561	76410	BASF Agri Production S.A.S.	[NULL]	46.75Z	76561	Saint-Aubin-lès-Elbeuf	76	28	Seine-Maritime	Normandie


SELECT DISTINCT type_contrat FROM raw_france_travail_offres ORDER BY 1;
type_contrat
CDD
CDI
MIS

SELECT DISTINCT rome_code, rome_libelle FROM raw_france_travail_offres ORDER BY 1 LIMIT 30;
rome_code	rome_libelle
E1125	Concepteur / Conceptrice de jeux vidéo
F1112	Ingénieur / Ingénieure calcul et structure
H1204	Designer
H1206	Ingénieur / Ingénieure R&D en industrie
L1401	Sportif professionnel / Sportive professionnelle
M1405	Data scientist
M1419	Data analyst
M1702	Planneur / Planneuse stratégique
M1801	Administrateur / Administratrice de systèmes d'information (SI)
M1805	Développeur / Développeuse informatique
M1811	Data engineer
M1818	Ingénieur / Ingénieure d'étude informatique
M1842	Qualiticien / Qualiticienne logiciel en informatique
M1846	Ingénieur / Ingénieure Cybersécurité Datacenter
M1868	Architecte base de données
M1879	Ingénieur / Ingénieure Cloud computing