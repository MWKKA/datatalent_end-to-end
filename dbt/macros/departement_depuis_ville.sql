{# Repli : libellé ville (city_std) → code département quand le CP ne suffit pas. #}
{% macro departement_depuis_ville(col) %}
CASE
  WHEN {{ col }} IS NULL THEN NULL
  WHEN REGEXP_CONTAINS(
    LOWER(TRIM({{ col }})),
    r'^(lille|lille-nord|roubaix|tourcoing|dunkerque|valenciennes|douai|calais|boulogne-sur-mer|arras|lens|bethune|henin-beaumont|marcq-en-baroeul|croix|wasquehal|lambersart|haubourdin|ronchin|wattignies|villeneuve-dascq|la-madeleine)$'
  ) THEN '59'
  WHEN REGEXP_CONTAINS(LOWER(TRIM({{ col }})), r'^(paris)$') THEN '75'
  WHEN REGEXP_CONTAINS(
    LOWER(TRIM({{ col }})),
    r'^(lyon|villeurbanne|venissieux|saint-priest|caluire-et-cuire|echirolles)$'
  ) THEN '69'
  WHEN REGEXP_CONTAINS(
    LOWER(TRIM({{ col }})),
    r'^(marseille|aix-en-provence|aubagne|arles)$'
  ) THEN '13'
  WHEN REGEXP_CONTAINS(LOWER(TRIM({{ col }})), r'^(toulouse|colomiers)$') THEN '31'
  WHEN REGEXP_CONTAINS(
    LOWER(TRIM({{ col }})),
    r'^(nice|antibes|cannes|grasse)$'
  ) THEN '06'
  WHEN REGEXP_CONTAINS(
    LOWER(TRIM({{ col }})),
    r'^(nantes|saint-nazaire|reze|pont-rousseau)$'
  ) THEN '44'
  WHEN REGEXP_CONTAINS(
    LOWER(TRIM({{ col }})),
    r'^(strasbourg|mulhouse|colmar|haguenau)$'
  ) THEN '67'
  WHEN REGEXP_CONTAINS(
    LOWER(TRIM({{ col }})),
    r'^(nancy|vandoeuvre-les-nancy|luneville)$'
  ) THEN '54'
  WHEN REGEXP_CONTAINS(
    LOWER(TRIM({{ col }})),
    r'^(metz|thionville|forbach)$'
  ) THEN '57'
  WHEN REGEXP_CONTAINS(
    LOWER(TRIM({{ col }})),
    r'^(montpellier|beziers|sete)$'
  ) THEN '34'
  WHEN REGEXP_CONTAINS(
    LOWER(TRIM({{ col }})),
    r'^(bordeaux|merignac|pessac|talence)$'
  ) THEN '33'
  WHEN REGEXP_CONTAINS(
    LOWER(TRIM({{ col }})),
    r'^(rennes|saint-malo)$'
  ) THEN '35'
  WHEN REGEXP_CONTAINS(
    LOWER(TRIM({{ col }})),
    r'^(brest|quimper)$'
  ) THEN '29'
  WHEN REGEXP_CONTAINS(
    LOWER(TRIM({{ col }})),
    r'^(lorient|vannes|lanester)$'
  ) THEN '56'
  WHEN REGEXP_CONTAINS(
    LOWER(TRIM({{ col }})),
    r'^(reims|chalons-en-champagne|epernay)$'
  ) THEN '51'
  WHEN REGEXP_CONTAINS(
    LOWER(TRIM({{ col }})),
    r'^(le havre|rouen|dieppe)$'
  ) THEN '76'
  WHEN REGEXP_CONTAINS(
    LOWER(TRIM({{ col }})),
    r'^(saint-etienne|roanne)$'
  ) THEN '42'
  WHEN REGEXP_CONTAINS(
    LOWER(TRIM({{ col }})),
    r'^(toulon|la seyne-sur-mer|hyeres)$'
  ) THEN '83'
  WHEN REGEXP_CONTAINS(LOWER(TRIM({{ col }})), r'^(grenoble)$') THEN '38'
  WHEN REGEXP_CONTAINS(LOWER(TRIM({{ col }})), r'^(dijon|beaune)$') THEN '21'
  WHEN REGEXP_CONTAINS(
    LOWER(TRIM({{ col }})),
    r'^(angers|cholet|saumur)$'
  ) THEN '49'
  WHEN REGEXP_CONTAINS(LOWER(TRIM({{ col }})), r'^(le mans|la fleche)$') THEN '72'
  WHEN REGEXP_CONTAINS(LOWER(TRIM({{ col }})), r'^(nimes|ales)$') THEN '30'
  WHEN REGEXP_CONTAINS(LOWER(TRIM({{ col }})), r'^(limoges)$') THEN '87'
  WHEN REGEXP_CONTAINS(
    LOWER(TRIM({{ col }})),
    r'^(clermont-ferrand|vichy)$'
  ) THEN '63'
  WHEN REGEXP_CONTAINS(
    LOWER(TRIM({{ col }})),
    r'^(tours|joue-les-tours|saint-cyr-sur-loire)$'
  ) THEN '37'
  WHEN REGEXP_CONTAINS(
    LOWER(TRIM({{ col }})),
    r'^(amiens|abbeville)$'
  ) THEN '80'
  WHEN REGEXP_CONTAINS(LOWER(TRIM({{ col }})), r'^(perpignan)$') THEN '66'
  WHEN REGEXP_CONTAINS(LOWER(TRIM({{ col }})), r'^(besancon)$') THEN '25'
  WHEN REGEXP_CONTAINS(
    LOWER(TRIM({{ col }})),
    r'^(boulogne-billancourt|nanterre|creteil|issy-les-moulineaux|levallois-perret|montreuil|saint-denis|argenteuil|versailles|colombes|asnieres-sur-seine|courbevoie|rueil-malmaison|champigny-sur-marne|drancy|aubervilliers|sarcelles|neuilly-sur-seine|clichy|vitry-sur-seine)$'
  ) THEN '92'
  WHEN REGEXP_CONTAINS(LOWER(TRIM({{ col }})), r'^(orleans|blois)$') THEN '45'
  WHEN REGEXP_CONTAINS(
    LOWER(TRIM({{ col }})),
    r'^(caen|cherbourg|herouville-saint-clair)$'
  ) THEN '14'
  ELSE NULL
END
{% endmacro %}
