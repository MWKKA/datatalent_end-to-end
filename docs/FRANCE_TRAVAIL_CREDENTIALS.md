# France Travail — Identifiants et auth

## Sécurité

- **Ne jamais committer le fichier `.env`** (il est dans `.gitignore`).
- **Ne pas coller ton `client_secret`** dans un chat, un ticket ou un document partagé. Si tu l’as déjà fait, va sur francetravail.io → ton application → régénérer / changer la clé secrète.

---

# Où trouver ton Client ID et Client Secret (France Travail)

Tu as déjà rempli le formulaire (nom d’application, description, URL) sur **francetravail.io**. Les identifiants OAuth2 peuvent apparaître à plusieurs endroits selon la version du portail.

## 1. Juste après la création de l’application

- **Page de confirmation** : après avoir soumis le formulaire, une page affiche parfois une seule fois ton **Client ID** et ton **Client Secret** (parfois avec un bouton “Copier”). Si tu as fermé la page sans les noter, passe à l’étape 2.
- **Email** : vérifie la boîte mail associée à ton compte France Travail (y compris spams) : certains portails envoient les identifiants par email après validation.

## 2. Dans ton espace / tes applications

- Connecte-toi sur **https://francetravail.io**.
- Cherche un lien du type :
  - **« Mon espace »**, **« Espace partenaire »**, **« Tableau de bord »**
  - ou **« Mes applications »** / **« Mes projets »**
- Ouvre **ton application** (celle que tu as créée avec le nom “projet_scolaire” ou similaire).
- Sur la fiche de l’application, tu devrais voir :
  - **Identifiant client** (Client ID)
  - **Clé secrète** (Client Secret) — parfois masquée avec un bouton “Afficher” ou “Révéler”.

Les libellés peuvent varier : “Client ID”, “Identifiant”, “Application ID”, “Clé secrète”, “Secret”, “Client secret”.

## 3. Si tu ne trouves toujours pas

- **Support France Travail** : sur francetravail.io, regarde en bas de page ou dans la section “Aide” / “Contact” / “Support” pour un formulaire ou une adresse email dédiée aux **partenaires API** ou **développeurs**.
- **Documentation** : la doc technique (celle qui décrit le Client Credentials Flow que tu as partagée) est souvent liée depuis la même zone “Produits partagés” ou “Catalogue” ; une page “Premiers pas” ou “Obtenir ses identifiants” peut indiquer le chemin exact dans l’interface actuelle.

## 4. Une fois que tu les as

- Crée un fichier **`.env`** à la racine du projet (ne pas le committer).
- Renseigne :
  ```
  FRANCE_TRAVAIL_CLIENT_ID=ton_identifiant_ici
  FRANCE_TRAVAIL_CLIENT_SECRET=ta_cle_secrete_ici
  ```
- Teste avec : `python scripts/ingestion/france_travail_auth.py`

Si l’interface a changé et que tu vois des menus différents (noms de liens, onglets), décris ce que tu vois (ex. “Je suis sur Mon espace et j’ai seulement…”) pour qu’on cible la bonne section.

---

## URL d’auth et scopes

- **URL du token** : `POST https://authentification-partenaire.francetravail.io/connexion/oauth2/access_token?realm=/partenaire`
- **Corps** : `application/x-www-form-urlencoded` avec `grant_type=client_credentials`, `client_id`, `client_secret`, `scope=...`

Scopes selon l’API :
- **Offres d’emploi v2** (pour le projet DataTalent) : `api_offresdemploiv2 o2dsoffre`
- **La Bonne Boite v2** : `api_labonneboitev2 search office`

Si ton application a été créée depuis le catalogue « La Bonne Boite », tu as peut‑être uniquement ce scope. Pour utiliser l’API **Offres d’emploi**, ajoute le produit « Offres d’emploi v2 » à ton application dans le catalogue francetravail.io (ou crée une seconde application dédiée aux offres).
