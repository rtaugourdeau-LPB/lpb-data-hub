import streamlit as st

# ============================================================
# ⚙️ CONFIG GENERALE
# ============================================================

st.set_page_config(
    page_title="Data Hub LPB",
    page_icon="🛠️",
    layout="wide",
)

import os
import csv
import psycopg2
import pandas as pd
import requests
from io import BytesIO
import zipfile
import json
from datetime import datetime

TODAY_STR = datetime.now().strftime("%Y-%m-%d")

# ============================================================
# 🔐 CONFIG NOTION (via st.secrets)
# ============================================================

NOTION_TOKEN     = st.secrets["notion"]["token"]
DB_BAC_SABLE     = st.secrets["notion"]["db_bac_sable"]
DB_NOTION_PROJET = st.secrets["notion"]["db_notion_projet"]

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Notion-Version": "2022-06-28",
    "Content-Type": "application/json",
}

# ============================================================
# 🔐 CONFIG POSTGRESQL BACK-OFFICE (via st.secrets)
# ============================================================

PGHOST     = st.secrets["postgres"]["host"]
PGPORT     = st.secrets["postgres"]["port"]
PGDATABASE = st.secrets["postgres"]["db"]
PGUSER     = st.secrets["postgres"]["user"]
PGPASSWORD = st.secrets["postgres"]["password"]
PGSSLMODE  = st.secrets["postgres"]["sslmode"]

# ============================================================
# 🧠 ETAT GLOBAL STREAMLIT
# ============================================================

# API Notion
if "api_df" not in st.session_state:
    st.session_state.api_df = None

# Import ZIP/CSV
if "imp_df" not in st.session_state:
    st.session_state.imp_df = None
if "imp_filename" not in st.session_state:
    st.session_state.imp_filename = None

# Back-office (PostgreSQL)
if "bo_df" not in st.session_state:
    st.session_state.bo_df = None
if "bo_table_id" not in st.session_state:
    st.session_state.bo_table_id = None  # full_name = schema.table
if "bo_tables_df" not in st.session_state:
    st.session_state.bo_tables_df = None

# ============================================================
# 🔧 FONCTIONS COMMUNES
# ============================================================

def df_to_excel_bytes(df: pd.DataFrame, sheet_name: str = "Data"):
    """
    Export propre vers Excel :
    - Table structurée avec style
    - Largeur de colonnes auto (avec petite marge)
    - Ligne d’en-tête figée
    - Filtres automatiques
    - Hyperliens désactivés (évite les fichiers corrompus)
    - Certaines colonnes forcées en texte (id, url, email, codes…)
    """
    out = BytesIO()

    # -------- 1) Préparation des données pour Excel --------
    df_export = df.copy()

    # Colonnes que l'on veut plutôt en texte (par nom)
    text_col_keywords = (
        "url", "link", "href", "file", "image", "img", "path",
        "uuid", "token", "hash", "id", "code", "ref", "reference",
        "email", "mail", "phone", "tel"
    )

    for col in df_export.columns:
        s = df_export[col]

        # On ne touche pas aux nombres, booléens, dates → Excel garde les bons types
        if not (pd.api.types.is_object_dtype(s) or pd.api.types.is_string_dtype(s)):
            continue

        col_lower = str(col).lower()
        force_text = any(k in col_lower for k in text_col_keywords)

        # On passe en string pour analyser le contenu
        s_str = s.astype(str)

        # Heuristiques de contenu
        url_like   = s_str.str.startswith(("http://", "https://", "www."))
        email_like = s_str.str.contains("@", na=False)
        long_like  = s_str.str.len() > 120  # gros blobs de texte / json / url

        if url_like.mean() > 0.1 or \
           email_like.mean() > 0.3 or \
           long_like.mean() > 0.3:
            force_text = True

        def to_excel_safe(v):
            if pd.isna(v):
                return None
            s_loc = str(v)

            # Évite les formules Excel involontaires (=, +, -, @)
            if s_loc.startswith(("=", "+", "-", "@")):
                s_loc = "'" + s_loc

            # Évite les hyperliens auto, notamment très longs
            if s_loc.startswith(("http://", "https://", "www.")):
                s_loc = "'" + s_loc

            return s_loc

        if force_text:
            # Texte pur pour toute la colonne (Excel affiche 123, mais c’est du texte)
            df_export[col] = s_str.map(to_excel_safe)
        else:
            # On ne force pas en texte, mais on neutralise formules & URLs trop longues
            def sanitize(v):
                if pd.isna(v):
                    return None
                s_local = v
                if isinstance(s_local, str):
                    if s_local.startswith(("=", "+", "-", "@")):
                        s_local = "'" + s_local
                    if s_local.startswith(("http://", "https://", "www.")) and len(s_local) > 255:
                        s_local = "'" + s_local
                return s_local

            df_export[col] = s.map(sanitize)

    # -------- 2) Écriture Excel + mise en forme --------
    with pd.ExcelWriter(out, engine="xlsxwriter") as writer:
        df_export.to_excel(writer, index=False, sheet_name=sheet_name)

        workbook  = writer.book
        worksheet = writer.sheets[sheet_name]

        n_rows, n_cols = df_export.shape

        # Largeur des colonnes automatique + petite marge
        for col_idx, col_name in enumerate(df_export.columns):
            col_series = df_export[col_name].astype(str).fillna("")
            sample = col_series.head(500)
            max_len_value = sample.map(len).max() if not sample.empty else 0
            max_len = max(len(str(col_name)), max_len_value)

            # ajustement + petite marge de 5
            base_width = min(max_len + 2, 60)
            width = base_width + 4
            worksheet.set_column(col_idx, col_idx, width)

        # Freeze de la ligne d’en-tête
        worksheet.freeze_panes(1, 0)

        # Table structurée Excel + filtres
        if n_cols > 0:
            table_columns = [{"header": str(col)} for col in df_export.columns]
            worksheet.add_table(
                0, 0,
                n_rows, n_cols - 1,
                {
                    "columns": table_columns,
                    "style": "Table Style Medium 2",
                    "autofilter": True,
                },
            )

        # Zoom confortable
        worksheet.set_zoom(100)

    return out.getvalue()

def df_to_csv_bytes(df: pd.DataFrame):
    """
    Export CSV simple, compatible Excel FR :
    - Séparateur ;
    - Encodage UTF-8 avec BOM (accents OK)
    - Pas de paramètres avancés => très robuste
    """
    csv_str = df.to_csv(
        index=False,
        sep=";",
        encoding="utf-8-sig"
    )
    return csv_str.encode("utf-8-sig")

# ---------- PARTIE API NOTION ----------

def get_database_schema(db_id: str):
    """Récupère la liste (nom, type) de toutes les propriétés de la base, dans l'ordre API."""
    url = f"https://api.notion.com/v1/databases/{db_id}"
    r = requests.get(url, headers=HEADERS)
    if r.status_code != 200:
        raise Exception(f"Erreur schéma Notion : {r.text}")
    data = r.json()
    props = data.get("properties", {})
    return [(name, definition.get("type")) for name, definition in props.items()]


def get_all_rows(db_id: str):
    """Récupère toutes les pages d'une base Notion (pagination)."""
    url = f"https://api.notion.com/v1/databases/{db_id}/query"
    payload = {"page_size": 100}
    results = []

    while True:
        r = requests.post(url, headers=HEADERS, json=payload)
        if r.status_code != 200:
            raise Exception(f"Erreur Notion : {r.text}")

        data = r.json()
        results.extend(data.get("results", []))

        if not data.get("has_more"):
            break
        payload["start_cursor"] = data.get("next_cursor")

    return results


def _join_plain_text(rich_list):
    if not rich_list:
        return None
    try:
        return "".join([r.get("plain_text", "") for r in rich_list]) or None
    except Exception:
        return None


def parse_property_value(prop: dict):
    """
    Convertit un property Notion -> valeur simple.
    On couvre au max les types, le reste en JSON pour ne rien perdre.
    """
    if not isinstance(prop, dict):
        return prop

    t = prop.get("type")
    value = prop.get(t)

    # texte
    if t in ("title", "rich_text"):
        return _join_plain_text(value)

    # select / status
    if t in ("select", "status"):
        return value["name"] if value else None

    # multi_select
    if t == "multi_select":
        if not value:
            return None
        return ", ".join(v.get("name", "") for v in value)

    # number
    if t == "number":
        return value

    # date
    if t == "date":
        if not value:
            return None
        start = value.get("start")
        end = value.get("end")
        return f"{start} → {end}" if end else start

    # checkbox
    if t == "checkbox":
        return value

    # url / email / phone
    if t in ("url", "email", "phone_number"):
        return value

    # people
    if t == "people":
        if not value:
            return None
        names = [p.get("name") or p.get("id") for p in value if p.get("name") or p.get("id")]
        return ", ".join(names) if names else None

    # files
    if t == "files":
        if not value:
            return None
        parts = []
        for f in value:
            name = f.get("name")
            f_type = f.get("type")
            url = None
            if f_type == "file":
                url = f.get("file", {}).get("url")
            elif f_type == "external":
                url = f.get("external", {}).get("url")
            if name and url:
                parts.append(f"{name} ({url})")
            elif url:
                parts.append(url)
            elif name:
                parts.append(name)
        return ", ".join(parts) if parts else None

    # formula
    if t == "formula":
        if not value:
            return None
        f_type = value.get("type")
        if f_type in ("string", "number", "boolean"):
            return value.get(f_type)
        if f_type == "date":
            d = value.get("date")
            if not d:
                return None
            s = d.get("start")
            e = d.get("end")
            return f"{s} → {e}" if e else s
        return json.dumps(value, ensure_ascii=False)

    # relation
    if t == "relation":
        if not value:
            return None
        return ", ".join(rel.get("id", "") for rel in value) or None

    # rollup
    if t == "rollup":
        if not value:
            return None
        r_type = value.get("type")
        if r_type in ("number", "boolean"):
            return value.get(r_type)
        if r_type == "date":
            d = value.get("date")
            if not d:
                return None
            s = d.get("start")
            e = d.get("end")
            return f"{s} → {e}" if e else s
        if r_type == "array":
            items_txt = []
            for it in value.get("array", []):
                it_type = it.get("type")
                it_val = it.get(it_type)
                if isinstance(it_val, list):
                    txt = _join_plain_text(it_val)
                    if txt:
                        items_txt.append(txt)
                elif isinstance(it_val, dict):
                    if "name" in it_val:
                        items_txt.append(it_val["name"])
                else:
                    if it_val is not None:
                        items_txt.append(str(it_val))
            return ", ".join(items_txt) if items_txt else None
        return json.dumps(value, ensure_ascii=False)

    # created_time / last_edited_time
    if t in ("created_time", "last_edited_time"):
        return value

    # created_by / last_edited_by
    if t in ("created_by", "last_edited_by"):
        if not value:
            return None
        return value.get("name") or value.get("id")

    # unique_id
    if t == "unique_id":
        if not value:
            return None
        prefix = value.get("prefix") or ""
        number = value.get("number")
        if prefix and number is not None:
            return f"{prefix}-{number}"
        if number is not None:
            return str(number)
        return json.dumps(value, ensure_ascii=False)

    # fallback : JSON
    return json.dumps({k: v for k, v in prop.items() if k != "id"}, ensure_ascii=False)


def notion_to_df(results, schema_order):
    """Construit un DataFrame API Notion avec une colonne par propriété, ordre = schéma."""
    prop_names = [name for name, _ in schema_order]
    rows = []
    for page in results:
        props = page.get("properties", {})
        row = {}
        for name in prop_names:
            prop_value = props.get(name)
            row[name] = parse_property_value(prop_value) if prop_value is not None else None
        rows.append(row)
    return pd.DataFrame(rows, columns=prop_names)

# ---------- PARTIE IMPORT ZIP / CSV ----------

def extract_csv_recursive(zip_file):
    """
    Ouvre un ZIP Notion (ZIP dans ZIP possible) et renvoie
    une liste de (nom, DataFrame) pour tous les CSV trouvés.
    """
    csv_files = []

    def explore(zip_bytes):
        z = zipfile.ZipFile(zip_bytes)
        for name in z.namelist():
            if name.lower().endswith(".csv"):
                with z.open(name) as f:
                    csv_files.append((name, pd.read_csv(f)))
            elif name.lower().endswith(".zip"):
                nested_bytes = BytesIO(z.read(name))
                explore(nested_bytes)

    explore(zip_file)
    return csv_files

# ---------- PARTIE POSTGRESQL (BACK-OFFICE) ----------

@st.cache_resource(show_spinner=False)
def get_pg_connection():
    conn = psycopg2.connect(
        host=PGHOST,
        port=PGPORT,
        dbname=PGDATABASE,
        user=PGUSER,
        password=PGPASSWORD,
        sslmode=PGSSLMODE,
    )
    return conn


@st.cache_data(show_spinner=False)
def list_pg_tables():
    """
    Renvoie un DataFrame (table_schema, table_name) pour toutes les tables utilisateur.
    """
    conn = get_pg_connection()
    query = """
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
          AND table_schema NOT IN ('pg_catalog', 'information_schema')
        ORDER BY table_schema, table_name;
    """
    return pd.read_sql(query, conn)


@st.cache_data(show_spinner=False)
def read_pg_table(schema: str, table: str) -> pd.DataFrame:
    """
    Lit TOUTE la table PostgreSQL (sans LIMIT).
    ⚠️ Attention aux très grosses tables.
    """
    conn = get_pg_connection()
    query = f'SELECT * FROM "{schema}"."{table}"'
    return pd.read_sql(query, conn)

# ============================================================
# 🧱 MENU LATERAL
# ============================================================

with st.sidebar:
    section = st.radio(
        "🛠️ Data Hub LPB",
        ["Notion", "Back-office"],
    )

# ============================================================
# 🏠 RUBRIQUE NOTION
# ============================================================

if section == "Notion":
    st.title("🏠 Notion")
    st.caption("Connexion API Notion ou import d’un export Notion (ZIP/CSV).")

    tab_api, tab_import = st.tabs(["Mode API", "Mode Export (ZIP/CSV)"])

    # ---------- Mode API ----------
    with tab_api:
        st.subheader("Connexion directe à Notion (API)")

        base_choisie = st.radio(
            "Sélection",
            ["Notion projet", "Bac à sable"],
            horizontal=True,
        )

        db_id = DB_BAC_SABLE if base_choisie == "Bac à sable" else DB_NOTION_PROJET

        if st.button("⚙️ Charger via l’API Notion"):
            try:
                with st.spinner("Récupération du schéma Notion…"):
                    schema_order = get_database_schema(db_id)

                with st.spinner("Récupération de toutes les lignes…"):
                    results = get_all_rows(db_id)
                    if not results:
                        st.info("Aucune page trouvée dans cette base.")
                        st.session_state.api_df = None
                    else:
                        df_api = notion_to_df(results, schema_order)
                        st.session_state.api_df = df_api
                        st.session_state.pop("api_cols_multiselect", None)

            except Exception as e:
                st.error(f"❌ Erreur API Notion : {e}")

        df_api = st.session_state.api_df

        if df_api is None or df_api.empty:
            st.info("Aucune donnée API chargée pour le moment. Clique sur le bouton ci-dessus.")
        else:
            st.success(f"{len(df_api)} lignes • {len(df_api.columns)} colonnes")

            all_cols = list(df_api.columns)

            if "api_cols_multiselect" not in st.session_state:
                st.session_state.api_cols_multiselect = all_cols.copy()
            else:
                st.session_state.api_cols_multiselect = [
                    c for c in st.session_state.api_cols_multiselect if c in all_cols
                ]

            st.caption("Colonnes à afficher (tu peux taper pour filtrer les noms).")

            c_sel1, c_sel2 = st.columns(2)
            with c_sel1:
                if st.button("✅ Tout sélectionner", key="api_select_all"):
                    st.session_state.api_cols_multiselect = all_cols.copy()
            with c_sel2:
                if st.button("🚫 Tout désélectionner", key="api_select_none"):
                    st.session_state.api_cols_multiselect = []

            st.multiselect(
                "Colonnes à afficher",
                options=all_cols,
                key="api_cols_multiselect",
            )

            selected_cols = st.session_state.api_cols_multiselect

            if selected_cols:
                df_view = df_api[selected_cols].copy()
            else:
                df_view = df_api.iloc[:, []].copy()

            edited_df = st.data_editor(
                df_view,
                use_container_width=True,
                height=550,
                hide_index=True,
                key="api_table",
            )

            base_slug = base_choisie.replace(" ", "_").lower()
            csv_name = f"{TODAY_STR}_notion_api_{base_slug}.csv"
            xlsx_name = f"{TODAY_STR}_notion_api_{base_slug}.xlsx"

            csv_bytes = df_to_csv_bytes(edited_df)
            excel_bytes = df_to_excel_bytes(edited_df, sheet_name=base_choisie[:31])

            c1, c2 = st.columns(2)
            with c1:
                st.download_button(
                    "📥 Télécharger CSV",
                    data=csv_bytes,
                    file_name=csv_name,
                    mime="text/csv",
                )
            with c2:
                st.download_button(
                    "📥 Télécharger Excel",
                    data=excel_bytes,
                    file_name=xlsx_name,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )

    # ---------- Mode Import ZIP/CSV ----------
    with tab_import:
        st.subheader("Import d’un export Notion (ZIP/CSV)")

        uploaded = st.file_uploader(
            "Dépose ici ton export Notion : soit le ZIP principal, soit un CSV",
            type=["zip", "csv"],
            accept_multiple_files=False,
        )

        if uploaded is not None:
            try:
                if uploaded.name.lower().endswith(".csv"):
                    df_import = pd.read_csv(uploaded)
                    chosen_name = uploaded.name
                else:
                    with st.spinner("Lecture du ZIP Notion (ZIP → ZIP → CSV)…"):
                        csv_list = extract_csv_recursive(uploaded)

                    if not csv_list:
                        st.error("Aucun CSV trouvé dans ce ZIP.")
                        df_import = None
                        chosen_name = None
                    else:
                        all_candidates = [
                            c for c in csv_list if c[0].lower().endswith("_all.csv")
                        ]
                        if len(all_candidates) == 1:
                            chosen_name, df_import = all_candidates[0]
                        else:
                            chosen_name = st.selectbox(
                                "Plusieurs CSV trouvés, choisis celui à afficher :",
                                [name for name, _ in csv_list],
                            )
                            df_import = next(df for (name, df) in csv_list if name == chosen_name)

                if df_import is not None:
                    st.session_state.imp_df = df_import
                    st.session_state.imp_filename = chosen_name
                    st.session_state.pop("imp_cols_multiselect", None)

            except Exception as e:
                st.error(f"❌ Erreur lors de la lecture de l’export : {e}")

        df_import = st.session_state.imp_df
        chosen_name = st.session_state.imp_filename

        if df_import is None:
            st.info("Glisse ton fichier ici pour commencer.")
        else:
            st.success(
                f"Fichier chargé : **{chosen_name}** "
                f"({len(df_import)} lignes × {len(df_import.columns)} colonnes)"
            )

            all_cols_imp = list(df_import.columns)

            if "imp_cols_multiselect" not in st.session_state:
                st.session_state.imp_cols_multiselect = all_cols_imp.copy()
            else:
                st.session_state.imp_cols_multiselect = [
                    c for c in st.session_state.imp_cols_multiselect if c in all_cols_imp
                ]

            st.caption("Colonnes à afficher (tu peux taper pour filtrer les noms).")

            c2_sel1, c2_sel2 = st.columns(2)
            with c2_sel1:
                if st.button("✅ Tout sélectionner", key="imp_select_all"):
                    st.session_state.imp_cols_multiselect = all_cols_imp.copy()
            with c2_sel2:
                if st.button("🚫 Tout désélectionner", key="imp_select_none"):
                    st.session_state.imp_cols_multiselect = []

            st.multiselect(
                "Colonnes à afficher",
                options=all_cols_imp,
                key="imp_cols_multiselect",
            )

            selected_cols_imp = st.session_state.imp_cols_multiselect

            if selected_cols_imp:
                df_view_imp = df_import[selected_cols_imp].copy()
            else:
                df_view_imp = df_import.iloc[:, []].copy()

            edited_df_imp = st.data_editor(
                df_view_imp,
                use_container_width=True,
                height=550,
                hide_index=True,
                key="imp_table",
            )

            base_name = (chosen_name or "export").replace(".csv", "")
            csv_name_imp = f"{TODAY_STR}_notion_export.csv"
            xlsx_name_imp = f"{TODAY_STR}_notion_export.xlsx"

            csv_bytes_imp = df_to_csv_bytes(edited_df_imp)
            excel_bytes_imp = df_to_excel_bytes(edited_df_imp, sheet_name="ExportNotion")

            c1, c2 = st.columns(2)
            with c1:
                st.download_button(
                    "📥 Télécharger CSV",
                    data=csv_bytes_imp,
                    file_name=csv_name_imp,
                    mime="text/csv",
                )
            with c2:
                st.download_button(
                    "📥 Télécharger Excel",
                    data=excel_bytes_imp,
                    file_name=xlsx_name_imp,
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                )

# ============================================================
# 🗄 RUBRIQUE BACK-OFFICE (POSTGRESQL)
# ============================================================

if section == "Back-office":
    st.title("🗄 Back-office")
    st.caption("Connexion en lecture au read-replica PostgreSQL de production.")
    
    # Connexion & liste des tables
    try:
        with st.spinner("Connexion à PostgreSQL et récupération des tables…"):
            tables_df = list_pg_tables()
            st.session_state.bo_tables_df = tables_df
        st.success("✅ Connexion PostgreSQL réussie")
    except Exception as e:
        st.error(f"❌ Erreur de connexion PostgreSQL : {e}")
        st.stop()

    tables_df = st.session_state.bo_tables_df
    st.image("assets/schema_bo.png")
    if tables_df is None or tables_df.empty:
        st.info("Aucune table utilisateur trouvée dans cette base.")
        st.stop()

    # full_name = schema.table (interne)
    tables_df["full_name"] = tables_df["table_schema"] + "." + tables_df["table_name"]

    # label affiché : sans "public." pour le schéma public
    def make_label(row):
        if row["table_schema"] == "public":
            return row["table_name"]
        else:
            return row["table_schema"] + "." + row["table_name"]

    tables_df["label"] = tables_df.apply(make_label, axis=1)

    table_labels = tables_df["label"].tolist()
    full_names = tables_df["full_name"].tolist()
    label_to_fullname = dict(zip(table_labels, full_names))

    default_full = st.session_state.bo_table_id or full_names[0]
    if default_full in full_names:
        default_label = tables_df.loc[tables_df["full_name"] == default_full, "label"].iloc[0]
    else:
        default_label = table_labels[0]

    selected_label = st.selectbox(
        "Table PostgreSQL",
        options=table_labels,
        index=table_labels.index(default_label),
        help="Tu peux taper ici pour filtrer les tables en temps réel.",
    )

    selected_full = label_to_fullname[selected_label]
    st.session_state.bo_table_id = selected_full
    schema, table = selected_full.split(".", 1)

    if st.button("⚙️ Charger via l'API Postgre"):
        try:
            with st.spinner(f"Lecture de {schema}.{table} (toutes les lignes)…"):
                df_bo = read_pg_table(schema, table)
                st.session_state.bo_df = df_bo
                st.session_state.pop("bo_cols_multiselect", None)
        except Exception as e:
            st.error(f"❌ Erreur lors de la lecture de la table : {e}")

    df_bo = st.session_state.bo_df

    if df_bo is None or df_bo.empty:
        st.info("Aucune donnée chargée pour le moment. Choisis une table puis clique sur le bouton.")
    else:
        st.success(f"{len(df_bo)} lignes chargées • {len(df_bo.columns)} colonnes")

        all_cols_bo = list(df_bo.columns)

        if "bo_cols_multiselect" not in st.session_state:
            st.session_state.bo_cols_multiselect = all_cols_bo.copy()
        else:
            st.session_state.bo_cols_multiselect = [
                c for c in st.session_state.bo_cols_multiselect if c in all_cols_bo
            ]

        st.caption("Colonnes à afficher (tu peux taper pour filtrer les noms).")

        b1, b2 = st.columns(2)
        with b1:
            if st.button("✅ Tout sélectionner", key="bo_select_all"):
                st.session_state.bo_cols_multiselect = all_cols_bo.copy()
        with b2:
            if st.button("🚫 Tout désélectionner", key="bo_select_none"):
                st.session_state.bo_cols_multiselect = []

        st.multiselect(
            "Colonnes à afficher",
            options=all_cols_bo,
            key="bo_cols_multiselect",
        )

        selected_cols_bo = st.session_state.bo_cols_multiselect

        if selected_cols_bo:
            df_bo_view = df_bo[selected_cols_bo].copy()
        else:
            df_bo_view = df_bo.iloc[:, []].copy()

        edited_bo = st.data_editor(
            df_bo_view,
            use_container_width=True,
            height=550,
            hide_index=True,
            key="bo_table",
        )

        st.markdown("### 💾 Export (vue affichée)")
        safe_table_name = selected_label.replace(".", "_")
        csv_name_bo = f"{TODAY_STR}_{safe_table_name}.csv"
        xlsx_name_bo = f"{TODAY_STR}_{safe_table_name}.xlsx"

        csv_bytes_bo = df_to_csv_bytes(edited_bo)
        excel_bytes_bo = df_to_excel_bytes(edited_bo, sheet_name=safe_table_name[:31])

        c1, c2 = st.columns(2)
        with c1:
            st.download_button(
                "📥 Télécharger CSV",
                data=csv_bytes_bo,
                file_name=csv_name_bo,
                mime="text/csv",
            )
        with c2:
            st.download_button(
                "📥 Télécharger Excel",
                data=excel_bytes_bo,
                file_name=xlsx_name_bo,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",

            )
