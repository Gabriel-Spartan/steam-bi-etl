#!/usr/bin/env python3
"""
dim_user_country_imputation_kmeans.py

Objetivo
--------
Completar los usuarios de dim_user que no tienen country_key mediante un
proceso reproducible que:

1) Extrae dim_user, dim_country y la moneda de mercado por país desde SQL Server.
2) Usa OpenRefine solo como apoyo exploratorio rápido (conteos/facets), no como
   motor operativo de imputación.
3) Construye una característica combinada pais_moneda según la moneda dominante
   con la que Steam publica precios por país en fact_game_price_period y
   fact_game_price_snapshot.
4) Aplica codificación One-Hot a variables categóricas.
5) Ejecuta K-means con 4 clusters.
6) Imputa country_key faltante usando el país dominante de cada cluster.
7) Genera archivos de salida y, opcionalmente, actualiza dim_user en SQL Server.

Uso recomendado
----------------
Desde la raíz del proyecto steam-bi-etl:

python src/analytics/dim_user_country_imputation_kmeans.py --use-project-db --output-dir artifacts/dim_user_kmeans

O para aplicar los cambios a la base:

python src/analytics/dim_user_country_imputation_kmeans.py --use-project-db --apply-updates

Notas metodológicas
-------------------
- K-means requiere vectores numéricos; por eso se usa OneHotEncoder.
- La combinación pais_moneda se deriva del comportamiento de precios de Steam
  por país, usando la moneda más frecuente observada en las tablas de hechos.
- La imputación final NO usa asignación aleatoria. Se usa la mayoría del cluster,
  con fallback al modo global si un cluster quedara sin países conocidos.
"""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from sqlalchemy import create_engine, text

try:
    from sklearn.cluster import KMeans
    from sklearn.preprocessing import OneHotEncoder
except Exception as exc:
    raise SystemExit(
        "No se pudo importar scikit-learn. Instala el paquete en tu entorno virtual."
    ) from exc

LOGGER = logging.getLogger("dim_user_kmeans")
DEFAULT_OUTPUT_DIR = "output_dim_user_kmeans"
DEFAULT_N_CLUSTERS = 4
DEFAULT_SCHEMA = "dbo"


@dataclass
class Config:
    output_dir: Path
    input_csv: Optional[Path]
    schema: str
    n_clusters: int
    random_state: int
    use_project_db: bool
    sql_url: Optional[str]
    apply_updates: bool
    only_current: bool
    only_active: bool
    save_model_matrix: bool


def parse_args() -> Config:
    parser = argparse.ArgumentParser(
        description="Imputa country_key de dim_user con One-Hot + K-means."
    )
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--input-csv", help="CSV opcional; si se omite, se lee de SQL Server.")
    parser.add_argument("--schema", default=DEFAULT_SCHEMA)
    parser.add_argument("--n-clusters", type=int, default=DEFAULT_N_CLUSTERS)
    parser.add_argument("--random-state", type=int, default=42)
    parser.add_argument("--use-project-db", action="store_true",
                        help="Usa src.db.get_session() del proyecto steam-bi-etl.")
    parser.add_argument("--sql-url", help="Cadena SQLAlchemy alternativa.")
    parser.add_argument("--apply-updates", action="store_true",
                        help="Aplica UPDATE real sobre dim_user para completar country_key.")
    parser.add_argument("--all-rows", action="store_true",
                        help="No filtra por is_current ni is_active.")
    parser.add_argument("--save-model-matrix", action="store_true",
                        help="Guarda la matriz one-hot completa como CSV.")
    args = parser.parse_args()

    return Config(
        output_dir=Path(args.output_dir),
        input_csv=Path(args.input_csv) if args.input_csv else None,
        schema=args.schema,
        n_clusters=args.n_clusters,
        random_state=args.random_state,
        use_project_db=args.use_project_db,
        sql_url=args.sql_url,
        apply_updates=args.apply_updates,
        only_current=not args.all_rows,
        only_active=not args.all_rows,
        save_model_matrix=args.save_model_matrix,
    )


def ensure_output_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def import_project_db_helpers():
    try:
        from src.db import get_session, start_etl_run, finish_etl_run  # type: ignore
        return get_session, start_etl_run, finish_etl_run
    except Exception:
        return None, None, None


def get_engine(config: Config):
    if config.use_project_db:
        get_session, _, _ = import_project_db_helpers()
        if get_session is None:
            raise SystemExit(
                "No se pudo importar src.db.get_session. Ejecuta el script desde el proyecto o usa --sql-url."
            )
        with get_session() as session:
            return session.bind

    if config.sql_url:
        return create_engine(config.sql_url)

    raise SystemExit("Debes usar --use-project-db o proporcionar --sql-url.")


def build_extraction_sql(schema: str) -> str:
    return f"""
    WITH country_currency_votes AS (
        SELECT
            country_key,
            currency_key,
            SUM(freq) AS total_freq
        FROM (
            SELECT
                country_key,
                currency_key,
                COUNT_BIG(*) AS freq
            FROM {schema}.fact_game_price_period
            WHERE country_key IS NOT NULL AND currency_key IS NOT NULL
            GROUP BY country_key, currency_key

            UNION ALL

            SELECT
                country_key,
                currency_key,
                COUNT_BIG(*) AS freq
            FROM {schema}.fact_game_price_snapshot
            WHERE country_key IS NOT NULL AND currency_key IS NOT NULL
            GROUP BY country_key, currency_key
        ) q
        GROUP BY country_key, currency_key
    ),
    country_currency_ranked AS (
        SELECT
            country_key,
            currency_key,
            total_freq,
            ROW_NUMBER() OVER (
                PARTITION BY country_key
                ORDER BY total_freq DESC, currency_key
            ) AS rn
        FROM country_currency_votes
    )
    SELECT
        u.user_key,
        u.steamid_hash,
        u.visibility_state,
        u.profile_state,
        u.persona_state,
        u.country_key,
        u.account_created_date,
        u.account_created_year,
        u.account_age_band,
        u.last_logoff_date,
        u.last_logoff_time_bucket_key,
        u.created_at,
        u.updated_at,
        u.etl_run_id,
        u.valid_from,
        u.valid_to,
        u.is_current,
        u.is_active,
        c.iso_code AS country_iso,
        c.country_name AS country_name,
        r.currency_key AS market_currency_key,
        cur.currency_code AS market_currency_code,
        cur.currency_name AS market_currency_name
    FROM {schema}.dim_user u
    LEFT JOIN {schema}.dim_country c
        ON c.country_key = u.country_key
    LEFT JOIN country_currency_ranked r
        ON r.country_key = u.country_key
       AND r.rn = 1
    LEFT JOIN {schema}.dim_currency cur
        ON cur.currency_key = r.currency_key
    WHERE 1 = 1
      AND (:only_current = 0 OR u.is_current = 1)
      AND (:only_active = 0 OR u.is_active = 1)
    """


def load_data_from_db(config: Config) -> pd.DataFrame:
    engine = get_engine(config)
    sql = build_extraction_sql(config.schema)
    params = {
        "only_current": 1 if config.only_current else 0,
        "only_active": 1 if config.only_active else 0,
    }
    df = pd.read_sql(text(sql), engine, params=params)
    return df


def load_data(config: Config) -> pd.DataFrame:
    if config.input_csv:
        df = pd.read_csv(config.input_csv)
    else:
        df = load_data_from_db(config)
    return clean_dataframe(df)


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    for col in out.columns:
        if pd.api.types.is_object_dtype(out[col]) or str(out[col].dtype).startswith("string"):
            out[col] = out[col].astype("string").str.strip()
            out[col] = out[col].replace({
                "": pd.NA,
                "NULL": pd.NA,
                "null": pd.NA,
                "None": pd.NA,
            })

    # Columnas numéricas claves
    numeric_cols = [
        "user_key", "visibility_state", "profile_state", "persona_state",
        "country_key", "account_created_year", "last_logoff_time_bucket_key",
        "market_currency_key"
    ]
    for col in numeric_cols:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce")

    # Caso observado en OpenRefine: puede aparecer un valor anómalo tipo 'country_key'
    if "country_key" in out.columns:
        mask_bad = out["country_key"].astype("string").str.lower().eq("country_key")
        if mask_bad.any():
            out.loc[mask_bad, "country_key"] = np.nan
            for col in ["country_iso", "country_name", "market_currency_code", "market_currency_name"]:
                if col in out.columns:
                    out.loc[mask_bad, col] = pd.NA

    return out


def build_profile(df: pd.DataFrame) -> Dict[str, object]:
    total_rows = int(len(df))
    missing_country = int(df["country_key"].isna().sum()) if "country_key" in df.columns else 0
    distinct_country = int(df["country_key"].dropna().nunique()) if "country_key" in df.columns else 0

    profile = {
        "total_rows": total_rows,
        "total_columns": int(len(df.columns)),
        "columns": list(df.columns),
        "nulls_by_column": {col: int(df[col].isna().sum()) for col in df.columns},
        "distinct_country_key": distinct_country,
        "missing_country_key": missing_country,
        "missing_country_pct": round((missing_country / total_rows) * 100, 4) if total_rows else 0.0,
    }
    return profile


def save_profile(profile: Dict[str, object], output_dir: Path) -> None:
    with (output_dir / "dim_user_profile.json").open("w", encoding="utf-8") as f:
        json.dump(profile, f, ensure_ascii=False, indent=2)


def build_country_currency(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    if "country_iso" not in out.columns:
        out["country_iso"] = pd.NA
    if "market_currency_code" not in out.columns:
        out["market_currency_code"] = pd.NA

    out["country_iso"] = out["country_iso"].astype("string").str.upper().str.strip()
    out["market_currency_code"] = out["market_currency_code"].astype("string").str.upper().str.strip()

    out["country_currency"] = np.where(
        out["country_iso"].notna() & out["market_currency_code"].notna(),
        out["country_iso"] + "_" + out["market_currency_code"],
        pd.NA
    )
    out["country_currency"] = pd.Series(out["country_currency"], dtype="string")
    out["country_currency_feature"] = out["country_currency"].fillna("UNKNOWN_UNKNOWN")
    return out


def add_derived_features(df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str]]:
    out = df.copy()

    # Variables categóricas base
    out["visibility_state_cat"] = out["visibility_state"].fillna(-1).astype("Int64").astype("string")
    out["profile_state_cat"] = out["profile_state"].fillna(-1).astype("Int64").astype("string")
    out["persona_state_cat"] = out["persona_state"].fillna(-1).astype("Int64").astype("string")
    out["account_age_band_cat"] = out["account_age_band"].fillna("DESCONOCIDO").astype("string")
    out["last_logoff_bucket_cat"] = (
        out["last_logoff_time_bucket_key"]
        .fillna(-1)
        .astype("Int64")
        .astype("string")
    )

    # Agrupación de año para evitar demasiada cardinalidad
    if "account_created_year" in out.columns:
        year = pd.to_numeric(out["account_created_year"], errors="coerce")
        out["account_created_year_bin"] = pd.cut(
            year,
            bins=[0, 2005, 2010, 2015, 2020, 2100],
            labels=["<=2005", "2006-2010", "2011-2015", "2016-2020", "2021+"],
            include_lowest=True,
            right=True,
        ).astype("string").fillna("DESCONOCIDO")
    else:
        out["account_created_year_bin"] = "DESCONOCIDO"

    feature_cols = [
        "visibility_state_cat",
        "profile_state_cat",
        "persona_state_cat",
        "account_age_band_cat",
        "account_created_year_bin",
        "last_logoff_bucket_cat",
        "country_currency_feature",
    ]
    return out, feature_cols


def build_onehot_encoder() -> OneHotEncoder:
    # compatibilidad entre versiones de scikit-learn
    try:
        return OneHotEncoder(handle_unknown="ignore", sparse_output=False)
    except TypeError:
        return OneHotEncoder(handle_unknown="ignore", sparse=False)


def run_kmeans(df: pd.DataFrame, feature_cols: List[str], n_clusters: int, random_state: int):
    encoder = build_onehot_encoder()
    X = encoder.fit_transform(df[feature_cols])

    try:
        model = KMeans(
            n_clusters=n_clusters,
            init="k-means++",
            n_init="auto",
            max_iter=300,
            random_state=random_state,
        )
    except TypeError:
        model = KMeans(
            n_clusters=n_clusters,
            init="k-means++",
            n_init=10,
            max_iter=300,
            random_state=random_state,
        )

    labels = model.fit_predict(X)
    feature_names = encoder.get_feature_names_out(feature_cols)
    matrix_df = pd.DataFrame(X, columns=feature_names, index=df.index)
    centroids_df = pd.DataFrame(model.cluster_centers_, columns=feature_names)
    centroids_df.insert(0, "cluster_id", range(n_clusters))
    return labels, encoder, model, matrix_df, centroids_df


def build_cluster_summary(df_clustered: pd.DataFrame) -> pd.DataFrame:
    rows: List[Dict[str, object]] = []

    global_mode_key = pd.to_numeric(df_clustered["country_key"], errors="coerce").dropna().mode()
    global_mode_key = int(global_mode_key.iloc[0]) if not global_mode_key.empty else None

    for cluster_id, group in df_clustered.groupby("cluster_id"):
        known = group[group["country_key"].notna()].copy()
        blanks = group[group["country_key"].isna()].copy()

        dominant_country_key = known["country_key"].mode(dropna=True)
        dominant_country_iso = known["country_iso"].mode(dropna=True)
        dominant_country_name = known["country_name"].mode(dropna=True)
        dominant_currency_code = known["market_currency_code"].mode(dropna=True)
        dominant_country_currency = known["country_currency"].mode(dropna=True)

        if not dominant_country_key.empty:
            dominant_key = int(float(dominant_country_key.iloc[0]))
            share = float((known["country_key"] == dominant_key).sum() / len(known)) if len(known) else 0.0
        else:
            dominant_key = global_mode_key
            share = 0.0

        rows.append({
            "cluster_id": int(cluster_id),
            "cluster_size": int(len(group)),
            "known_country_rows": int(len(known)),
            "blank_country_rows": int(len(blanks)),
            "dominant_country_key": dominant_key,
            "dominant_country_iso": dominant_country_iso.iloc[0] if not dominant_country_iso.empty else pd.NA,
            "dominant_country_name": dominant_country_name.iloc[0] if not dominant_country_name.empty else pd.NA,
            "dominant_currency_code": dominant_currency_code.iloc[0] if not dominant_currency_code.empty else pd.NA,
            "dominant_country_currency": dominant_country_currency.iloc[0] if not dominant_country_currency.empty else pd.NA,
            "dominant_share_known": round(share, 4),
        })

    return pd.DataFrame(rows).sort_values("cluster_id")


def impute_missing_country(df_clustered: pd.DataFrame, cluster_summary: pd.DataFrame) -> pd.DataFrame:
    out = df_clustered.copy()
    out = out.merge(cluster_summary, on="cluster_id", how="left", suffixes=("", "_cluster"))

    global_mode_key = pd.to_numeric(out["country_key"], errors="coerce").dropna().mode()
    global_mode_key = int(global_mode_key.iloc[0]) if not global_mode_key.empty else None

    out["imputed_country_key"] = pd.to_numeric(out["country_key"], errors="coerce")
    out["imputed_country_iso"] = out["country_iso"]
    out["imputed_country_name"] = out["country_name"]
    out["imputed_market_currency_code"] = out["market_currency_code"]
    out["imputation_method"] = np.where(out["country_key"].notna(), "original", pd.NA)

    missing_mask = out["country_key"].isna()
    if missing_mask.any():
        out.loc[missing_mask, "imputed_country_key"] = out.loc[missing_mask, "dominant_country_key"]

        fallback_mask = missing_mask & out["imputed_country_key"].isna()
        if fallback_mask.any() and global_mode_key is not None:
            out.loc[fallback_mask, "imputed_country_key"] = global_mode_key
            out.loc[fallback_mask, "imputation_method"] = "global_mode_fallback"

        normal_mask = missing_mask & out["imputation_method"].isna()
        out.loc[normal_mask, "imputation_method"] = "cluster_majority"

        out.loc[missing_mask, "imputed_country_iso"] = out.loc[missing_mask, "dominant_country_iso"]
        out.loc[missing_mask, "imputed_country_name"] = out.loc[missing_mask, "dominant_country_name"]
        out.loc[missing_mask, "imputed_market_currency_code"] = out.loc[missing_mask, "dominant_currency_code"]

    out["imputed_country_key"] = pd.to_numeric(out["imputed_country_key"], errors="coerce").astype("Int64")
    return out


def build_updates_dataframe(df_imputed: pd.DataFrame) -> pd.DataFrame:
    updates = df_imputed[
        df_imputed["country_key"].isna() &
        df_imputed["imputed_country_key"].notna()
    ][[
        "user_key",
        "cluster_id",
        "dominant_share_known",
        "imputed_country_key",
        "imputed_country_iso",
        "imputed_country_name",
        "imputed_market_currency_code",
        "imputation_method",
    ]].copy()

    updates["user_key"] = pd.to_numeric(updates["user_key"], errors="coerce").astype("Int64")
    updates["imputed_country_key"] = pd.to_numeric(updates["imputed_country_key"], errors="coerce").astype("Int64")
    return updates.sort_values("user_key")


def save_sql_update_script(updates: pd.DataFrame, output_dir: Path, schema: str) -> Path:
    path = output_dir / "apply_country_imputation.sql"
    lines = [
        "-- Script generado automáticamente",
        f"-- Total filas a actualizar: {len(updates)}",
        "BEGIN TRANSACTION;",
        "",
    ]
    for row in updates.itertuples(index=False):
        lines.append(
            f"UPDATE {schema}.dim_user "
            f"SET country_key = {int(row.imputed_country_key)}, updated_at = GETDATE() "
            f"WHERE user_key = {int(row.user_key)} AND country_key IS NULL;"
        )
    lines += ["", "COMMIT TRANSACTION;", ""]
    path.write_text("\n".join(lines), encoding="utf-8")
    return path


def apply_updates_to_db(config: Config, updates: pd.DataFrame) -> int:
    if updates.empty:
        return 0

    get_session, start_etl_run, finish_etl_run = import_project_db_helpers()
    if get_session is None:
        raise SystemExit("Para aplicar updates reales debes ejecutar con --use-project-db dentro del proyecto.")

    run_id = None
    if start_etl_run and finish_etl_run:
        run_id = start_etl_run("dim_user_country_imputation_kmeans.py")

    updated_rows = 0
    try:
        with get_session() as session:
            stmt = text(f"""
                UPDATE {config.schema}.dim_user
                SET country_key = :country_key,
                    updated_at = GETDATE()
                WHERE user_key = :user_key
                  AND country_key IS NULL
                  AND (:only_current = 0 OR is_current = 1)
                  AND (:only_active = 0 OR is_active = 1)
            """)
            payload = [
                {
                    "country_key": int(row.imputed_country_key),
                    "user_key": int(row.user_key),
                    "only_current": 1 if config.only_current else 0,
                    "only_active": 1 if config.only_active else 0,
                }
                for row in updates.itertuples(index=False)
            ]
            result = session.execute(stmt, payload)
            updated_rows = int(result.rowcount or 0)

        if run_id is not None:
            finish_etl_run(
                run_id=run_id,
                status="success",
                rows_updated=updated_rows,
                rows_skipped=max(len(updates) - updated_rows, 0),
            )
        return updated_rows

    except Exception as exc:
        if run_id is not None and finish_etl_run is not None:
            finish_etl_run(
                run_id=run_id,
                status="error",
                rows_updated=updated_rows,
                rows_skipped=max(len(updates) - updated_rows, 0),
                error_message=str(exc)[:4000],
            )
        raise


def save_outputs(
    df_raw: pd.DataFrame,
    df_model: pd.DataFrame,
    matrix_df: pd.DataFrame,
    centroids_df: pd.DataFrame,
    cluster_summary: pd.DataFrame,
    updates_df: pd.DataFrame,
    output_dir: Path,
    config: Config,
) -> None:
    df_raw.to_csv(output_dir / "dim_user_input.csv", index=False)
    df_model.to_csv(output_dir / "dim_user_clustered_imputed.csv", index=False)
    cluster_summary.to_csv(output_dir / "cluster_summary.csv", index=False)
    updates_df.to_csv(output_dir / "country_imputation_updates.csv", index=False)
    centroids_df.to_csv(output_dir / "kmeans_centroids.csv", index=False)

    if config.save_model_matrix:
        matrix_df.to_csv(output_dir / "onehot_matrix.csv", index=False)

    save_sql_update_script(updates_df, output_dir, config.schema)


def print_summary(profile: Dict[str, object], cluster_summary: pd.DataFrame, updates_df: pd.DataFrame, output_dir: Path) -> None:
    print("\n=== RESUMEN DIM_USER + K-MEANS ===")
    print(f"Filas analizadas: {profile['total_rows']}")
    print(f"country_key faltante: {profile['missing_country_key']} ({profile['missing_country_pct']}%)")
    print(f"country_key distintos: {profile['distinct_country_key']}")
    print(f"Clusters generados: {cluster_summary['cluster_id'].nunique()}")
    print(f"Filas a imputar: {len(updates_df)}")
    print(f"Salida: {output_dir.resolve()}")
    print("\nResumen por cluster:")
    print(cluster_summary[[
        'cluster_id','cluster_size','known_country_rows','blank_country_rows',
        'dominant_country_key','dominant_country_iso','dominant_share_known'
    ]].to_string(index=False))


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
    config = parse_args()
    ensure_output_dir(config.output_dir)

    df_raw = load_data(config)
    profile = build_profile(df_raw)
    save_profile(profile, config.output_dir)

    df_model = build_country_currency(df_raw)
    df_model, feature_cols = add_derived_features(df_model)

    labels, encoder, model, matrix_df, centroids_df = run_kmeans(
        df_model,
        feature_cols=feature_cols,
        n_clusters=config.n_clusters,
        random_state=config.random_state,
    )

    df_model["cluster_id"] = labels
    df_model["cluster_assignment_method"] = "kmeans"

    cluster_summary = build_cluster_summary(df_model)
    df_model = impute_missing_country(df_model, cluster_summary)
    updates_df = build_updates_dataframe(df_model)

    save_outputs(
        df_raw=df_raw,
        df_model=df_model,
        matrix_df=matrix_df,
        centroids_df=centroids_df,
        cluster_summary=cluster_summary,
        updates_df=updates_df,
        output_dir=config.output_dir,
        config=config,
    )

    if config.apply_updates:
        updated_rows = apply_updates_to_db(config, updates_df)
        LOGGER.info("Se actualizaron %s filas en dim_user.", updated_rows)

    print_summary(profile, cluster_summary, updates_df, config.output_dir)


if __name__ == "__main__":
    main()