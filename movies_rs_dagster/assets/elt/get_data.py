from dagster_airbyte import build_airbyte_assets
from dagster_dbt import load_assets_from_dbt_project
import os

airbyte_peliculas_assets = build_airbyte_assets(
    connection_id=os.environ.get("AIRBYTE_MOVIES_CONNECTION_ID"),
    asset_key_prefix=['recommmender_system_raw'],
    destination_tables=["peliculas"],
)

airbyte_scores_assets = build_airbyte_assets(
    connection_id=os.environ.get("AIRBYTE_SCORES_CONNECTION_ID"),
    asset_key_prefix=['recommmender_system_raw'],
    destination_tables=["scores"]
)

airbyte_usuarios_assets = build_airbyte_assets(
    connection_id=os.environ.get("AIRBYTE_USERS_CONNECTION_ID"),
    asset_key_prefix=['recommmender_system_raw'],
    destination_tables=["usuarios"],
)

dbt_assets = load_assets_from_dbt_project(
    project_dir=os.environ.get("DBT_PROJECT_DIR"), 
    profiles_dir=os.environ.get("DBT_PROFILES_DIR"),
    key_prefix=["dbt"]
)
