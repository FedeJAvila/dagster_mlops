from dagster import asset, Output, String, AssetIn, FreshnessPolicy, MetadataValue
from dagster_mlflow import mlflow_tracking
import pandas as pd
import os
from .core_helper import get_db_data

movies_categories_columns = [
    'film_noir', 'action', 'adventure', 'horror', 'war', 
    'romance', 'western', 'documentary', 'sci_fi', 'drama', 
    'thriller', 'crime', 'childrens', 'fantasy', 'animation', 
    'comedy', 'mystery', 'musical']

@asset(
    freshness_policy=FreshnessPolicy(maximum_lag_minutes=5),
    code_version="2",
    config_schema={
        'uri': String
    },
)
def movies(context) -> Output[pd.DataFrame]:
    #uri = context.op_config["uri"]
    #result = pd.read_csv(uri)
    result = get_db_data(os.environ.get('DATA_DB_SCHEMA'), os.environ.get('MOVIES_TABLE_NAME'))
    return Output(
        result,
        metadata={
            "Total rows": len(result),
            **result[movies_categories_columns].sum().to_dict(),
            "preview": MetadataValue.md(result.head().to_markdown()),
        },
    )


@asset(
    config_schema={
         'uri': String
    }
)
def users(context) -> Output[pd.DataFrame]:
    #uri = context.op_config["uri"]
    #result = pd.read_csv(uri)
    result = get_db_data(os.environ.get('DATA_DB_SCHEMA'), os.environ.get('USERS_TABLE_NAME'))
    return Output(
        result,
        metadata={
            "Total rows": len(result),
            **result.groupby('occupation').count()['user_id'].to_dict()
        },
    )


@asset(
    resource_defs={'mlflow': mlflow_tracking},
    config_schema={
        'uri': String
    }
)
def scores(context) -> Output[pd.DataFrame]:
    mlflow = context.resources.mlflow
    #uri = context.op_config["uri"]
    #result = pd.read_csv(uri)
    result = get_db_data(os.environ.get('DATA_DB_SCHEMA'), os.environ.get('SCORES_TABLE_NAME'))
    metrics = {
        "Total rows": len(result),
        "scores_mean": result['rating'].mean(),
        "scores_std": result['rating'].std(),
        "unique_movies": len(result['movie_id'].unique()),
        "unique_users": len(result['user_id'].unique())
    }
    mlflow.log_metrics(metrics)

    return Output(
        result,
        metadata=metrics,
    )

@asset(ins={
    "scores": AssetIn(),
    "movies": AssetIn(),
    "users": AssetIn(),
})
def training_data(users: pd.DataFrame, movies: pd.DataFrame, scores: pd.DataFrame) -> Output[pd.DataFrame]:
    #scores_users = pd.merge(scores, users, left_on='user_id', right_on='id')
    #all_joined = pd.merge(scores_users, movies, left_on='movie_id', right_on='id')
    all_joined = get_db_data(os.environ.get('DATA_DB_SCHEMA'), os.environ.get('JOINED_TABLE_NAME'))
    return Output(
        all_joined,
        metadata={
            "Total rows": len(all_joined),
        },
    )
