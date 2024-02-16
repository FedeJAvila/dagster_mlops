from dagster import Definitions, define_asset_job, AssetSelection, ScheduleDefinition, FilesystemIOManager
from dagster_airbyte import AirbyteResource
from dagster_dbt import DbtCliResource
from .assets import core_assets, recommender_assets, airbyte_dbt_assets
import os

all_assets = [*core_assets, *recommender_assets, *airbyte_dbt_assets]

mlflow_resources = {
    'mlflow': {
        'config': {
            'experiment_name': 'recommender_system',
        }            
    },
}
data_ops_config = {
    'movies': {
        'config': {
            'uri': 'https://raw.githubusercontent.com/mlops-itba/Datos-RS/main/data/peliculas_0.csv'
            }
    },
    'users': {
        'config': {
            'uri': 'https://raw.githubusercontent.com/mlops-itba/Datos-RS/main/data/usuarios_0.csv'
            }
    },
    'scores': {
        'config': {
            'uri': 'https://raw.githubusercontent.com/mlops-itba/Datos-RS/main/data/scores_0.csv'
            }
    }
}

training_config = {
    'keras_dot_product_model': {
        'config': {
            'batch_size': 128,
            'epochs': 10,
            'learning_rate': 1e-3,
            'embeddings_dim': 5
        }
    }
}

job_data_config = {
    'resources': {
        **mlflow_resources
    },
    'ops': {
        **data_ops_config,
    }
}

job_elt_config = {}

job_training_config = {
    'resources': {
        **mlflow_resources
    },
    'ops': {
        **training_config
    }
}

job_all_config = {
    'resources': {
        **mlflow_resources
    },
    'ops': {
        **data_ops_config,
        **training_config
    }
}

get_data_job = define_asset_job(
    name='get_data',
    selection=['movies', 'users', 'scores', 'training_data'],
    config=job_data_config
)

get_data_schedule = ScheduleDefinition(
    job=get_data_job,
    cron_schedule="0 * * * *",  # every hour
)

io_manager = FilesystemIOManager(
    base_dir="data",  # Path is built relative to where `dagster dev` is run
)

airbyte_resource = AirbyteResource(
    host=os.environ.get("AIRBYTE_HOST"),
    port=os.environ.get("AIRBYTE_PORT"),
    username=os.environ.get("AIRBYTE_USER"),
    password=os.environ.get("AIRBYTE_PASSWORD")
)

dbt_resource = DbtCliResource(
    project_dir=os.environ.get("DBT_PROJECT_DIR"), 
    profiles_dir=os.environ.get("DBT_PROFILES_DIR")
)

defs = Definitions(
    assets=all_assets,
    jobs=[
        get_data_job,
        define_asset_job("full_process",
                         selection=AssetSelection.groups('elt', 'recommender', 'core'),
                         config=job_all_config),
        define_asset_job(
            "only_training",
            # selection=['preprocessed_training_data', 'user2Idx', 'movie2Idx'],
            selection=AssetSelection.groups('recommender'),
            config=job_training_config
        ),
        define_asset_job(
            "do_elt",
            selection=AssetSelection.groups('elt'),
            config=job_elt_config
        )
    ],
    resources={
        # 'mlflow': mlflow_tracking
        "io_manager": io_manager,
        "airbyte": airbyte_resource,
        "dbt": dbt_resource
    },
    schedules=[get_data_schedule],
    # sensors=all_sensors,
)

