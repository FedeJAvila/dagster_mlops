from dagster import load_assets_from_package_module
from . import core
from . import recommender
from . import elt

core_assets = load_assets_from_package_module(
    package_module=core, group_name='core',
    
)
recommender_assets = load_assets_from_package_module(
    package_module=recommender, group_name='recommender'
)

airbyte_dbt_assets = load_assets_from_package_module(
    package_module=elt, group_name='elt'
)