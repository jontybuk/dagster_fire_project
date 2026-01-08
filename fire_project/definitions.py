from dagster import Definitions, load_assets_from_modules
from fire_project.assets import bronze_layer, silver_layer, ons_layer, gold_layer # <--- 1. Import gold_layer

# Load assets from all modules
bronze_assets = load_assets_from_modules([bronze_layer])
silver_assets = load_assets_from_modules([silver_layer])
ons_assets = load_assets_from_modules([ons_layer])
gold_assets = load_assets_from_modules([gold_layer]) # <--- 2. Load the assets

defs = Definitions(
    assets=[
        *bronze_assets, 
        *silver_assets, 
        *ons_assets, 
        *gold_assets # <--- 3. Register them here
    ],
)