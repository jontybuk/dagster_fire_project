import pandas as pd
import os
import requests
from bs4 import BeautifulSoup
from dagster import asset, MaterializeResult, AssetExecutionContext
from pathlib import Path
from deltalake import write_deltalake, DeltaTable

from deltalake import write_deltalake, DeltaTable
from .utils import save_and_vacuum
LAKE_ROOT = Path(os.environ["DAGSTER_LAKE_ROOT"])
NOMIS_LANDING_ROOT = LAKE_ROOT / "Landing" / "Nomis_Data"
BRONZE_ROOT = LAKE_ROOT / "Bronze" / "ONS_Data"
ONS_POPULATION_PAGE = "https://www.ons.gov.uk/peoplepopulationandcommunity/populationandmigration/populationestimates/datasets/lowersuperoutputareamidyearpopulationestimates"

# -------------------------------------------------
# DAGSTER ASSET
# -------------------------------------------------
@asset(
    group_name="bronze",
    compute_kind="python",
    description="Ingests Nomis Data using Column A Anchor."
)
def ons_data_bronze(context: AssetExecutionContext):
    
    if not BRONZE_ROOT.exists(): 
        BRONZE_ROOT.mkdir(parents=True)
    
    results = {}

    # ==============================================================================
    # PART 1: POPULATION DATA
    # ==============================================================================
    context.log.info("📂 Checking for Local Nomis Files...")
    
    pop_filename = "LSOA Populations 2011 to 2024.csv"
    pop_path = NOMIS_LANDING_ROOT / pop_filename

    if pop_path.exists():
        context.log.info(f"   Processing Population File: {pop_filename}")
        try:
            # 1. Load CSV, skipping first 6 rows so row 7 becomes the header
            # We use 'latin1' because Gov UK files often contain weird spacing characters
            df = pd.read_csv(pop_path, skiprows=6, encoding="latin1")
            context.log.info(f"   🎯 Header row loaded (skipped first 6 rows)")

            # 2. Rename Columns by POSITION (Safest method)
            # We know Col 0 is Name and Col 1 is Code based on the file structure
            if len(df.columns) >= 2:
                df.rename(columns={
                    df.columns[0]: 'lsoa_name', 
                    df.columns[1]: 'lsoa_code'
                }, inplace=True)
            
            # 3. Clean Headers (Strip spaces, lowercase)
            df.columns = df.columns.astype(str).str.strip().str.lower()
            
            # 4. REMOVE BLANK ROWS
            # We filter out rows where 'lsoa_code' is empty (this catches Row 8)
            initial_count = len(df)
            df = df[df['lsoa_code'].notna()]
            
            # Also filter out empty strings just in case
            df = df[df['lsoa_code'].astype(str).str.strip() != ""]
            
            dropped = initial_count - len(df)
            if dropped > 0:
                context.log.info(f"   ℹ️ Dropped {dropped} blank/metadata rows.")

            # 5. Sanitize Column Names for Delta Lake
            df.columns = df.columns.str.replace(r'[^a-z0-9_]+', '_', regex=True)
            
            # 6. Write
            target_path = BRONZE_ROOT / "population_estimates"
            save_and_vacuum(df, target_path, context)

            results["population_nomis"] = len(df)
            context.log.info(f"   ✅ Ingested {len(df)} rows.")

        except Exception as e:
            context.log.error(f"   ❌ Failed to process population csv: {e}")
            raise e
    else:
        context.log.warning(f"   ⚠️ File not found: {pop_path}")

    # ==============================================================================
    # PART 2: LOOKUPS
    # ==============================================================================
    lookups = {
        "lookup_lsoa_msoa_lad": "LSOA to MSOA to LAD.csv",
        "lookup_lad_fra": "LAD to FRA.csv"
    }

    for table_name, filename in lookups.items():
        file_path = NOMIS_LANDING_ROOT / filename
        if file_path.exists():
            context.log.info(f"   Processing Lookup: {filename}")
            try:
                df = pd.read_csv(file_path, encoding='latin1')
                df.columns = df.columns.str.strip().str.lower().str.replace(r'[^a-z0-9_]+', '_', regex=True)
                
                target_path = BRONZE_ROOT / table_name
                save_and_vacuum(df, target_path, context)

                results[table_name] = len(df)
                context.log.info(f"   ✅ Ingested {table_name}")
            except Exception as e:
                context.log.error(f"   ❌ Failed lookup {filename}: {e}")

    # ==============================================================================
    # PART 3: ONS CHECK
    # ==============================================================================
    try:
        session = requests.Session()
        session.headers.update({'User-Agent': 'Mozilla/5.0 DataEngineeringProject/1.0'})
        r = session.get(ONS_POPULATION_PAGE)
        soup = BeautifulSoup(r.content, 'html.parser')
        
        future_years = ["mid-2025", "mid-2026"]
        found_future = False
        for link in soup.find_all('a', href=True):
            if any(y in link.get_text().lower() for y in future_years):
                context.log.warning(f"   🌟 NEW DATA AVAILABLE ON ONS: {link['href']}")
                found_future = True
                break
        
        if not found_future:
            context.log.info("   ℹ️ No future data (2025+) found on ONS yet.")
    except Exception as e:
        context.log.warning(f"   ⚠️ ONS Check skipped: {e}")

    return MaterializeResult(metadata={"Items Processed": str(results)})