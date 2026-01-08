import pandas as pd
import numpy as np
import re
from dagster import asset, MaterializeResult, AssetExecutionContext
from pathlib import Path
from deltalake import write_deltalake, DeltaTable
from .utils import save_and_vacuum

# --- CONFIGURATION ---
LAKE_ROOT = Path(r"C:\DataLake_JB")
BRONZE_FIRE_ROOT = LAKE_ROOT / "Bronze" / "GovUK_FireStats"
BRONZE_ONS_ROOT = LAKE_ROOT / "Bronze" / "ONS_Data"
BRONZE_EXT_ROOT = LAKE_ROOT / "Bronze" / "External_Data" 
SILVER_ROOT = LAKE_ROOT / "Silver"

# --- MAPPINGS (STRICT 2023 FRA CODES) ---
# Maps NFCC Names to the User's Verified Map Codes
NFCC_TO_ECODE = {
    # --- Alphabetical Counties & Combined ---
    "Avon FRS": "E31000001",
    "Bedfordshire and Luton FRS": "E31000002",
    "Royal Berkshire FRS": "E31000003",
    "Buckinghamshire FRS": "E31000004",
    "Cambridgeshire FRS": "E31000005",
    "Cheshire FRS": "E31000006",
    "Cleveland Fire Brigade": "E31000007",
    "Cornwall FRS": "E31000008",
    "Cumbria FRS": "E31000009",
    "Derbyshire FRS": "E31000010",
    "Devon & Somerset FRS": "E31000011",
    "County Durham and Darlington FRS": "E31000013", # Note: 12 is skipped in your list
    "East Sussex FRS": "E31000014",
    "Essex County FRS": "E31000015",
    "Gloucestershire FRS": "E31000016",
    "Hereford & Worcester FRS": "E31000018", # Note: 17 is skipped (Old Hants)
    "Hertfordshire FRS": "E31000019",
    "Humberside FRS": "E31000020",
    "Kent FRS": "E31000022", # Note: 21 is skipped (Old IOW)
    "Lancashire FRS": "E31000023",
    "Leicestershire FRS": "E31000024",
    "Lincolnshire FRS": "E31000025",
    "Norfolk FRS": "E31000026",
    "North Yorkshire FRS": "E31000027",
    "Northamptonshire FRS": "E31000028",
    "Northumberland FRS": "E31000029",
    "Nottinghamshire FRS": "E31000030",
    "Oxfordshire FRS": "E31000031",
    "Shropshire FRS": "E31000032",
    "Staffordshire FRS": "E31000033",
    "Suffolk FRS": "E31000034",
    "Surrey FRS": "E31000035",
    "Warwickshire FRS": "E31000036",
    "West Sussex FRS": "E31000037",
    "Isles of Scilly FRS": "E31000039",
    
    # --- Metros & Merged (Higher Numbers) ---
    "Greater Manchester FRS": "E31000040",
    "Merseyside FRS": "E31000041",
    "South Yorkshire FRS": "E31000042",
    "Tyne & Wear FRS": "E31000043",
    "West Midlands Fire Service": "E31000044",
    "West Yorkshire FRS": "E31000045",
    "London Fire Brigade": "E31000046",
    "Dorset & Wiltshire": "E31000047",
    "Hampshire & Isle of Wight FRS": "E31000048",

    # --- Devolved (Excluded from English Data Model) ---
    "Mid & West Wales FRS": None,
    "North Wales FRS": None,
    "South Wales FRS": None,
    "Scottish FRS": None,
    "Northern Ireland FRS": None
}

# ==============================================================================
# HELPER FUNCTIONS
# ==============================================================================
def standardise_headers(df: pd.DataFrame) -> pd.DataFrame:
    new_cols = []
    for col in df.columns:
        c = str(col).strip().lower()
        c = re.sub(r"^[pc]_", "", c) 
        c = re.sub(r'[^a-z0-9]+', '_', c).strip('_') 
        new_cols.append(c)
    df.columns = new_cols
    return df

def add_financial_year(df: pd.DataFrame) -> pd.DataFrame:
    date_col = next((c for c in df.columns if "date" in c and "update" not in c), None)
    if date_col:
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        fy_start = np.where(df[date_col].dt.month >= 4, df[date_col].dt.year, df[date_col].dt.year - 1)
        mask = ~np.isnan(fy_start)
        if mask.any():
            df.loc[mask, "financial_year"] = (
                fy_start[mask].astype(int).astype(str) + "/" + 
                (fy_start[mask] + 1).astype(int).astype(str).str[-2:]
            )
    return df

def calculate_midpoint(text_value):
    if pd.isna(text_value) or str(text_value).strip() in ['', '.', 'nan', 'None', 'not known']:
        return np.nan
    text = str(text_value).lower().strip()
    numbers = [float(x) for x in re.findall(r"(\d+\.?\d*)", text)]
    if not numbers:
        return np.nan
    if any(keyword in text for keyword in ["more", "over", "+", "plus"]):
        return numbers[0] 
    if "up to" in text:
        return numbers[0] / 2
    return np.mean(numbers)

def apply_drill_through_mapping(df: pd.DataFrame, context: AssetExecutionContext) -> pd.DataFrame:
    if "fris_incident_type" not in df.columns:
        return df

    type_mapping = {
        "Dwellings": 10, "Dwelling": 10, "Dwelling fires": 10, "Chimney Fires": 10,
        "Other Buildings": 20, "Other building": 20,
        "Road Vehicles": 30, "Road Vehicle": 30,
        "Secondary Fires": 31, "Other Outdoors": 32, "Other Outdoor": 32, "Grass Fire": 32,
        "Due to apparatus": 40, "Good intent": 40, "Malicious": 40, "False Alarm": 40, "Non-fire false alarms": 40,
        "Non-fire incidents": 50 
    }
    clean_key = df["fris_incident_type"].astype(str).str.strip()
    df["drill_through_id"] = clean_key.map(type_mapping).fillna(99).astype(int)
    
    unknowns = df[df["drill_through_id"] == 99]["fris_incident_type"].unique()
    if len(unknowns) > 0:
        context.log.warning(f"   ⚠️ WARNING: Unmapped Incident Types found (ID 99): {unknowns}")
    return df

# ==============================================================================
# ASSET 1: FIRE STATS CLEANER
# ==============================================================================
@asset(
    deps=["fire_stats_bronze_all"], 
    group_name="silver",
    compute_kind="pandas",
    description="Standardises headers, adds Midpoint estimates, Drill-Through IDs, and enforces types."
)
def fire_stats_silver(context: AssetExecutionContext):
    processed_list = []
    
    for folder in BRONZE_FIRE_ROOT.iterdir():
        if not folder.is_dir() or folder.name.startswith("_"): continue
        dataset_name = folder.name
        context.log.info(f"⚙️ Processing {dataset_name}...")
        
        try:
            df = DeltaTable(folder).to_pandas()
            if "financial_year" not in df.columns:
                df = add_financial_year(df)

            if "fris_incident_type" in df.columns:
                 df = apply_drill_through_mapping(df, context)
            else:
                # FALLBACK ID LOGIC
                normalized_name = dataset_name.lower().replace('-', '_').replace(' ', '_')
                fallback_map = {
                    'dwelling': 10, 'other_building': 20, 'road_vehicle': 30, 
                    'outdoor': 31, 'secondary': 31, 'false_alarm': 40,
                    'road_traffic': 50, 'medical': 50, 'flood': 50, 
                    'water': 50, 'animal': 50, 'collaborating': 50,
                    'bariatric': 50, 'other_non_fire': 50 
                }
                found_id = 99
                for key, val in fallback_map.items():
                    if key in normalized_name:
                        found_id = val
                        break
                if found_id != 99:
                    df['drill_through_id'] = found_id

            midpoint_targets = {
                'vehicles': '_midpoint', 'personnel': '_midpoint', 'response_time': '_midpoint',
                'time_at_scene': '_midpoint', 'evacuations': '_count_est', 'victim_age': '_est', 'damage': '_sqm_est'
            }

            for col in df.columns:
                for target, suffix in midpoint_targets.items():
                    if target in col and "code" not in col and suffix not in col:
                        df[col + suffix] = df[col].apply(calculate_midpoint)

            midpoint_suffixes = ['_midpoint', '_est', '_count_est', '_sqm_est']
            for col in df.columns:
                should_be_numeric = False
                if "_code" in col:
                    if not any(x in col for x in ['e_code', 'frs', 'lad', 'lsoa', 'msoa', 'admin']):
                        should_be_numeric = True
                elif any(col.endswith(s) for s in midpoint_suffixes): should_be_numeric = True
                elif col == "drill_through_id": should_be_numeric = True
                elif col == "daily_incidents": should_be_numeric = True

                if should_be_numeric:
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
                else:
                    df[col] = df[col].astype(str).replace({'nan': None, 'None': None})

            target_path = SILVER_ROOT / "GovUK_FireStats" / dataset_name
            save_and_vacuum(df, target_path, context)
            processed_list.append(dataset_name)
            
        except Exception as e:
            context.log.error(f"❌ Failed {dataset_name}: {e}")

    return MaterializeResult(metadata={"Datasets": str(processed_list)})

# ==============================================================================
# ASSET 2: POPULATION TRANSFORMER
# ==============================================================================
@asset(
    deps=["ons_data_bronze"], 
    group_name="silver",
    compute_kind="pandas",
    description="Unpivots Nomis population data, Renames mnemonic, Imputes 2010."
)
def population_silver(context: AssetExecutionContext):
    bronze_path = BRONZE_ONS_ROOT / "population_estimates"
    if not bronze_path.exists(): raise Exception("Bronze Population data not found!")
        
    df = DeltaTable(bronze_path).to_pandas()
    if 'mnemonic' in df.columns:
        df.rename(columns={'mnemonic': 'lsoa_code'}, inplace=True)
    
    year_cols = [c for c in df.columns if c.isdigit()]
    id_vars = [c for c in df.columns if c not in year_cols]
    
    df_long = df.melt(id_vars=id_vars, value_vars=year_cols, var_name="year", value_name="population")
    df_long['year'] = pd.to_numeric(df_long['year'])
    df_long['population'] = pd.to_numeric(df_long['population'].astype(str).str.replace(",",""), errors='coerce').fillna(0).astype(int)
    
    min_year, max_year = 2010, 2025
    all_years = np.arange(min_year, max_year + 1)
    unique_lsoas = df_long['lsoa_code'].dropna().unique()
    
    index = pd.MultiIndex.from_product([unique_lsoas, all_years], names=['lsoa_code', 'year'])
    df_scaffold = pd.DataFrame(index=index).reset_index()
    df_final = pd.merge(df_scaffold, df_long[['lsoa_code', 'year', 'population']], on=['lsoa_code', 'year'], how='left')
    df_final.sort_values(['lsoa_code', 'year'], inplace=True)
    df_final['population'] = df_final.groupby('lsoa_code')['population'].ffill().bfill().fillna(0).astype(int)
    
    target_path = SILVER_ROOT / "ONS_Data" / "population_long"
    save_and_vacuum(df_final, target_path, context)
    return MaterializeResult(metadata={"Rows": len(df_final)})

# ==============================================================================
# ASSET 3: LOOKUP CLEANER
# ==============================================================================
@asset(
    deps=["ons_data_bronze"], 
    group_name="silver",
    compute_kind="pandas",
    description="Standardises headers for Geography Lookups and handles 2021/2023 Boundary Changes."
)
def geography_lookups_silver(context: AssetExecutionContext):
    lookups = ["lookup_lsoa_msoa_lad", "lookup_lad_fra"]
    processed = []
    
    lad_updates = {
        "E07000026": "E06000063", "E07000027": "E06000063", "E07000028": "E06000063", 
        "E07000029": "E06000064", "E07000030": "E06000064", "E07000031": "E06000064",
        "E07000163": "E06000065", "E07000164": "E06000065", "E07000165": "E06000065", 
        "E07000166": "E06000065", "E07000167": "E06000065", "E07000168": "E06000065", "E07000169": "E06000065",
        "E07000186": "E06000066", "E07000187": "E06000066", "E07000189": "E06000066", "E07000242": "E06000066",
        "E07000190": "E06000066", "E07000192": "E06000066",
        "E07000151": "E06000061", "E07000152": "E06000061", "E07000153": "E06000061", "E07000154": "E06000061",
        "E07000150": "E06000062", "E07000155": "E06000062", "E07000156": "E06000062"
    }

    for name in lookups:
        bronze_path = BRONZE_ONS_ROOT / name
        if not bronze_path.exists(): continue
        
        df = DeltaTable(bronze_path).to_pandas()
        df = standardise_headers(df)
        
        if "lsoa" in name:
            lad_col = next((c for c in df.columns if c.startswith('lad') and c.endswith('cd')), None)
            if lad_col: df[lad_col] = df[lad_col].replace(lad_updates)

        target_path = SILVER_ROOT / "ONS_Data" / name
        save_and_vacuum(df, target_path, context)
        processed.append(name)
        
    return MaterializeResult(metadata={"Processed": str(processed)})

# ==============================================================================
# ASSET 4: NFCC FAMILY GROUPS (UPDATED: Uses Verified 2023 Codes)
# ==============================================================================
@asset(
    deps=["nfcc_family_group_bronze"],
    group_name="silver",
    compute_kind="pandas",
    description="Maps NFCC names to 2023 FRA Codes and removes line-break artifacts."
)
def nfcc_family_groups_silver(context: AssetExecutionContext):
    
    bronze_path = BRONZE_EXT_ROOT / "NFCC_Family_Groups"
    if not bronze_path.exists(): raise Exception("Bronze NFCC data not found!")

    df = DeltaTable(bronze_path).to_pandas()
    context.log.info(f"   📖 Read Bronze NFCC: {len(df)} rows.")

    # 2. Apply Mapping
    df['master_frs_code'] = df['frs_name'].map(NFCC_TO_ECODE)

    # 3. Filter Devolved
    df = df.dropna(subset=['master_frs_code'])
    
    # 4. Clean Family Group Strings
    if 'family_group' in df.columns:
        df['family_group'] = (
            df['family_group']
            .astype(str)
            .str.replace("_x000D_", "", regex=False)
            .str.replace(r"[\r\n]+", " ", regex=True)
            .str.strip()
        )

    target_path = SILVER_ROOT / "External_Data" / "nfcc_family_groups"
    if not target_path.parent.exists(): target_path.parent.mkdir(parents=True)

    save_and_vacuum(df, target_path, context)
    return MaterializeResult(metadata={"Rows": len(df)})