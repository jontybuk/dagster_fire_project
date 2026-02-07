import pandas as pd
import os
from dagster import asset, MaterializeResult, AssetExecutionContext
from pathlib import Path
from deltalake import write_deltalake, DeltaTable
from .utils import save_and_vacuum

# --- CONFIGURATION ---
LAKE_ROOT = Path(os.environ["DAGSTER_LAKE_ROOT"])
SILVER_ONS_ROOT = LAKE_ROOT / "Silver" / "ONS_Data"
SILVER_FIRE_ROOT = LAKE_ROOT / "Silver" / "GovUK_FireStats"
SILVER_EXT_ROOT = LAKE_ROOT / "Silver" / "External_Data" 
GOLD_ROOT = LAKE_ROOT / "Gold" / "Fire_Incident_Model"

# --- MASTER DATA MAPPINGS (Corrected) ---
# Maps Historical Codes to the modern 2023 Standard
FRS_MAPPINGS = {
    # HAMPSHIRE & IOW (Merger) -> E31000048
    "E31000017": "E31000048", # Old Hampshire
    "E31000021": "E31000048", # Old IOW
    
    # DORSET & WILTSHIRE (Merger) -> E31000047
    # "E31000015": "E31000047",  <-- REMOVED: This was incorrect (Essex). 
    # Wiltshire usually kept 47, so no map needed if it was already 47.
}

# The Official Names for the Master Codes (Matches your map)
MASTER_FRS_NAMES = {
    "E31000048": "Hampshire and Isle of Wight Fire and Rescue Service",
    "E31000047": "Dorset & Wiltshire", 
    "E31000008": "Cornwall" # Fallback if Scilly maps here
}

# ==============================================================================
# ASSET 1: DIM_GEOGRAPHY (The Hierarchy - Deduplicated)
# ==============================================================================
@asset(
    deps=["geography_lookups_silver"],
    group_name="gold",
    compute_kind="pandas",
    description="Joins LSOA, LAD, and FRA lookups. Enforces 1 row per LSOA."
)
def dim_geography(context: AssetExecutionContext):
    
    # 1. Read Silver Lookups
    try:
        df_lsoa_lad = DeltaTable(SILVER_ONS_ROOT / "lookup_lsoa_msoa_lad").to_pandas()
        df_lad_fra = DeltaTable(SILVER_ONS_ROOT / "lookup_lad_fra").to_pandas()
    except Exception as e:
        raise Exception(f"❌ Missing Silver Lookups. Run silver_layer first! Error: {e}")

    # 2. Join LSOA->LAD with LAD->FRA
    lad_key_1 = next(c for c in df_lsoa_lad.columns if c.startswith('lad') and c.endswith('cd'))
    lad_key_2 = next(c for c in df_lad_fra.columns if c.startswith('lad') and c.endswith('cd'))
    
    context.log.info(f"🔗 Joining on {lad_key_1} = {lad_key_2}")
    
    df_joined = pd.merge(df_lsoa_lad, df_lad_fra, left_on=lad_key_1, right_on=lad_key_2, how="left")
    
    # 3. Clean and Select Columns
    lsoa_name_col = next(c for c in df_joined.columns if c.startswith('lsoa') and c.endswith('nm'))
    lsoa_code_col = next(c for c in df_joined.columns if c.startswith('lsoa') and c.endswith('cd'))
    msoa_name_col = next(c for c in df_joined.columns if c.startswith('msoa') and c.endswith('nm'))
    msoa_code_col = next(c for c in df_joined.columns if c.startswith('msoa') and c.endswith('cd'))
    lad_name_col = next(c for c in df_joined.columns if c.startswith('lad') and c.endswith('nm'))
    lad_code_col = lad_key_1
    
    fra_name_col = next((c for c in df_joined.columns if c.startswith('fra') and c.endswith('nm')), None)
    fra_code_col = next((c for c in df_joined.columns if c.startswith('fra') and c.endswith('cd')), None)

    cols_to_keep = {
        lsoa_code_col: 'lsoa_code', lsoa_name_col: 'lsoa_name',
        msoa_code_col: 'msoa_code', msoa_name_col: 'msoa_name',
        lad_code_col: 'lad_code', lad_name_col: 'lad_name'
    }
    
    if fra_name_col:
        cols_to_keep[fra_code_col] = 'frs_code'
        cols_to_keep[fra_name_col] = 'frs_name'
        
    df_gold = df_joined[list(cols_to_keep.keys())].rename(columns=cols_to_keep)
    
    # --- NEW: APPLY FRS MAPPINGS ---
    if 'frs_code' in df_gold.columns:
        df_gold['frs_code'] = df_gold['frs_code'].replace(FRS_MAPPINGS)
    
    # --- DEDUPLICATION ---
    initial_count = len(df_gold)
    df_gold = df_gold.drop_duplicates(subset=['lsoa_code'], keep='first').reset_index(drop=True)
    
    target_path = GOLD_ROOT / "Dim_Geography"
    save_and_vacuum(df_gold, target_path, context)
    
    return MaterializeResult(metadata={"Rows": len(df_gold)})

# ==============================================================================
# ASSET 2: FACT_POPULATION
# ==============================================================================
@asset(
    deps=["population_silver", "dim_geography"],
    group_name="gold",
    compute_kind="pandas",
    description="Aggregates Pop to MSOA/LAD/FRS (with FY conversion)."
)
def fact_population(context: AssetExecutionContext):
    
    df_pop = DeltaTable(SILVER_ONS_ROOT / "population_long").to_pandas()
    df_geo = DeltaTable(GOLD_ROOT / "Dim_Geography").to_pandas()
    
    df_pop['financial_year'] = (
        df_pop['year'].astype(str) + "/" + 
        (df_pop['year'] + 1).astype(str).str[-2:]
    )
    
    df_merged = pd.merge(df_pop, df_geo, on="lsoa_code", how="inner")
    outputs = {}
    
    path_lsoa = GOLD_ROOT / "Fact_Population_LSOA"
    save_and_vacuum(df_pop, path_lsoa, context)
    outputs["LSOA"] = len(df_pop)
    
    df_msoa = df_merged.groupby(['financial_year', 'msoa_code', 'msoa_name'])['population'].sum().reset_index()
    path_msoa = GOLD_ROOT / "Fact_Population_MSOA"
    save_and_vacuum(df_msoa, path_msoa, context)
    outputs["MSOA"] = len(df_msoa)
    
    df_lad = df_merged.groupby(['financial_year', 'lad_code', 'lad_name'])['population'].sum().reset_index()
    path_lad = GOLD_ROOT / "Fact_Population_LAD"
    save_and_vacuum(df_lad, path_lad, context)
    outputs["LAD"] = len(df_lad)
    
    if 'frs_code' in df_geo.columns:
        df_frs = df_merged.groupby(['financial_year', 'frs_code', 'frs_name'])['population'].sum().reset_index()
        path_frs = GOLD_ROOT / "Fact_Population_FRS"
        save_and_vacuum(df_frs, path_frs, context)
        outputs["FRS"] = len(df_frs)
        
    return MaterializeResult(metadata={"Aggregations": str(outputs)})

# ==============================================================================
# ASSET 3: DIM_FRS (Linked to NFCC Family Groups)
# ==============================================================================
@asset(
    deps=["fire_stats_silver", "nfcc_family_groups_silver"], 
    group_name="gold",
    compute_kind="pandas",
    description="Creates Master FRS list and enriches with NFCC Family Groups."
)
def dim_frs(context: AssetExecutionContext):
    all_pairs = []
    
    potential_name_cols = ['frs_name', 'frs_territory_name', 'authority_name', 'area_name', 'fra_name', 'territory_name']
    potential_code_cols = ['frs_e_code', 'e_code', 'frs_code', 'areacode', 'authority_code', 'fra_code', 'operating_territory_code']
    ignore_terms = ['lsoa', 'msoa', 'lad', 'incident', 'uprn', 'postcode', 'year', 'date']

    context.log.info(f"📂 Scanning {SILVER_FIRE_ROOT}...")

    for folder in (SILVER_FIRE_ROOT).iterdir():
        if not folder.is_dir() or folder.name.startswith("_"): continue
        dataset_name = folder.name
        
        try:
            df = DeltaTable(folder).to_pandas()
            df.columns = [str(c).lower().strip() for c in df.columns]
            
            found_name = next((c for c in df.columns if c in potential_name_cols), None)
            if not found_name:
                found_name = next((c for c in df.columns if 'frs' in c and 'name' in c), None)

            found_code = next((c for c in df.columns if c in potential_code_cols), None)
            if not found_code:
                found_code = next((
                    c for c in df.columns 
                    if 'code' in c 
                    and ('frs' in c or 'e_' in c or 'area' in c)
                    and not any(bad in c for bad in ignore_terms)
                ), None)

            if found_name and found_code:
                subset = df[[found_code, found_name]].dropna().drop_duplicates()
                pairs = subset.to_dict('records')
                
                for p in pairs:
                    c = str(p[found_code]).strip()
                    n = str(p[found_name]).strip()
                    
                    if len(c) == 9 and c[0] in ['E', 'S', 'W']:
                        # Apply Mapping immediately
                        if c in FRS_MAPPINGS:
                            c = FRS_MAPPINGS[c]
                            if c in MASTER_FRS_NAMES:
                                n = MASTER_FRS_NAMES[c]
                        
                        all_pairs.append({"frs_e_code": c, "frs_name": n})

        except Exception as e:
            context.log.error(f"  ❌ Error reading {dataset_name}: {e}")
        
    if not all_pairs: raise Exception("❌ CRITICAL: No FRS codes found!")

    df_all = pd.DataFrame(all_pairs)
    df_all['name_len'] = df_all['frs_name'].astype(str).apply(len)
    df_all.sort_values(by=['frs_e_code', 'name_len'], ascending=[True, False], inplace=True)
    
    df_unique = df_all.drop_duplicates(subset=['frs_e_code'], keep='first')
    df_unique = df_unique[['frs_e_code', 'frs_name']].reset_index(drop=True)
    
    # --- NEW: JOIN NFCC FAMILY GROUPS ---
    try:
        nfcc_path = SILVER_EXT_ROOT / "nfcc_family_groups"
        if nfcc_path.exists():
            context.log.info("🔗 Linking NFCC Family Groups...")
            df_nfcc = DeltaTable(nfcc_path).to_pandas()
            df_nfcc = df_nfcc[['master_frs_code', 'family_group']].drop_duplicates()
            
            df_unique = df_unique.merge(
                df_nfcc, left_on='frs_e_code', right_on='master_frs_code', how='left'
            )
            df_unique.drop(columns=['master_frs_code'], inplace=True, errors='ignore')
            df_unique['family_group'] = df_unique['family_group'].fillna("Unknown")
        else:
            df_unique['family_group'] = "Unknown"
            
    except Exception as e:
         context.log.warning(f"⚠️ Failed to merge Family Groups: {e}")
         df_unique['family_group'] = "Unknown"

    target_path = GOLD_ROOT / "Dim_FRS"
    save_and_vacuum(df_unique, target_path, context)
    
    return MaterializeResult(metadata={"FRS Count": len(df_unique)})

# ==============================================================================
# ASSET 4: DIM_FINANCIAL_YEAR (UPDATED)
# ==============================================================================
@asset(
    deps=["fire_stats_silver"],
    group_name="gold",
    compute_kind="pandas",
    description="Generates master list of Financial Years with future buffer."
)
def dim_financial_year(context: AssetExecutionContext):
    years = set()
    for folder in (SILVER_FIRE_ROOT).iterdir():
        if not folder.is_dir(): continue
        try:
            df = DeltaTable(folder).to_pandas()
            if 'financial_year' in df.columns:
                years.update(df['financial_year'].dropna().unique())
        except: pass
        
    valid_years = [y for y in years if isinstance(y, str) and len(y) == 7]
    current_max_year = max([int(y[:4]) for y in valid_years]) if valid_years else 2010
    
    for i in range(1, 3):
        next_start = current_max_year + i
        next_end = next_start + 1
        years.add(f"{next_start}/{str(next_end)[-2:]}")

    year_list = sorted(list(years))
    df_years = pd.DataFrame(year_list, columns=['financial_year'])
    df_years['year_sort'] = df_years['financial_year'].str.slice(0, 4).astype(int)
    
    target_path = GOLD_ROOT / "Dim_FinancialYear"
    save_and_vacuum(df_years, target_path, context)
    
    return MaterializeResult(metadata={"Total Years": len(year_list)})

# ==============================================================================
# ASSET 5: DIM_INCIDENT_TYPE
# ==============================================================================
@asset(
    deps=["fire_stats_silver"],
    group_name="gold",
    compute_kind="pandas",
    description="Creates master list of Incident Types with correct Hub Groupings."
)
def dim_incident_type(context: AssetExecutionContext):
    data = []
    category_map = {
        'dwelling': 'Fire', 'domestic_appliance': 'Fire', 'other_building': 'Fire', 
        'road_vehicle': 'Fire', 'outdoor': 'Fire', 'secondary': 'Fire', 'chimney': 'Fire',
        'false_alarm': 'False Alarm',
        'road_traffic': 'Special Service', 'medical': 'Special Service', 'collaborating': 'Special Service',
        'flood': 'Special Service', 'water': 'Special Service', 'animal': 'Special Service',
        'bariatric': 'Special Service', 'other_non_fire': 'Special Service', 'other_incidents': 'Special Service',
        'casualty': 'Victims', 'casualties': 'Victims', 'fatality': 'Victims', 'fatalities': 'Victims'
    }

    for folder in SILVER_FIRE_ROOT.iterdir():
        if not folder.is_dir() or folder.name.startswith("_"): continue
        dataset_name = folder.name
        clean_name = dataset_name.lower().replace('-', '_').replace(' ', '_')
        
        found_category = "Uncategorized" 
        for key, category in category_map.items():
            if key in clean_name:
                found_category = category
                break
        
        friendly_name = dataset_name.replace("_", " ").replace("-", " ").title()
        friendly_name = friendly_name.replace("Non Fire Incidents", "").replace("Govuk", "").strip()
        
        data.append({
            "incident_dataset_key": dataset_name,
            "dataset_name_friendly": friendly_name,
            "incident_category": found_category
        })
        
    df_types = pd.DataFrame(data)
    df_types.sort_values(by=['incident_category', 'dataset_name_friendly'], inplace=True)

    target_path = GOLD_ROOT / "Dim_IncidentType"
    save_and_vacuum(df_types, target_path, context)
    return MaterializeResult(metadata={"Types Created": len(df_types)})

# ==============================================================================
# ASSET 6: FACT GENERATOR
# ==============================================================================
@asset(
    deps=["fire_stats_silver"],
    group_name="gold",
    compute_kind="pandas",
    description="Promotes Silver to Gold Facts, applying FRS mappings, Lineage Keys, and Drill-Through IDs."
)
def create_fire_facts(context: AssetExecutionContext):
    
    processed_facts = []
    SKIP_FY_CHECK = ['uncategorized_fire_data', 'fire_stations']

    for folder in SILVER_FIRE_ROOT.iterdir():
        if not folder.is_dir() or folder.name.startswith("_"): continue
        dataset_name = folder.name
        
        try:
            df = DeltaTable(folder).to_pandas()
            df['incident_dataset_key'] = dataset_name
            
            if 'drill_through_id' not in df.columns:
                 context.log.warning(f"⚠️ {dataset_name} missing drill_through_id!")
                 df['drill_through_id'] = 99 

            if dataset_name not in SKIP_FY_CHECK and 'financial_year' not in df.columns:
                context.log.warning(f"   ⚠️ {dataset_name} missing 'financial_year'.")
            
            frs_col = next((c for c in df.columns if 'e_code' in c), None)
            if frs_col:
                df['master_frs_code'] = df[frs_col].replace(FRS_MAPPINGS)
            
            fact_name = "Fact_" + dataset_name.replace("-", " ").replace("_", " ").title().replace(" ", "")
            target_path = GOLD_ROOT / fact_name
            save_and_vacuum(df, target_path, context)
            processed_facts.append(fact_name)
            
        except Exception as e:
            context.log.error(f"❌ Failed to create fact for {dataset_name}: {e}")

    return MaterializeResult(metadata={"Facts Created": str(processed_facts)})

# ==============================================================================
# ASSET 7: FACT_FRS_RISK_PROFILES (The NFCC Risk Metrics)
# ==============================================================================
@asset(
    deps=["nfcc_family_groups_silver"],
    group_name="gold",
    compute_kind="pandas",
    description="Creates a Fact table of FRS Risk Metrics (Motorways, Urban %, Deprivation). Static Snapshot."
)
def fact_frs_risk_profiles(context: AssetExecutionContext):
    
    # 1. Read Silver NFCC Data
    source_path = SILVER_EXT_ROOT / "nfcc_family_groups"
    if not source_path.exists():
        raise Exception("❌ NFCC Silver data missing. Run 'nfcc_family_groups_silver' first.")
        
    df = DeltaTable(source_path).to_pandas()
    
    # 2. Define the Metrics to Keep
    risk_metrics = [
        'perimeter', 'area', 'motorway', 'a_roads', 
        'surface_water', 'coastlength', 'urban', 
        'population', 'population_density', 'woodland', 
        'imd_decile1', 
        'grade1_p', 'residential_p', 'commerical_p', 
        'highrise', 'ports', 'airports', 
        'rail_length', 'rail_lines', 'comah'
    ]
    
    # 3. Select and Clean
    cols_to_keep = ['master_frs_code'] + [c for c in risk_metrics if c in df.columns]
    
    df_risk = df[cols_to_keep].copy()
    
    # 4. Enforce Numeric Types
    for col in df_risk.columns:
        if col != 'master_frs_code':
            df_risk[col] = pd.to_numeric(df_risk[col], errors='coerce').fillna(0)

    # 5. Add Context Columns
    df_risk['financial_year'] = '2023/24' 
    df_risk['data_source'] = 'NFCC Family Groups (Static Snapshot)'

    # 6. Save
    target_path = GOLD_ROOT / "Fact_FRS_Risk_Profiles"
    save_and_vacuum(df_risk, target_path, context)
    
    context.log.info(f"✅ Created Risk Fact with {len(df_risk)} rows and {len(cols_to_keep)} metrics.")
    
    return MaterializeResult(metadata={"Metrics Loaded": str(cols_to_keep)})