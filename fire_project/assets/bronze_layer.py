import requests
import pandas as pd
import time
import os
import re
from io import BytesIO 
from bs4 import BeautifulSoup
from dagster import asset, MaterializeResult, AssetExecutionContext
from pathlib import Path
from deltalake import write_deltalake, DeltaTable
from urllib.parse import urljoin
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from .utils import save_and_vacuum

# --- CONFIGURATION ---
SOURCE_URL = "https://www.gov.uk/government/statistics/fire-statistics-incident-level-datasets"
NFCC_URL = "https://nfcc.org.uk/wp-content/uploads/2023/09/Family-Groups-Summary-Data_0.xlsx"

# Landing Paths (Physical Files)
LANDING_ROOT = Path(r"C:\DataLake_JB\Landing\GovUK_FireStats")
LANDING_EXT_ROOT = Path(r"C:\DataLake_JB\Landing\External_Data") # New Landing Folder

# Bronze Paths (Delta Tables)
BRONZE_ROOT = Path(r"C:\DataLake_JB\Bronze\GovUK_FireStats")
BRONZE_EXT_ROOT = Path(r"C:\DataLake_JB\Bronze\External_Data") 

# ==============================================================================
# ASSET 1: GOV.UK FIRE STATISTICS (Existing Scraper)
# ==============================================================================
@asset(
    group_name="bronze", 
    compute_kind="python",
    description="Scrapes ALL Fire Stats .ods files, combines all year-sheets, and ingests them into Delta tables."
)
def fire_stats_bronze_all(context: AssetExecutionContext):
    
    # --- SETUP ---
    if not LANDING_ROOT.exists():
        LANDING_ROOT.mkdir(parents=True)
        
    session = requests.Session()
    retries = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'})

    # --- STEP 1: DISCOVERY ---
    context.log.info(f"🔎 Scanning {SOURCE_URL}...")
    response = session.get(SOURCE_URL, timeout=30)
    soup = BeautifulSoup(response.content, 'html.parser')
    
    # Find all unique ODS links
    download_queue = []
    seen_urls = set()
    
    for link in soup.find_all('a', href=True):
        href = link['href'].lower()
        if "dataset" in href and href.endswith('.ods'):
            full_url = urljoin(SOURCE_URL, link['href'])
            if full_url not in seen_urls:
                download_queue.append(full_url)
                seen_urls.add(full_url)
    
    context.log.info(f"⬇️ Found {len(download_queue)} files to process.")

    # --- STEP 2: BULK DOWNLOAD ---
    local_file_map = {} 

    for i, url in enumerate(download_queue):
        filename = url.split("/")[-1]
        local_filepath = LANDING_ROOT / filename
        
        # Determine Group
        match = re.match(r"^(.*)_dataset", filename)
        if match:
            group_name = match.group(1)
        else:
            group_name = "uncategorized_fire_data"
            
        if group_name not in local_file_map:
            local_file_map[group_name] = []
        local_file_map[group_name].append(local_filepath)

        # Download
        context.log.info(f"  💾 Downloading {i+1}/{len(download_queue)}: {filename} (Group: {group_name})")
        
        try:
            with session.get(url, stream=True, timeout=120) as r:
                r.raise_for_status()
                with open(local_filepath, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192): 
                        f.write(chunk)
            time.sleep(3) 
            
        except Exception as e:
            context.log.error(f"❌ Failed to download {url}: {e}")
            continue

    # --- STEP 3: PROCESS GROUPS ---
    results_summary = {}

    for group_name, file_paths in local_file_map.items():
        context.log.info(f"⚙️ Processing Group: '{group_name}' ({len(file_paths)} files)...")
        
        dfs = []
        for filepath in file_paths:
            if not filepath.exists(): continue 
                
            try:
                # Read ODS with Calamine
                xl_file = pd.ExcelFile(filepath, engine="calamine")
                
                # Find Year Sheets
                year_sheets = [s for s in xl_file.sheet_names if re.match(r'^\d{6}$', s)]
                
                if not year_sheets:
                    fallback_sheet = next((s for s in xl_file.sheet_names if "dataset" in s.lower()), None)
                    if fallback_sheet:
                        year_sheets = [fallback_sheet]
                    elif len(xl_file.sheet_names) > 1:
                        year_sheets = [xl_file.sheet_names[1]]
                    else:
                        context.log.warning(f"    ⚠️ Skipping {filepath.name}: No valid sheets found.")
                        continue
                
                # Loop Sheets
                for sheet in year_sheets:
                    df = pd.read_excel(filepath, sheet_name=sheet, engine="calamine")
                    
                    # Clean Headers
                    df.columns = df.columns.str.lower().str.replace(r'[^a-z0-9_]+', '_', regex=True).str.strip('_')
                    
                    # Metadata
                    df["source_file"] = filepath.name
                    df["source_sheet"] = sheet 
                    
                    if re.match(r'^\d{6}$', sheet):
                        formatted_year = f"{sheet[:4]}/{sheet[4:]}" 
                        df["sheet_financial_year"] = formatted_year

                    df = df.astype(str)
                    dfs.append(df)
                
            except Exception as e:
                context.log.error(f"    ❌ Error reading {filepath.name}: {e}")

        # Save Group
        if dfs:
            final_df = pd.concat(dfs, ignore_index=True)
            target_table_path = BRONZE_ROOT / group_name
            save_and_vacuum(final_df, target_table_path, context)
            results_summary[group_name] = len(final_df)

    return MaterializeResult(metadata={"Groups Processed": str(list(results_summary.keys())), "Row Counts": str(results_summary)})


# ==============================================================================
# ASSET 2: NFCC FAMILY GROUPS (Final: Landing First + Dedupe + Sheet 0)
# ==============================================================================
@asset(
    group_name="bronze",
    compute_kind="python",
    description="Downloads NFCC data to Landing folder, then ingests to Bronze Delta."
)
def nfcc_family_group_bronze(context: AssetExecutionContext):
    
    # 1. Setup Landing Path
    if not LANDING_EXT_ROOT.exists():
        LANDING_EXT_ROOT.mkdir(parents=True)

    filename = "nfcc_family_groups.xlsx"
    local_landing_path = LANDING_EXT_ROOT / filename
    
    # 2. Download to Landing (Physical File)
    context.log.info(f"📥 Downloading NFCC Data to {local_landing_path}...")
    try:
        response = requests.get(NFCC_URL, headers={'User-Agent': 'Mozilla/5.0'})
        response.raise_for_status()
        
        with open(local_landing_path, 'wb') as f:
            f.write(response.content)
            
        context.log.info("✅ Download complete.")
        
    except Exception as e:
        raise Exception(f"❌ Failed to download NFCC data: {e}")

    # 3. Read from Local Landing File
    try:
        # sheet_name=0 ensures we ONLY read the first tab (ignoring notes/extra sheets)
        df = pd.read_excel(local_landing_path, sheet_name=0)
        
        # 4. Bronze Cleanup & DEDUPLICATION
        # First, standardise the names
        df.columns = df.columns.str.strip().str.lower().str.replace(r'[^a-z0-9_]+', '_', regex=True)
        
        # --- FIX: Deduplicate Columns ---
        # This handles the "Duplicate column names found" error (FRS_Code appearing twice)
        new_columns = []
        seen_columns = {}
        
        for col in df.columns:
            if col in seen_columns:
                seen_columns[col] += 1
                new_col = f"{col}_{seen_columns[col]}" # e.g., frs_code_1
            else:
                seen_columns[col] = 0
                new_col = col
            new_columns.append(new_col)
            
        df.columns = new_columns
        # -------------------------------

        df = df.astype(str)
        
        # Add metadata
        df['ingestion_source'] = str(local_landing_path)
        
    except Exception as e:
        raise Exception(f"❌ Failed to parse local Excel file: {e}")

    # 5. Save to Bronze Delta Table
    target_bronze_path = BRONZE_EXT_ROOT / "NFCC_Family_Groups"
    
    if not target_bronze_path.parent.exists():
        target_bronze_path.parent.mkdir(parents=True)
    
    save_and_vacuum(df, target_bronze_path, context)
    
    context.log.info(f"✅ Successfully ingested {len(df)} rows to Bronze.")
    
    return MaterializeResult(
        metadata={
            "rows": len(df),
            "landing_path": str(local_landing_path),
            "bronze_path": str(target_bronze_path)
        }
    )