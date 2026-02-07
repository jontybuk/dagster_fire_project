import shutil
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

# --- CONFIGURATION ---
LAKE_ROOT = Path(os.environ["DAGSTER_LAKE_ROOT"])

# Folders to completely obliterate (The Output Layers)
RESET_DIRS = ["Bronze", "Silver", "Gold"]

# Folders to clean CAREFULLY (The Input Layer)
LANDING_DIR = LAKE_ROOT / "Landing"
PROTECTED_FOLDERS = ["Nomis_Data"] # <--- KEEPS THIS SAFE

def reset_lake():
    print(f"💣 STARTING DATA LAKE RESET ON: {LAKE_ROOT}")
    
    # 1. DELETE BRONZE, SILVER, GOLD
    for layer in RESET_DIRS:
        target = LAKE_ROOT / layer
        if target.exists():
            print(f"   🔥 Deleting entire {layer} layer...")
            try:
                shutil.rmtree(target)
                print(f"      ✅ {layer} deleted.")
            except Exception as e:
                print(f"      ❌ Failed to delete {layer}: {e}")
        else:
            print(f"   ℹ️ {layer} layer was already empty.")

    # 2. CLEAN LANDING (Preserving Nomis)
    if LANDING_DIR.exists():
        print(f"   🧹 Cleaning Landing Zone (Preserving {PROTECTED_FOLDERS})...")
        
        # Iterate over everything in Landing
        for item in LANDING_DIR.iterdir():
            # If it's one of our protected folders, SKIP IT
            if item.name in PROTECTED_FOLDERS:
                print(f"      🛡️ Skipped protected folder: {item.name}")
                continue
            
            # Otherwise, delete it
            try:
                if item.is_dir():
                    shutil.rmtree(item)
                else:
                    item.unlink()
                print(f"      🗑️ Deleted: {item.name}")
            except Exception as e:
                print(f"      ⚠️ Could not delete {item.name}: {e}")
    
    print("\n✨ RESET COMPLETE. Your Lake is clean (Nomis files preserved).")
    print("🚀 You can now go to Dagster and click 'Materialize All' to test the full rebuild.")

if __name__ == "__main__":
    # Safety confirmation
    confirm = input("⚠️  ARE YOU SURE you want to delete all Bronze/Silver/Gold data? (y/n): ")
    if confirm.lower() == "y":
        reset_lake()
    else:
        print("❌ Reset cancelled.")