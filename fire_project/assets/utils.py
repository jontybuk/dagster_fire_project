from pathlib import Path
from deltalake import write_deltalake, DeltaTable
from dagster import AssetExecutionContext
import pandas as pd

def save_and_vacuum(
    df: pd.DataFrame, 
    path: Path, 
    context: AssetExecutionContext,
    mode: str = "overwrite", 
    schema_mode: str = "overwrite"
):
    """
    Saves a DataFrame to a Delta table and immediately performs a vacuum.
    """
    context.log.info(f"  💾 Saving {len(df)} rows to {path}")
    
    write_deltalake(
        path, 
        df, 
        mode=mode, 
        schema_mode=schema_mode
    )
    
    try:
        dt = DeltaTable(path)
        dt.vacuum(retention_hours=0, enforce_retention_duration=False, dry_run=False)
        context.log.info(f"  🧹 Vacuumed {path.name}")
    except Exception as v_err:
        context.log.warning(f"  ⚠️ Vacuum failed for {path.name}: {v_err}")
