from fastmcp import FastMCP
from service import IcebergService
from models import TableMetadata, SnapshotInfo
from typing import List, Dict

# Initialize Service
svc = IcebergService()

# Initialize Server
mcp = FastMCP("Iceberg Data Ops")

@mcp.tool()
def list_databases() -> List[str]:
    """List all available databases (namespaces) in the catalog."""
    return svc.get_all_databases()

@mcp.tool()
def list_tables(database_name: str) -> List[str]:
    """
    List all tables in a specific database.
    Returns a list of full table names (e.g., 'db.table') capable of being passed to other tools.
    """
    tables = svc.list_tables_in_namespace(database_name)
    return [t.full_name for t in tables]

@mcp.tool()
def get_table_snapshot_id(full_table_name: str) -> int:
    """
    Get the current snapshot ID for a specific table.
    Use this to find the version identifier required for time-travel queries.
    """
    meta = svc.get_table_metadata(full_table_name)
    if meta.current_snapshot_id:
        return meta.current_snapshot_id
    return -1 # Or raise error depending on preference

@mcp.tool()
def analyze_table_types(database_name: str = None) -> Dict[str, int]:
    """
    Count the number of Merge-on-Read (MOR) vs Copy-on-Write (COW) tables.
    If database_name is omitted, scans the entire catalog.
    """
    return svc.get_table_type_distribution(database_name)

@mcp.tool()
def get_table_details(full_table_name: str) -> str:
    """
    Get comprehensive metadata about a table including location, partition keys, 
    and properties. Returns a JSON string summary.
    """
    meta = svc.get_table_metadata(full_table_name)
    return meta.model_dump_json(indent=2)

if __name__ == "__main__":
    mcp.run()
