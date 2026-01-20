from pyiceberg.catalog import load_catalog, Catalog
from pyiceberg.table import Table
from typing import List, Dict
from config import settings
from models import (
    TableIdentifier, 
    TableMetadata, 
    SnapshotInfo, 
    TableType, 
    DatabaseSummary
)

class IcebergService:
    def __init__(self):
        self.catalog: Catalog = load_catalog(
            settings.CATALOG_NAME, 
            **settings.get_pyiceberg_properties()
        )

    def _determine_table_type(self, properties: Dict[str, str]) -> TableType:
        # Logic to determine MOR vs COW based on write modes
        write_mode = properties.get("write.delete.mode", "copy-on-write")
        update_mode = properties.get("write.update.mode", "copy-on-write")
        merge_mode = properties.get("write.merge.mode", "copy-on-write")
        
        if "merge-on-read" in [write_mode, update_mode, merge_mode]:
            return TableType.MOR
        return TableType.COW

    def get_all_databases(self) -> List[str]:
        """Returns a simple list of namespace names."""
        return [ns[0] for ns in self.catalog.list_namespaces()]

    def get_database_details(self, namespace: str) -> DatabaseSummary:
        """Returns details about a specific database/namespace."""
        tables = self.catalog.list_tables(namespace)
        # Note: Catalog.load_namespace_properties might be needed for location
        return DatabaseSummary(
            name=namespace,
            table_count=len(tables)
        )

    def list_tables_in_namespace(self, namespace: str) -> List[TableIdentifier]:
        identifiers = self.catalog.list_tables(namespace)
        return [
            TableIdentifier(
                namespace=ident[0], 
                name=ident[1], 
                full_name=f"{ident[0]}.{ident[1]}"
            )
            for ident in identifiers
        ]

    def get_table_metadata(self, full_table_name: str) -> TableMetadata:
        """
        Loads table and returns enriched metadata.
        Args:
            full_table_name: format 'namespace.table'
        """
        table: Table = self.catalog.load_table(full_table_name)
        
        # Extract partition fields
        partitions = [f.name for f in table.schema().fields if f.field_id in [p.source_id for p in table.spec().fields]]
        
        return TableMetadata(
            identifier=TableIdentifier(
                namespace=table.identifier[0],
                name=table.identifier[1],
                full_name=full_table_name
            ),
            location=table.location(),
            table_type=self._determine_table_type(table.properties),
            current_snapshot_id=table.current_snapshot().snapshot_id if table.current_snapshot() else None,
            partition_fields=partitions,
            properties=table.properties
        )

    def get_snapshot_details(self, full_table_name: str, snapshot_id: int = None) -> SnapshotInfo:
        table = self.catalog.load_table(full_table_name)
        snapshot = table.snapshot_by_id(snapshot_id) if snapshot_id else table.current_snapshot()
        
        if not snapshot:
            raise ValueError(f"Snapshot not found for {full_table_name}")

        return SnapshotInfo(
            snapshot_id=snapshot.snapshot_id,
            parent_snapshot_id=snapshot.parent_snapshot_id,
            timestamp_ms=snapshot.timestamp_ms,
            manifest_list=snapshot.manifest_list
        )

    def get_table_type_distribution(self, namespace: str = None) -> Dict[str, int]:
        """Counts MOR vs COW tables."""
        namespaces = [namespace] if namespace else self.get_all_databases()
        stats = {TableType.COW.value: 0, TableType.MOR.value: 0}
        
        for ns in namespaces:
            try:
                tables = self.list_tables_in_namespace(ns)
                for t_ident in tables:
                    try:
                        meta = self.get_table_metadata(t_ident.full_name)
                        stats[meta.table_type.value] += 1
                    except Exception:
                        continue # Skip tables that fail to load
            except Exception:
                continue
                
        return stats
