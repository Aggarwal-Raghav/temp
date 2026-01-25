package com.example.mcp.service;

import com.example.mcp.config.KerberosConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.tool.annotation.Tool;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Service exposing Iceberg operations.
 * Operations are wrapped in Kerberos doAs blocks via KerberosConfig.
 * Uses Spring AI @Tool annotations to automatically expose methods to the MCP server.
 */
@Service
public class IcebergToolService {

    private static final Logger log = LoggerFactory.getLogger(IcebergToolService.class);

    private final KerberosConfig kerberosConfig;
    private final Configuration hadoopConfiguration;

    @Value("${app.iceberg.catalog-uri}")
    private String catalogUri;

    @Value("${app.iceberg.warehouse-location}")
    private String warehouseLocation;

    private Catalog catalog;

    public IcebergToolService(KerberosConfig kerberosConfig, Configuration hadoopConfiguration) {
        this.kerberosConfig = kerberosConfig;
        this.hadoopConfiguration = hadoopConfiguration;
    }

    /**
     * Lazy initialization of the catalog inside a privileged action
     */
    private synchronized Catalog getCatalog() {
        if (catalog == null) {
            catalog = kerberosConfig.doAs(() -> {
                log.info("Initializing HiveCatalog connecting to {}", catalogUri);
                HiveCatalog hiveCatalog = new HiveCatalog();
                hiveCatalog.setConf(hadoopConfiguration);

                Map<String, String> properties = new HashMap<>();
                properties.put(CatalogProperties.URI, catalogUri);
                properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouseLocation);
                properties.put(CatalogProperties.CLIENT_POOL_SIZE, "5");
                
                hiveCatalog.initialize("hive", properties);
                return hiveCatalog;
            });
        }
        return catalog;
    }

    @Tool(description = "List all Iceberg tables available within a specific namespace (database).")
    public List<String> listTables(String namespaceName) {
        return kerberosConfig.doAs(() -> {
            Namespace namespace = Namespace.of(namespaceName);
            try {
                List<TableIdentifier> tables = getCatalog().listTables(namespace);
                return tables.stream()
                        .map(TableIdentifier::name)
                        .collect(Collectors.toList());
            } catch (Exception e) {
                log.error("Error listing tables in namespace {}", namespaceName, e);
                throw new RuntimeException("Failed to list tables: " + e.getMessage());
            }
        });
    }

    @Tool(description = "Retrieve metadata (schema, location, properties, current snapshot) for a specific Iceberg table.")
    public Map<String, Object> getTableMetadata(String namespace, String tableName) {
        return kerberosConfig.doAs(() -> {
            TableIdentifier id = TableIdentifier.of(namespace, tableName);
            try {
                Table table = getCatalog().loadTable(id);
                Map<String, Object> metadata = new HashMap<>();
                metadata.put("location", table.location());
                metadata.put("schema", table.schema().toString());
                metadata.put("spec", table.spec().toString());
                metadata.put("properties", table.properties());
                
                // Add snapshot summary if available
                if (table.currentSnapshot() != null) {
                    metadata.put("currentSnapshotId", table.currentSnapshot().snapshotId());
                    metadata.put("summary", table.currentSnapshot().summary());
                }
                
                return metadata;
            } catch (Exception e) {
                log.error("Error getting metadata for table {}.{}", namespace, tableName, e);
                throw new RuntimeException("Table not found or inaccessible: " + e.getMessage());
            }
        });
    }
}
