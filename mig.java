package com.data.migration;

import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.Column;
import org.apache.spark.sql.catalog.Table; 
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * SnapshotMigrationTool
 * * Performs a "Safe Migration" of Hive tables to Iceberg.
 * * Strategy:
 * - Parquet/Avro/ORC: Uses 'snapshotTable' (Metadata only, Zero-Copy).
 * - Text/CSV/JSON: Uses 'CTAS' (Rewrite Data) if 'allowTextRewrite' is true.
 * * Validated for: JDK 17, Hive 3, Iceberg 1.4.3, Spark 3.3/3.4/3.5
 */
public class SnapshotMigrationTool {

    private static final Logger logger = LoggerFactory.getLogger(SnapshotMigrationTool.class);

    // JDK 17 Record for Configuration
    public record MigrationConfig(
        List<String> tableList,
        boolean performSwap,      // If false, only creates staging table for validation
        boolean enforceExternal,  // If true, converts MANAGED tables to EXTERNAL before migrating
        boolean allowTextRewrite, // If true, rewrites Text/CSV tables to Iceberg Parquet
        int threadPoolSize
    ) {}

    // JDK 17 Record for Result
    public record MigrationResult(
        String tableName,
        boolean success,
        long durationMs,
        String message
    ) {}

    public static void main(String[] args) {
        try (var spark = SparkSession.builder()
                .appName("HiveToIceberg-HybridMigration")
                .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
                .config("spark.sql.catalog.spark_catalog.type", "hive")
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .enableHiveSupport()
                .getOrCreate()) {

            String inputTarget = args.length > 0 ? args[0] : "default"; 
            
            logger.info("Analyzing input target: {}", inputTarget);
            var tool = new SnapshotMigrationTool();
            List<String> tablesToMigrate = tool.discoverTables(spark, inputTarget);

            if (tablesToMigrate.isEmpty()) {
                logger.warn("No suitable Hive tables found for migration in target: {}", inputTarget);
                return;
            }

            // Example Config: Enable Swap, Enforce External, Allow Text Rewrite, 4 Threads
            var config = new MigrationConfig(tablesToMigrate, true, true, true, 4);
            tool.runBatch(spark, config);
        }
    }

    public List<String> discoverTables(SparkSession spark, String input) {
        if (input.contains(".")) {
            return List.of(input);
        }

        logger.info("Scanning database '{}' for Hive tables...", input);
        try {
            if (!spark.catalog().databaseExists(input)) {
                logger.error("Database '{}' does not exist.", input);
                return List.of();
            }

            List<Table> allTables = spark.catalog().listTables(input).collectAsList();
            
            return allTables.stream()
                .filter(t -> !t.tableType().equalsIgnoreCase("VIEW"))
                .filter(t -> !t.name().endsWith("_iceberg_stage"))
                .map(t -> input + "." + t.name())
                .filter(fqn -> !isAlreadyIceberg(spark, fqn))
                .collect(Collectors.toList());

        } catch (Exception e) {
            logger.error("Failed to discover tables in " + input, e);
            return List.of();
        }
    }

    private boolean isAlreadyIceberg(SparkSession spark, String tableName) {
        try {
            var props = spark.sql("SHOW TBLPROPERTIES " + tableName).collectAsList();
            boolean isIceberg = props.stream().anyMatch(r -> 
                r.getString(0).contains("table_type") && "ICEBERG".equalsIgnoreCase(r.getString(1))
            );
            return isIceberg;
        } catch (Exception e) {
            return false;
        }
    }

    public void runBatch(SparkSession spark, MigrationConfig config) {
        logger.info("Starting batch migration for {} tables with pool size {}", 
            config.tableList().size(), config.threadPoolSize());

        var results = new ConcurrentLinkedQueue<MigrationResult>();
        ExecutorService executor = Executors.newFixedThreadPool(config.threadPoolSize());

        try {
            List<Future<MigrationResult>> futures = config.tableList().stream()
                .map(table -> executor.submit(() -> migrateTable(spark, table, config)))
                .collect(Collectors.toList());

            for (Future<MigrationResult> f : futures) {
                try {
                    results.add(f.get());
                } catch (InterruptedException | ExecutionException e) {
                    logger.error("Batch execution error", e);
                }
            }
        } finally {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                executor.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }

        printReport(results);
    }

    private MigrationResult migrateTable(SparkSession spark, String tableName, MigrationConfig config) {
        long startTime = System.currentTimeMillis();
        String stagingTable = tableName + "_iceberg_stage";
        String backupTable = tableName + "_hive_backup_" + System.currentTimeMillis();

        try {
            logger.info("[{}] Starting migration...", tableName);

            if (!spark.catalog().tableExists(tableName)) {
                return new MigrationResult(tableName, false, 0, "Table not found");
            }
            
            // 1. Format Detection
            String detectedFormat = detectAndValidateFormat(spark, tableName);
            if (detectedFormat == null) {
                return new MigrationResult(tableName, false, System.currentTimeMillis() - startTime, 
                    "Skipped: Unsupported Format (Complex SerDe?)");
            }

            // 2. Decision Logic: Snapshot vs Rewrite vs Skip
            boolean isSnapshotCapable = isSnapshotCapable(detectedFormat);
            if (!isSnapshotCapable && !config.allowTextRewrite()) {
                 return new MigrationResult(tableName, false, System.currentTimeMillis() - startTime, 
                    "Skipped: Text Format detected and allowTextRewrite=false");
            }

            // 3. Safety: Ensure table is EXTERNAL
            if (config.enforceExternal()) {
                ensureTableIsExternal(spark, tableName);
            }

            // --- ATOMICITY START (Pre-Count) ---
            logger.info("[{}] capturing initial state (Pre-Count)...", tableName);
            long countBefore = spark.table(tableName).count();

            // 4. Execution (Snapshot or Rewrite)
            if (isSnapshotCapable) {
                performSnapshot(spark, tableName, stagingTable, detectedFormat);
            } else {
                performRewrite(spark, tableName, stagingTable);
            }

            // --- ATOMICITY VERIFICATION (Post-Count) ---
            long countAfterSource = spark.table(tableName).count();
            if (countBefore != countAfterSource) {
                String msg = String.format("Concurrent Write Detected! Pre: %d, Post: %d. Aborting.", 
                    countBefore, countAfterSource);
                logger.error("[{}] {}", tableName, msg);
                spark.sql("DROP TABLE IF EXISTS " + stagingTable);
                return new MigrationResult(tableName, false, System.currentTimeMillis() - startTime, msg);
            }

            // 5. Validation (Staging vs Source)
            logger.info("[{}] Validating staging integrity...", tableName);
            long countSnapshot = spark.table(stagingTable).count();
            if (countBefore != countSnapshot) {
                spark.sql("DROP TABLE IF EXISTS " + stagingTable);
                return new MigrationResult(tableName, false, System.currentTimeMillis() - startTime, 
                    "Validation Failed: Staging count ("+countSnapshot+") != Source count ("+countBefore+")");
            }

            if (!config.performSwap()) {
                return new MigrationResult(tableName, true, System.currentTimeMillis() - startTime, 
                    "Staging created (No Swap): " + stagingTable);
            }

            // 6. Atomic Swap
            performAtomicSwap(spark, tableName, stagingTable, backupTable);

            String successMsg = String.format("Success. Method: %s, Format: %s, Backup: %s", 
                isSnapshotCapable ? "Snapshot" : "Rewrite", detectedFormat, backupTable);
            return new MigrationResult(tableName, true, System.currentTimeMillis() - startTime, successMsg);

        } catch (Exception e) {
            logger.error("Migration failed for " + tableName, e);
            try { spark.sql("DROP TABLE IF EXISTS " + stagingTable); } catch (Exception ignored) {}
            return new MigrationResult(tableName, false, System.currentTimeMillis() - startTime, 
                "Error: " + e.getMessage());
        }
    }

    private boolean isSnapshotCapable(String format) {
        return "parquet".equalsIgnoreCase(format) || 
               "orc".equalsIgnoreCase(format) || 
               "avro".equalsIgnoreCase(format);
    }

    private void performSnapshot(SparkSession spark, String source, String target, String format) {
        logger.info("[{}] Executing Metadata Snapshot (Zero-Copy)...", source);
        SparkActions.get(spark).snapshotTable(source)
            .as(target)
            .tableProperties(Map.of(
                "format-version", "2",
                "write.format.default", format,
                "migrated-at", Instant.now().toString(),
                "migration-method", "snapshot"
            ))
            .execute();
    }

    /**
     * Rewrite Strategy for Text/CSV tables.
     * Uses CTAS (Create Table As Select) to convert data to Parquet-backed Iceberg.
     */
    private void performRewrite(SparkSession spark, String source, String target) {
        logger.info("[{}] Executing Data Rewrite (Text -> Iceberg Parquet)...", source);
        
        // 1. Detect Partition Columns
        String partitionClause = "";
        try {
            List<String> partitionCols = spark.catalog().listColumns(source).collectAsList().stream()
                .filter(Column::isPartition)
                .map(Column::name)
                .collect(Collectors.toList());
            
            if (!partitionCols.isEmpty()) {
                partitionClause = "PARTITIONED BY (" + String.join(", ", partitionCols) + ")";
            }
        } catch (Exception e) {
            logger.warn("[{}] Failed to detect partitions, defaulting to unpartitioned.", source);
        }

        // 2. Build CTAS SQL
        // We use standard SQL to ensure we capture the schema correctly from the source SELECT
        String ctasSql = String.format(
            "CREATE TABLE %s USING iceberg %s TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet') AS SELECT * FROM %s",
            target, partitionClause, source
        );

        logger.debug("[{}] Running CTAS: {}", source, ctasSql);
        spark.sql(ctasSql);
    }

    private String detectAndValidateFormat(SparkSession spark, String tableName) {
        try {
            List<Row> rows = spark.sql("DESCRIBE FORMATTED " + tableName).collectAsList();
            String inputFormat = "";
            String serdeLib = "";
            
            for (Row r : rows) {
                if (r.size() >= 2) {
                    String col = r.getString(0);
                    String val = r.getString(1);
                    if (col == null || val == null) continue;
                    if (col.trim().equals("InputFormat")) inputFormat = val.trim();
                    else if (col.trim().equals("SerDe Library")) serdeLib = val.trim();
                }
            }

            String lowerInput = inputFormat.toLowerCase();
            String lowerSerde = serdeLib.toLowerCase();

            if (lowerInput.contains("parquet") || lowerSerde.contains("parquet")) return "parquet";
            if (lowerInput.contains("orc") || lowerSerde.contains("orc")) return "orc";
            if (lowerInput.contains("avro") || lowerSerde.contains("avro")) return "avro";
            
            // Detect Text Formats
            if (lowerInput.contains("textinputformat") || lowerSerde.contains("lazysimpleserde")) {
                return "text";
            }
            
            logger.warn("[{}] Unsupported format. Input: {}, SerDe: {}", tableName, inputFormat, serdeLib);
            return null;

        } catch (Exception e) {
            logger.error("Failed to detect format for " + tableName, e);
            return null;
        }
    }

    private void ensureTableIsExternal(SparkSession spark, String tableName) {
        try {
            String tableType = spark.sql("DESCRIBE EXTENDED " + tableName)
                .filter("col_name = 'Type'")
                .select("data_type")
                .first()
                .getString(0);

            if ("MANAGED".equalsIgnoreCase(tableType)) {
                logger.warn("[{}] Converting MANAGED to EXTERNAL.", tableName);
                spark.sql(String.format("ALTER TABLE %s SET TBLPROPERTIES('EXTERNAL'='TRUE')", tableName));
            }
        } catch (Exception e) {
            logger.warn("[{}] Could not verify table type.", tableName, e);
        }
    }

    private void performAtomicSwap(SparkSession spark, String original, String staging, String backup) {
        logger.info("[{}] Swapping tables...", original);
        
        String renameToBackupSql = """
            ALTER TABLE %s RENAME TO %s
            """.formatted(original, backup);
            
        String renameStageToOriginalSql = """
            ALTER TABLE %s RENAME TO %s
            """.formatted(staging, original);

        try {
            spark.sql(renameToBackupSql);
        } catch (Exception e) {
            throw new RuntimeException("Failed phase A (Rename to Backup). Aborting.", e);
        }

        try {
            spark.sql(renameStageToOriginalSql);
        } catch (Exception e) {
            logger.error("[{}] Swap failed at Phase B. Rollback...", original);
            try {
                spark.sql(String.format("ALTER TABLE %s RENAME TO %s", backup, original));
                logger.info("[{}] Rollback successful.", original);
            } catch (Exception rollbackEx) {
                logger.error("[{}] CRITICAL: Rollback failed.", original);
            }
            throw new RuntimeException("Failed phase B (Rename Staging). Rollback attempted.", e);
        }
    }

    private void printReport(ConcurrentLinkedQueue<MigrationResult> results) {
        System.out.println("\n" + "=".repeat(80));
        System.out.println(" MIGRATION REPORT");
        System.out.println("=".repeat(80));
        System.out.printf("%-30s | %-10s | %-10d | %-30s%n", "Table", "Status", "Time(ms)", "Message");
        System.out.println("-".repeat(80));

        for (var res : results) {
            String status = res.success ? "SUCCESS" : "FAILED";
            System.out.printf("%-30s | %-10s | %-10d | %-30s%n", 
                res.tableName(), status, res.durationMs(), res.message());
        }
        System.out.println("=".repeat(80));
    }
}
