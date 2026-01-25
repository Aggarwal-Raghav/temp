package com.example.mcp.config;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import java.io.File;

@org.springframework.context.annotation.Configuration
public class HadoopConfig {

    private static final Logger log = LoggerFactory.getLogger(HadoopConfig.class);

    // Inject paths with standard cluster defaults.
    // Can be overridden in application.yml or via command line args.
    @Value("${app.hadoop.core-site:/etc/hadoop/conf/core-site.xml}")
    private String coreSitePath;

    @Value("${app.hadoop.hdfs-site:/etc/hadoop/conf/hdfs-site.xml}")
    private String hdfsSitePath;

    @Value("${app.hive.hive-site:/etc/hive/conf/hive-site.xml}")
    private String hiveSitePath;

    @Bean
    public Configuration hadoopConfiguration() {
        Configuration conf = new Configuration();
        
        // 1. Load standard Hadoop Configs
        addResourceIfExists(conf, coreSitePath);
        addResourceIfExists(conf, hdfsSitePath);
        
        // 2. Load Hive Config
        addResourceIfExists(conf, hiveSitePath);

        return conf;
    }

    private void addResourceIfExists(Configuration conf, String filePath) {
        File file = new File(filePath);
        if (file.exists() && !file.isDirectory()) {
            log.info("Loading configuration from: {}", filePath);
            // new Path("file://...") is important to tell Hadoop it's a local file
            conf.addResource(new Path("file://" + file.getAbsolutePath()));
        } else {
            log.debug("Configuration file not found (skipping): {}", filePath);
        }
    }
}
