package com.example.mcp.service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Service
public class KerberosAuthService {

    private static final Logger log = LoggerFactory.getLogger(KerberosAuthService.class);

    private final Configuration hadoopConfiguration;

    // We still allow explicit override, but also check the Hadoop Conf object
    @Value("${app.kerberos.enabled:true}") 
    private boolean kerberosEnabledFlag;

    @Value("${app.kerberos.principal}")
    private String principal;

    @Value("${app.kerberos.keytab-location}")
    private String keytabLocation;

    @Value("${app.kerberos.renewal-interval-ms:3600000}")
    private long renewalIntervalMs;

    private ScheduledExecutorService renewalExecutor;
    private boolean isKerberosActive = false;

    public KerberosAuthService(Configuration hadoopConfiguration) {
        this.hadoopConfiguration = hadoopConfiguration;
    }

    @PostConstruct
    public void initKerberos() throws IOException {
        // Check 1: Is it enabled in app.yml?
        // Check 2: Does the core-site.xml actually say 'kerberos'?
        String authenticationMethod = hadoopConfiguration.get("hadoop.security.authentication", "simple");
        
        if (!kerberosEnabledFlag || "simple".equalsIgnoreCase(authenticationMethod)) {
            log.info("Kerberos is disabled (app.kerberos.enabled={} or hadoop.security.authentication={}).", 
                     kerberosEnabledFlag, authenticationMethod);
            return;
        }

        log.info("Initializing Kerberos login for principal: {}", principal);
        
        // Ensure UGI uses the loaded configuration
        UserGroupInformation.setConfiguration(hadoopConfiguration);

        try {
            UserGroupInformation.loginUserFromKeytab(principal, keytabLocation);
            log.info("Successfully logged in with keytab for user: {}", UserGroupInformation.getLoginUser().getUserName());
            isKerberosActive = true;
        } catch (IOException e) {
            log.error("Failed to login via Kerberos keytab", e);
            throw e;
        }

        startRenewalThread();
    }

    private void startRenewalThread() {
        if (!isKerberosActive) return;

        renewalExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "kerberos-renewer");
            t.setDaemon(true);
            return t;
        });

        renewalExecutor.scheduleAtFixedRate(() -> {
            try {
                UserGroupInformation.getLoginUser().checkTGTAndReloginFromKeytab();
                log.debug("Kerberos TGT check/renewal completed successfully.");
            } catch (IOException e) {
                log.error("Failed to renew Kerberos ticket", e);
            }
        }, renewalIntervalMs, renewalIntervalMs, TimeUnit.MILLISECONDS);
    }

    public <T> T doAs(PrivilegedAction<T> action) {
        if (!isKerberosActive) {
            return action.run();
        }
        try {
            return UserGroupInformation.getLoginUser().doAs(action);
        } catch (IOException e) {
            throw new RuntimeException("Failed to get login user for doAs", e);
        }
    }

    @PreDestroy
    public void shutdown() {
        if (renewalExecutor != null) {
            renewalExecutor.shutdownNow();
        }
    }
}
