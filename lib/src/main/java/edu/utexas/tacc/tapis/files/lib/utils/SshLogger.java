package edu.utexas.tacc.tapis.files.lib.utils;

import edu.utexas.tacc.tapis.files.lib.dao.stats.SSHSessionLogDAO;
import edu.utexas.tacc.tapis.files.lib.dao.transfers.DAOTransactionContext;
import edu.utexas.tacc.tapis.files.lib.models.managementStats.ConnectionDetails;
import edu.utexas.tacc.tapis.shared.ssh.SshSessionPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import java.util.concurrent.TimeUnit;

public class SshLogger {
    private static final Logger log = LoggerFactory.getLogger(SshLogger.class);
    private ScheduledExecutorService loggerExecutorService = Executors.newSingleThreadScheduledExecutor();
    private ScheduledFuture<?> loggerTaskFuture = null;
    public final long SSH_LOGGER_SHUTDOWN_TIMEOUT_MILLISECONDS = 5000;
    private final String contextName;
    public SshLogger(String contextName) {
        this.contextName = contextName;
    }
    public void start() {
        loggerTaskFuture = loggerExecutorService.scheduleAtFixedRate(() -> {
            try {
                SSHSessionLogDAO msDao = new SSHSessionLogDAO();
                DAOTransactionContext.doInTransaction(tx -> {
                    msDao.deleteOlderThan(tx, contextName, Instant.now().minus(5, ChronoUnit.MINUTES));
                    ConnectionDetails details = new ConnectionDetails();
                    details.setContextName(contextName);
                    details.setSessionPoolDetails(SshSessionPool.getInstance().getDetailsAsJson(false));
                    return msDao.saveSshStats(tx, details);
                });
            } catch (Throwable th) {
                String msg = LibUtils.getMsg("SSH_LOGGER_ERROR");
                log.warn(msg, th);
            }
        }, 0, 1, TimeUnit.MINUTES);
    }
    public void shutdown() {
        log.info(LibUtils.getMsg("SSH_LOGGER_SHUTDOWN"));
        if (loggerTaskFuture != null) {
            loggerTaskFuture.cancel(true);
        }
        loggerExecutorService.shutdown();

        try {
            loggerExecutorService.awaitTermination(SSH_LOGGER_SHUTDOWN_TIMEOUT_MILLISECONDS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ex) {
            log.warn(LibUtils.getMsg("SSH_LOGGER_ERROR", ex.getMessage()), ex);
        } finally {
            if (!loggerExecutorService.isShutdown()) {
                loggerExecutorService.shutdownNow();
            }
        }

        log.warn(LibUtils.getMsg("SSH_LOGGER_SHUTDOWN_COMPLETE"));
    }
}
