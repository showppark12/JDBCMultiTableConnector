package com.inspien.connect;

import com.inspien.connect.source.MultiTableSourceConnectorConfig;
import com.inspien.connect.source.MultiTableSourceTask;
import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialects;
import io.confluent.connect.jdbc.source.JdbcSourceTaskConfig;
import io.confluent.connect.jdbc.source.TableMonitorThread;
import io.confluent.connect.jdbc.util.CachedConnectionProvider;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.TableId;
import io.confluent.connect.jdbc.util.Version;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class MultiTableSourceConnector extends SourceConnector {

    private static final Logger log = LoggerFactory.getLogger(MultiTableSourceConnector.class);

    private static final long MAX_TIMEOUT = 10000L;

    private Map<String, String> configProperties;
    private MultiTableSourceConnectorConfig config;
    private CachedConnectionProvider cachedConnectionProvider;
    private TableMonitorThread tableMonitorThread;
    private DatabaseDialect dialect;

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> properties) {
        log.info("Starting JDBC Source Connector");

        try {
            configProperties = properties;
            config = new MultiTableSourceConnectorConfig(configProperties);
        } catch (ConfigException e) {
            throw new ConnectException("Couldn't start JdbcSourceConnector due to configuration error",
                    e);
        }

        final String dbUrl = config.getString(MultiTableSourceConnectorConfig.CONNECTION_URL_CONFIG);
        final int maxConnectionAttempts = config.getInt(
                MultiTableSourceConnectorConfig.CONNECTION_ATTEMPTS_CONFIG
        );
        final long connectionRetryBackoff = config.getLong(
                MultiTableSourceConnectorConfig.CONNECTION_BACKOFF_CONFIG
        );
        dialect = DatabaseDialects.findBestFor(
                dbUrl,
                config
        );

        cachedConnectionProvider = connectionProvider(maxConnectionAttempts, connectionRetryBackoff);

        // Initial connection attempt
        cachedConnectionProvider.getConnection();

        long tablePollMs = config.getLong(MultiTableSourceConnectorConfig.TABLE_POLL_INTERVAL_MS_CONFIG);
        List<String> whitelist = config.getList(MultiTableSourceConnectorConfig.TABLE_WHITELIST_CONFIG);
        Set<String> whitelistSet = whitelist.isEmpty() ? null : new HashSet<>(whitelist);
        List<String> blacklist = config.getList(MultiTableSourceConnectorConfig.TABLE_BLACKLIST_CONFIG);
        Set<String> blacklistSet = blacklist.isEmpty() ? null : new HashSet<>(blacklist);

        if (whitelistSet != null && blacklistSet != null) {
            throw new ConnectException(MultiTableSourceConnectorConfig.TABLE_WHITELIST_CONFIG + " and "
                    + MultiTableSourceConnectorConfig.TABLE_BLACKLIST_CONFIG + " are "
                    + "exclusive.");
        }

        tableMonitorThread = new TableMonitorThread(
                dialect,
                cachedConnectionProvider,
                context,
                tablePollMs,
                whitelistSet,
                blacklistSet
        );
        tableMonitorThread.start();
    }


    protected CachedConnectionProvider connectionProvider(int maxConnAttempts, long retryBackoff) {
        return new CachedConnectionProvider(dialect, maxConnAttempts, retryBackoff);
    }

    @Override
    public Class<? extends Task> taskClass() {
        return MultiTableSourceTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> taskConfigs;

        List<TableId> currentTables = tableMonitorThread.tables();
        if (currentTables.isEmpty()) {
            taskConfigs = Collections.emptyList();
            log.warn("No tasks will be run because no tables were found");
        } else {
            // 입력받은 테이블의 사이즈와 직접 지정해준 maxTasks 사이즈 중 작은 것으로 태스크의 개수를 정한다.
            int numGroups = Math.min(currentTables.size(), maxTasks);
            List<List<TableId>> tablesGrouped =
                    ConnectorUtils.groupPartitions(currentTables, numGroups);
            taskConfigs = new ArrayList<>(tablesGrouped.size());
            for (List<TableId> taskTables : tablesGrouped) {
                Map<String, String> taskProps = new HashMap<>(configProperties);
                ExpressionBuilder builder = dialect.expressionBuilder();
                builder.appendList().delimitedBy(",").of(taskTables);
                taskProps.put(JdbcSourceTaskConfig.TABLES_CONFIG, builder.toString());
                taskConfigs.add(taskProps);
            }
            log.trace(
                    "Producing task configs with no custom query for tables: {}",
                    currentTables.toArray()
            );
        }
        return taskConfigs;
    }

    @Override
    public void stop() {
        log.info("Stopping table monitoring thread");
        tableMonitorThread.shutdown();
        try {
            tableMonitorThread.join(MAX_TIMEOUT);
        } catch (InterruptedException e) {
            // Ignore, shouldn't be interrupted
        } finally {
            try {
                cachedConnectionProvider.close();
            } finally {
                try {
                    if (dialect != null) {
                        dialect.close();
                    }
                } catch (Throwable t) {
                    log.warn("Error while closing the {} dialect: ", dialect, t);
                } finally {
                    dialect = null;
                }
            }
        }
    }

    @Override
    public ConfigDef config() {
        return MultiTableSourceConnectorConfig.CONFIG_DEF;
    }
}
