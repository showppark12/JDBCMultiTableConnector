package com.inspien.connect.source;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialects;
import io.confluent.connect.jdbc.source.*;
import io.confluent.connect.jdbc.util.*;

import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;

import org.quartz.*;
import org.quartz.impl.StdSchedulerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.Calendar;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

public class MultiTableSourceTask extends SourceTask {



    // When no results, periodically return control flow to caller to give it a chance to pause us.
    private static final int CONSECUTIVE_EMPTY_RESULTS_BEFORE_RETURN = 3;

    private static final Logger log = LoggerFactory.getLogger(JdbcSourceTask.class);

    private Time time;
    private MultiTableSourceTaskConfig config;
    private DatabaseDialect dialect;
    private CachedConnectionProvider cachedConnectionProvider;
    private PriorityQueue<BulkTableQuerier> tableQueue = new PriorityQueue<>();
    private final AtomicBoolean running = new AtomicBoolean(false);
    public final AtomicBoolean waiting = new AtomicBoolean(true);
    private final AtomicLong taskThreadId = new AtomicLong(0);

    private TimestampMultiTableQuerier headerQuerier;

    private Scheduler scheduler;

    public MultiTableSourceTask() {
        this.time = new SystemTime();
    }

    public MultiTableSourceTask(Time time) {
        this.time = time;
    }

    // offset클래스의 상수들을 여기서 재정의
    static final String INCREMENTING_FIELD = "incrementing";
    static final String TIMESTAMP_FIELD = "timestamp";
    static final String TIMESTAMP_NANOS_FIELD = "timestamp_nanos";

    @Override
    public String version() {
        return Version.getVersion();
    }

    @Override
    public void start(Map<String, String> properties) {
        log.info("Starting JDBC source task");
        try {
            config = new MultiTableSourceTaskConfig(properties);
        } catch (ConfigException e) {
            throw new ConnectException("Couldn't start JdbcSourceTask due to configuration error", e);
        }

        // 설정파일에서 헤더테이블 설정을받아옴
        final String headerTable = config.getString(MultiTableSourceConnectorConfig.HEADER_TABLE_CONFIG);
        final String dataHeaderQuery = config.getString(MultiTableSourceConnectorConfig.HEADER_TABLE_QUERY_CONFIG);
        final String dataHeaderUpdateQuery = config.getString(MultiTableSourceConnectorConfig.HEADER_TABLE_UPDATE_QUERY_CONFIG);
        final String dataHeaderCompleteQuery = config.getString(MultiTableSourceConnectorConfig.HEADER_TABLE_COMPLETE_QUERY_CONFIG);

        final String url = config.getString(MultiTableSourceConnectorConfig.CONNECTION_URL_CONFIG);
        final int maxConnAttempts = config.getInt(MultiTableSourceConnectorConfig.CONNECTION_ATTEMPTS_CONFIG);
        final long retryBackoff = config.getLong(MultiTableSourceConnectorConfig.CONNECTION_BACKOFF_CONFIG);

        final String dialectName = config.getString(MultiTableSourceConnectorConfig.DIALECT_NAME_CONFIG);
        if (dialectName != null && !dialectName.trim().isEmpty()) {
            dialect = DatabaseDialects.create(dialectName, config);
        } else {
            dialect = DatabaseDialects.findBestFor(url, config);
        }
        log.info("Using JDBC dialect {}", dialect.name());

        cachedConnectionProvider = connectionProvider(maxConnAttempts, retryBackoff);

        List<String> tables = config.getList(MultiTableSourceTaskConfig.TABLES_CONFIG);

        if (tables.isEmpty()) {
            throw new ConnectException("Invalid configuration: each JdbcSourceTask must have at "
                    + "least one table assigned to it or one query specified");
        }

        List<String> detailTables = tables;

        String mode = config.getString(MultiTableSourceTaskConfig.MODE_CONFIG);
        //used only in table mode
        Map<String, List<Map<String, String>>> partitionsByTableFqn = new HashMap<>();
        Map<Map<String, String>, Map<String, Object>> offsets = null;
        if (mode.equals(MultiTableSourceTaskConfig.MODE_INCREMENTING)
                || mode.equals(MultiTableSourceTaskConfig.MODE_TIMESTAMP)
                || mode.equals(MultiTableSourceTaskConfig.MODE_TIMESTAMP_INCREMENTING)) {
            List<Map<String, String>> partitions = new ArrayList<>(tables.size());

            log.info("I AM MULTI_TABLE mode");
            for (String table : tables) {
                // Find possible partition maps for different offset protocols
                // We need to search by all offset protocol partition keys to support compatibility
                List<Map<String, String>> tablePartitions = possibleTablePartitions(table);
                partitions.addAll(tablePartitions);
                partitionsByTableFqn.put(table, tablePartitions);
            }

            offsets = context.offsetStorageReader().offsets(partitions);
            log.info("The partition offsets are {}", offsets);
        }

        // 스케쥴러 관련 코드
        JobDetail jobDetail = newJob(ExecutePoll.class).build();
        JobDataMap jobDataMap = jobDetail.getJobDataMap();
        jobDataMap.put("Thread", Thread.currentThread());
        log.info("start 에서 쓰레드아이디 : " + Thread.currentThread().getId());

        Trigger trigger = newTrigger().withSchedule(cronSchedule("0 * * * * ?")).build();

        try {
            scheduler = StdSchedulerFactory.getDefaultScheduler();
            scheduler.start();
            scheduler.scheduleJob(jobDetail, trigger);
        } catch (SchedulerException e) {
            e.printStackTrace();
        }

        String incrementingColumn
                = config.getString(MultiTableSourceTaskConfig.INCREMENTING_COLUMN_NAME_CONFIG);
        List<String> timestampColumns
                = config.getList(MultiTableSourceTaskConfig.TIMESTAMP_COLUMN_NAME_CONFIG);
        Long timestampDelayInterval
                = config.getLong(MultiTableSourceTaskConfig.TIMESTAMP_DELAY_INTERVAL_MS_CONFIG);
        boolean validateNonNulls
                = config.getBoolean(MultiTableSourceTaskConfig.VALIDATE_NON_NULL_CONFIG);
        TimeZone timeZone = config.timeZone();
        String suffix = config.getString(MultiTableSourceTaskConfig.QUERY_SUFFIX_CONFIG).trim();



        for (String table : detailTables) {
            final List<Map<String, String>> tablePartitionsToCheck;
            final Map<String, String> partition;

            if (validateNonNulls) {
                validateNonNullable(
                        mode,
                        table,
                        incrementingColumn,
                        timestampColumns
                );
            }
            tablePartitionsToCheck = partitionsByTableFqn.get(table);

            // The partition map varies by offset protocol. Since we don't know which protocol each
            // table's offsets are keyed by, we need to use the different possible partitions
            // (newest protocol version first) to find the actual offsets for each table.
            Map<String, Object> offset = null;
            if (offsets != null) {
                for (Map<String, String> toCheckPartition : tablePartitionsToCheck) {
                    offset = offsets.get(toCheckPartition);
                    if (offset != null) {
                        log.info("Found offset {} for partition {}", offsets, toCheckPartition);
                        break;
                    }
                }
            }

            offset = computeInitialOffset(table, offset, timeZone);

            String topicPrefix = config.topicPrefix();
            String [] arrayList = table.split("\\.");
            String tableName = arrayList[1].replace("`", "");
            log.info("tableName : " + tableName);

            // 헤더 테이블이면 헤더쿼리어에 직접 만든 쿼리어로 집어넣고
            if (tableName.equals(headerTable)) {
                headerQuerier = new TimestampMultiTableQuerier(
                        dialect,
                        table,
                        topicPrefix,
                        timestampColumns,
                        null,
                        offset,
                        timestampDelayInterval,
                        timeZone,
                        suffix,
                        headerTable,
                        dataHeaderQuery,
                        dataHeaderUpdateQuery,
                        dataHeaderCompleteQuery
                );
                continue;
            }

            // 디테일 테이블이면 테이블 큐에 벌크테이블 쿼리어로 집어넣는다.
            tableQueue.add(
                    new BulkTableQuerier(
                            dialect,
                            table,
                            topicPrefix,
                            suffix,
                            headerTable,
                            dataHeaderQuery,
                            dataHeaderUpdateQuery,
                            dataHeaderCompleteQuery
                    )
            );
        }

        running.set(true);
        taskThreadId.set(Thread.currentThread().getId());
        log.info("Started JDBC source task");
    }

    protected CachedConnectionProvider connectionProvider(int maxConnAttempts, long retryBackoff) {
        return new CachedConnectionProvider(dialect, maxConnAttempts, retryBackoff) {
            @Override
            protected void onConnect(final Connection connection) throws SQLException {
                super.onConnect(connection);
                connection.setAutoCommit(false);
            }
        };
    }

    //This method returns a list of possible partition maps for different offset protocols
    //This helps with the upgrades
    private List<Map<String, String>> possibleTablePartitions(String table) {
        TableId tableId = dialect.parseTableIdentifier(table);
        return Arrays.asList(
                OffsetProtocols.sourcePartitionForProtocolV1(tableId),
                OffsetProtocols.sourcePartitionForProtocolV0(tableId)
        );
    }

    protected Map<String, Object> computeInitialOffset(
            String tableOrQuery,
            Map<String, Object> partitionOffset,
            TimeZone timezone) {
        if (!(partitionOffset == null)) {
            return partitionOffset;
        } else {
            Map<String, Object> initialPartitionOffset = null;
            // no offsets found
            Long timestampInitial = config.getLong(JdbcSourceConnectorConfig.TIMESTAMP_INITIAL_CONFIG);
            if (timestampInitial != null) {
                // start at the specified timestamp
                if (timestampInitial == JdbcSourceConnectorConfig.TIMESTAMP_INITIAL_CURRENT) {
                    // use the current time
                    try {
                        final Connection con = cachedConnectionProvider.getConnection();
                        Calendar cal = Calendar.getInstance(timezone);
                        timestampInitial = dialect.currentTimeOnDB(con, cal).getTime();
                    } catch (SQLException e) {
                        throw new ConnectException("Error while getting initial timestamp from database", e);
                    }
                }
                initialPartitionOffset = new HashMap<String, Object>();
                initialPartitionOffset.put(TIMESTAMP_FIELD, timestampInitial);
                log.info("No offsets found for '{}', so using configured timestamp {}", tableOrQuery,
                        timestampInitial);
            }
            return initialPartitionOffset;
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        log.info("{} Polling for new data");

        log.info("poll 에서 쓰레드아이디 : " + Thread.currentThread().getId());
        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            e.printStackTrace();
            log.info("sleep 빠져나옴");
        }


        Map<TableQuerier, Integer> consecutiveEmptyResults = tableQueue.stream().collect(
                Collectors.toMap(Function.identity(), (q) -> 0));

        while (running.get()) {
            final BulkTableQuerier querier = tableQueue.peek();

            // 멀티테이블 모드일때 헤더테이블에서 검색해온 row들을 담아놓을 자료구조
            final List<SourceRecord> headerResults = new ArrayList<>();
            // 실제로 반환할 List
            final List<SourceRecord> results = new ArrayList<>();

            try {
                // 헤더테이블을 위한 쿼리를 준비하고 실행해서 Results Set을 받아온다.
                headerQuerier.maybeStartQuery(cachedConnectionProvider.getConnection());
                int batchMaxRows = config.getInt(JdbcSourceTaskConfig.BATCH_MAX_ROWS_CONFIG);
                boolean hadNext = true;
                while (headerResults.size() < batchMaxRows && (hadNext = headerQuerier.next())) {
                    headerResults.add(headerQuerier.extractRecord());
                }

                // 잘받아왔나 일단 찍어보는것 삭제 예정
                for (SourceRecord headerResult : headerResults) {
                    log.info("헤더 Results : " + headerResult.value());
                }

                headerQuerier.resultSet = null;
                headerQuerier.stmt = null;

            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }

            List<Struct> structs = new ArrayList<>();
            Schema recordListSchema = null;

            for (SourceRecord headerResult : headerResults) {
                // 키 row 로 가질 struct를 querier의 headerStruct 변수에 집어넣는다.
                querier.headerStruct = (Struct) headerResult.value();
                try {
                    log.info("Checking for next block of results from {}", querier.toString());
                    // 키 row로 현재 테이블에서 검색할 쿼리문을 준비한다.
                    querier.maybeStartQuery(cachedConnectionProvider.getConnection());

                    recordListSchema = SchemaBuilder.struct()
                            .field("header", querier.headerStruct.schema())
                            .field("detail", SchemaBuilder.array(querier.schemaMapping.schema()).build())
                            .build();

                    int batchMaxRows = config.getInt(JdbcSourceTaskConfig.BATCH_MAX_ROWS_CONFIG);

                    SourceRecord sourceRecord = querier.extractRecordInMultiMode(batchMaxRows, recordListSchema);

                    // key row로 테이블을 SELECT했는데 데이터가 없으면 results 에 add하지 않는다.
                    if (!((Struct) sourceRecord.value()).getArray("detail").isEmpty()) {
                        structs.add((Struct) sourceRecord.value());
                    }

                    // resultSet 초기화
                    querier.resultSet = null;
                    querier.stmt = null;
                } catch (SQLNonTransientException sqle) {
                    log.error("Non-transient SQL exception while running query for table: {}",
                            querier, sqle);
                    resetAndRequeueHead(querier);
                    // This task has failed, so close any resources (may be reopened if needed) before throwing
                    closeResources();
                    throw new ConnectException(sqle);
                } catch (SQLException sqle) {
                    log.error("SQL exception while running query for table: {}", querier, sqle);
                    resetAndRequeueHead(querier);
                    return null;
                } catch (Throwable t) {
                    log.error("Failed to run query for table: {}", querier, t);
                    resetAndRequeueHead(querier);
                    // This task has failed, so close any resources (may be reopened if needed) before throwing
                    closeResources();
                    throw t;
                }
            }
            log.info("모든 헤더테이블 로우로 detail 테이블 긁어온 결과를 add 해놨다. " + structs.toString());

            Schema finalSchema = SchemaBuilder.struct()
                    .field("data", SchemaBuilder.array(structs.get(0).schema()).build())
                    .build();
            Struct finalStruct = new Struct(finalSchema);
            log.info("final struct : " + finalStruct);
            log.info("final struct : " + finalStruct.schema());

            finalStruct.put("data", structs);
            log.info("final Struct : " + finalStruct.toString());

            final String topic;
            final Map<String, String> partition;

            partition = Collections.singletonMap(JdbcSourceConnectorConstants.TABLE_NAME_KEY, querier.tableId.tableName());
            topic = querier.topicPrefix + querier.tableId.tableName();

            results.add(new SourceRecord(partition, null, topic, finalSchema, finalStruct));

            // If we finished processing the results from the current query, we can reset and send
            // the querier to the tail of the queue
            resetAndRequeueHead(querier);

            // 테이블 다돌았는데 리절츠가 없을 때
            if (results.isEmpty()) {
                consecutiveEmptyResults.compute(querier, (k, v) -> v + 1);
                log.info("No updates for {}", querier.toString());

                if (Collections.min(consecutiveEmptyResults.values())
                        >= CONSECUTIVE_EMPTY_RESULTS_BEFORE_RETURN) {
                    log.info("More than " + CONSECUTIVE_EMPTY_RESULTS_BEFORE_RETURN
                            + " consecutive empty results for all queriers, returning");
                    return null;
                } else {
                    continue;
                }
            } else {
                consecutiveEmptyResults.put(querier, 0);
            }

            log.info("Returning {} records for {}", results.size(), querier);

            // 여기서 Y로 바꿔줘야지 바꿔주는 함수를 여기서 호출해야겠네
            try {
                headerQuerier.updateAfterExcuteQuery();
            } catch (SQLException throwables) {
                log.error("error occur when updated y");
                try {
                    headerQuerier.db.rollback();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                resetAndRequeueHead(querier);
                // This task has failed, so close any resources (may be reopened if needed) before throwing
                closeResources();
            }

            try {
                headerQuerier.db.commit();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }
            return results;
        }

        shutdown();
        return null;
    }

    @Override
    public void stop() {

    }

    protected void closeResources() {
        log.info("Closing resources for JDBC source task");
        try {
            if (cachedConnectionProvider != null) {
                cachedConnectionProvider.close();
            }
        } catch (Throwable t) {
            log.warn("Error while closing the connections", t);
        } finally {
            cachedConnectionProvider = null;
            try {
                if (dialect != null) {
                    dialect.close();
                }
            } catch (Throwable t) {
                log.warn("Error while closing the {} dialect: ", dialect.name(), t);
            } finally {
                dialect = null;
            }
        }
    }

    private void shutdown() {
        final BulkTableQuerier querier = tableQueue.peek();
        if (querier != null) {
            resetAndRequeueHead(querier);
        }
        closeResources();
    }

    private void resetAndRequeueHead(BulkTableQuerier expectedHead) {
        log.debug("Resetting querier {}", expectedHead.toString());
        TableQuerier removedQuerier = tableQueue.poll();
        assert removedQuerier == expectedHead;
        expectedHead.reset(time.milliseconds());
        tableQueue.add(expectedHead);
    }


    private void validateNonNullable(
            String incrementalMode,
            String table,
            String incrementingColumn,
            List<String> timestampColumns
    ) {
        try {
            Set<String> lowercaseTsColumns = new HashSet<>();
            for (String timestampColumn: timestampColumns) {
                lowercaseTsColumns.add(timestampColumn.toLowerCase(Locale.getDefault()));
            }

            boolean incrementingOptional = false;
            boolean atLeastOneTimestampNotOptional = false;
            final Connection conn = cachedConnectionProvider.getConnection();
            boolean autoCommit = conn.getAutoCommit();
            try {
                conn.setAutoCommit(true);
                Map<ColumnId, ColumnDefinition> defnsById = dialect.describeColumns(conn, table, null);
                for (ColumnDefinition defn : defnsById.values()) {
                    String columnName = defn.id().name();
                    if (columnName.equalsIgnoreCase(incrementingColumn)) {
                        incrementingOptional = defn.isOptional();
                    } else if (lowercaseTsColumns.contains(columnName.toLowerCase(Locale.getDefault()))) {
                        if (!defn.isOptional()) {
                            atLeastOneTimestampNotOptional = true;
                        }
                    }
                }
            } finally {
                conn.setAutoCommit(autoCommit);
            }

            // Validate that requested columns for offsets are NOT NULL. Currently this is only performed
            // for table-based copying because custom query mode doesn't allow this to be looked up
            // without a query or parsing the query since we don't have a table name.
            if ((incrementalMode.equals(MultiTableSourceConnectorConfig.MODE_INCREMENTING)
                    || incrementalMode.equals(MultiTableSourceConnectorConfig.MODE_TIMESTAMP_INCREMENTING))
                    && incrementingOptional) {
                throw new ConnectException("Cannot make incremental queries using incrementing column "
                        + incrementingColumn + " on " + table + " because this column "
                        + "is nullable.");
            }
            if ((incrementalMode.equals(MultiTableSourceConnectorConfig.MODE_TIMESTAMP)
                    || incrementalMode.equals(MultiTableSourceConnectorConfig.MODE_TIMESTAMP_INCREMENTING))
                    && !atLeastOneTimestampNotOptional) {
                throw new ConnectException("Cannot make incremental queries using timestamp columns "
                        + timestampColumns + " on " + table + " because all of these "
                        + "columns "
                        + "nullable.");
            }
        } catch (SQLException e) {
            throw new ConnectException("Failed trying to validate that columns used for offsets are NOT"
                    + " NULL", e);
        }
    }

}
