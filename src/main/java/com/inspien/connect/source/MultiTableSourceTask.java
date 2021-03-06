package com.inspien.connect.source;

import com.inspien.connect.source.TableQuerier.StructsWithSchema;
import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialects;
import io.confluent.connect.jdbc.source.*;
import io.confluent.connect.jdbc.util.*;

import org.apache.kafka.clients.producer.RecordMetadata;
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

import static java.util.Arrays.*;
import static org.quartz.CronScheduleBuilder.cronSchedule;
import static org.quartz.JobBuilder.newJob;
import static org.quartz.TriggerBuilder.newTrigger;

public class MultiTableSourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(JdbcSourceTask.class);

    private Time time;
    private MultiTableSourceTaskConfig config;
    private DatabaseDialect dialect;
    private CachedConnectionProvider cachedConnectionProvider;
    private Map<String, BulkTableQuerier> tableMap = new HashMap<>();
    private Map<String, List<String>> tableTree = new HashMap<>();
    private Map<String, Schema> schemaMap = new HashMap<>();
    private PriorityQueue<TableQuerier> tableQueue = new PriorityQueue();
    private final AtomicBoolean running = new AtomicBoolean(false);
    public final AtomicBoolean waiting = new AtomicBoolean(true);
    private final AtomicLong taskThreadId = new AtomicLong(0);

    List<SourceRecord> headerResults = new ArrayList<>();

    private TableQuerier headerQuerier;

    private Scheduler scheduler;

    public MultiTableSourceTask() {
        this.time = new SystemTime();
    }

    public MultiTableSourceTask(Time time) {
        this.time = time;
    }

    // offset???????????? ???????????? ????????? ?????????
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

        final String url = config.getString(MultiTableSourceTaskConfig.CONNECTION_URL_CONFIG);
        final int maxConnAttempts = config.getInt(MultiTableSourceTaskConfig.CONNECTION_ATTEMPTS_CONFIG);
        final long retryBackoff = config.getLong(MultiTableSourceTaskConfig.CONNECTION_BACKOFF_CONFIG);

        dialect = DatabaseDialects.findBestFor(url, config);
        log.info("Using JDBC dialect {}", dialect.name());

        cachedConnectionProvider = connectionProvider(maxConnAttempts, retryBackoff);

        // ???????????? ???????????? ????????? ???????????? ????????????.
        List<String> tables = new ArrayList<>(config.getList(MultiTableSourceTaskConfig.TABLES_CONFIG));
        // ????????? ???????????? ???????????????
        String headerTable = tables.get(0);
        // ????????? ?????? ?????????
        String [] arrayList = headerTable.split("\\.");
        headerTable = arrayList[1].replace("`", "");
        log.info("headerTable Name : " + headerTable);

        // ?????????????????? ??????????????? ??????????????????
        final String dataHeaderQuery = config.getString(MultiTableSourceTaskConfig.HEADER_TABLE_QUERY_CONFIG);
        final String dataHeaderUpdateQuery = config.getString(MultiTableSourceTaskConfig.HEADER_TABLE_UPDATE_QUERY_CONFIG);
        final String dataHeaderCompleteQuery = config.getString(MultiTableSourceTaskConfig.HEADER_TABLE_COMPLETE_QUERY_CONFIG);

        String tablesFK = config.getString(MultiTableSourceTaskConfig.TABLE_KEY_CONFIG);
        List<String> tablesFk = new ArrayList<>(Arrays.asList(tablesFK.split(",")));
        String headerPk = tablesFk.remove(0);

        // ???????????? ?????? ????????? ????????? Tree ????????? ??????
        // ??? ?????? ???????????? ????????? ??????
        String hierarchy = config.getString(MultiTableSourceTaskConfig.TABLE_HIERARCHY_CONFIG);
        String[] hierarchyList = hierarchy.split(",");
        for (String s : hierarchyList) {
            String[] detailHierarchy = s.split("/");
            for (int i = 1; i < detailHierarchy.length; i++) {
                if (i == 1) {
                    tableTree.put(detailHierarchy[0], new ArrayList<>(asList(detailHierarchy[i])));
                } else{
                    tableTree.get(detailHierarchy[0]).add(detailHierarchy[i]);
                }
            }
        }
        log.info("tableTree : " + tableTree.toString());


        if (tables.isEmpty()) {
            throw new ConnectException("Invalid configuration: each JdbcSourceTask must have at "
                    + "least one table assigned to it or one query specified");
        }

        String mode = config.getString(MultiTableSourceTaskConfig.MODE_CONFIG);

        Map<String, List<Map<String, String>>> partitionsByTableFqn = new HashMap<>();
        Map<Map<String, String>, Map<String, Object>> offsets = null;
        if (mode.equals(MultiTableSourceTaskConfig.MODE_INCREMENTING)
                || mode.equals(MultiTableSourceTaskConfig.MODE_TIMESTAMP)
                || mode.equals(MultiTableSourceTaskConfig.MODE_TIMESTAMP_INCREMENTING)) {
            List<Map<String, String>> partitions = new ArrayList<>(tables.size());

            // Find possible partition maps for different offset protocols
            // We need to search by all offset protocol partition keys to support compatibility
            List<Map<String, String>> tablePartitions = possibleTablePartitions(headerTable);
            partitions.addAll(tablePartitions);
            partitionsByTableFqn.put(headerTable, tablePartitions);

            offsets = context.offsetStorageReader().offsets(partitions);
            log.info("The partition offsets are {}", offsets);
        }

        // ???????????? ?????? ?????? ?????? ??????
        JobDetail jobDetail = newJob(ExecutePoll.class).build();
        JobDataMap jobDataMap = jobDetail.getJobDataMap();
        jobDataMap.put("Thread", Thread.currentThread());
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

        for (String table : tables) {
            final List<Map<String, String>> tablePartitionsToCheck;
            Map<String, Object> offset = null;

            // ????????? ?????? ?????????
            String [] arrayList2 = table.split("\\.");
            String tableName = arrayList2[1].replace("`", "");

            if (table.equals(headerTable)) {
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

            }

            String topicName = config.topicName();

            // ?????? ??????????????? headerQuerier??? mode?????? ????????? ???????????? ????????????
            if (tableName.equals(headerTable)) {
                if (mode.equals(MultiTableSourceTaskConfig.MODE_BULK)) {
                    headerQuerier = new BulkTableQuerier(
                            dialect,
                            table,
                            topicName,
                            suffix,
                            dataHeaderQuery,
                            dataHeaderUpdateQuery,
                            dataHeaderCompleteQuery,
                            headerPk,
                            headerPk
                    );
                } else if (mode.equals(MultiTableSourceTaskConfig.MODE_TIMESTAMP)) {
                    headerQuerier = new TimestampMultiTableQuerier(
                            dialect,
                            table,
                            topicName,
                            timestampColumns,
                            null,
                            offset,
                            timestampDelayInterval,
                            timeZone,
                            suffix,
                            dataHeaderQuery,
                            dataHeaderUpdateQuery,
                            dataHeaderCompleteQuery,
                            headerPk
                    );
                } else if (mode.equals(MultiTableSourceTaskConfig.MODE_INCREMENTING)) {
                    headerQuerier = new IncrementingMultiTableQuerier(
                            dialect,
                            table,
                            topicName,
                            null,
                            incrementingColumn,
                            offset,
                            timestampDelayInterval,
                            timeZone,
                            suffix,
                            dataHeaderQuery,
                            dataHeaderUpdateQuery,
                            dataHeaderCompleteQuery,
                            headerPk
                    );
                }
                continue;
            }

            // ????????? ??????????????? ????????? ?????? ??????????????? ???????????? ???????????????.
            tableMap.put(tableName, new BulkTableQuerier(
                    dialect,
                    table,
                    topicName,
                    suffix,
                    dataHeaderQuery,
                    dataHeaderUpdateQuery,
                    dataHeaderCompleteQuery,
                    headerPk,
                    tablesFk.remove(0)
            ));
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
        return asList(
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

        // ????????? 1???????????? ???????????? ?????? ?????? ??????
        try {
            Thread.sleep(Long.MAX_VALUE);
        } catch (InterruptedException e) {
            e.printStackTrace();
            log.info("sleep ????????????");
        }

        try {
            // ?????????????????? ?????? ????????? ???????????? ???????????? Results Set??? ????????????.
            headerQuerier.maybeStartQuery(cachedConnectionProvider.getConnection(), headerQuerier.tableId.tableName());
            int batchMaxRows = config.getInt(MultiTableSourceTaskConfig.BATCH_MAX_ROWS_CONFIG);
            while (headerResults.size() < batchMaxRows && headerQuerier.next()) {
                headerResults.add(headerQuerier.extractRecord());
            }

            for (SourceRecord headerResult : headerResults) {
                log.info("headerResult : " + headerResult.toString());
            }

            headerQuerier.resultSet = null;
            headerQuerier.stmt = null;

        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        // ????????? ????????? List
        final List<SourceRecord> results = new ArrayList<>();

        // ??? ????????? ?????? ???????????? ???????????? ?????? ????????????
        Map<String, Schema> schemaMap = new LinkedHashMap<>();
        // ??? ????????? ?????? ?????? ?????? row ??? ???????????? ?????? ???????????? ???????????? ????????????
        Map<Struct, Map<String, List<Struct>>> structs = new LinkedHashMap<>();

        String headerTableName = headerQuerier.tableId.tableName();

        // ?????? ??????????????? ????????? row??? ???????????????
        for (SourceRecord headerResult : headerResults) {
            Struct headerStruct = (Struct) headerResult.value();
            structs.put(headerStruct, new HashMap<>());
            // ?????? ???????????? ?????????????????? ?????????
            for (String childTableName : tableTree.get(headerTableName)) {
                // ?????????????????? row ?????? ?????????????????? dfs ??? ??????????????? ?????? leaf table ?????? ?????? row ?????? ?????? ????????????. ????????????
                StructsWithSchema childStructWithSchema = pollingHierarchyTable(headerTableName, childTableName, headerStruct);
                if (!childStructWithSchema.getStruct().isEmpty()) {
                    structs.get(headerStruct).put(childTableName, childStructWithSchema.getStruct());
                }
                if (!schemaMap.containsKey(childTableName)) {
                    schemaMap.put(childTableName, childStructWithSchema.getSchema());
                }
            }
        }
        // ?????? ????????? ????????? headerResults ??? ???????????????.
        headerResults.clear();

        // ??? ???????????? ????????? ????????? ????????????????????? ???????????? ?????? ???????????? ?????????
        SchemaBuilder schemaBuilder = SchemaBuilder.struct();
        // ?????? ?????? ????????? ???????????? ??????
        schemaBuilder.field(headerTableName, headerQuerier.getSchema());
        // ????????? ????????? ?????? ????????? ?????? ???????????? ???????????? ?????????.
        for (Map.Entry<String, Schema> value : schemaMap.entrySet()) {
            schemaBuilder.field(value.getKey(), SchemaBuilder.array(value.getValue()).optional().build());
        }
        schemaBuilder.optional();
        Schema finalSchema = schemaBuilder.build();

        // final schema ??? ?????? ??????????????? ?????????
        List<Struct> finalStructList = new ArrayList<>();

        // ?????? row ??????
        Iterator rowIter = structs.entrySet().iterator();
        while (rowIter.hasNext()) {
            Struct finalStruct = new Struct(finalSchema);
            Map.Entry<Struct, Map<String, List<Struct>>> entry = (Map.Entry)rowIter.next();
            finalStruct.put(headerTableName, entry.getKey());

            // ?????????????????? ???????????? ?????????????????? ??????.
            Iterator tableIter = entry.getValue().entrySet().iterator();
            while (tableIter.hasNext()) {
                Map.Entry<String, List<Struct>> entry1 = (Map.Entry)tableIter.next();
                finalStruct.put(entry1.getKey(), entry1.getValue());
            }
            finalStructList.add(finalStruct);
        }

        if (!finalStructList.isEmpty()) {
            Schema dataSchema = SchemaBuilder.struct()
                    .field("data", SchemaBuilder.array(finalSchema).build())
                    .build();

            Struct dataStruct = new Struct(dataSchema);
            dataStruct.put("data", finalStructList);
            log.info("final Struct : " + dataStruct.toString());

            final String topic;
            final Map<String, String> partition;

            partition = Collections.singletonMap(JdbcSourceConnectorConstants.TABLE_NAME_KEY, headerQuerier.tableId.tableName());
            topic = headerQuerier.topicName;

            SourceRecord sourceRecord = new SourceRecord(partition, null, topic, dataSchema, dataStruct);
            results.add(sourceRecord);

            // If we finished processing the results from the current query, we can reset and send
            // the querier to the tail of the queue
            // ???????????? ????????? ?????????????????? ??? ???????????????

            log.info("Returning {} records for {}", results.size(), headerQuerier);

            // ???????????? ??? ???????????? ????????? ????????? ?????? -> commitRecord??? ????????? ??????
            try {
                headerQuerier.updateAfterExcuteQuery();
            } catch (SQLException throwables) {
                log.error("error occur when update Y");
                try {
                    headerQuerier.db.rollback();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
                resetAndRequeueHead(headerQuerier);
                // This task has failed, so close any resources (may be reopened if needed) before throwing
                closeResources();
            }

            try {
                headerQuerier.db.commit();
            } catch (SQLException throwables) {
                throwables.printStackTrace();
            }

            return results;

        } else {
            log.info("no data");
            return null;
        }
    }

    @Override
    public void commitRecord(SourceRecord record, RecordMetadata metadata) throws InterruptedException {
        log.info("{} Wrote record successfully: topic {} partition {} offset {}", new Object[]{this, metadata.topic(), metadata.partition(), metadata.offset()});
        log.info("metadata : " + metadata);
        log.info("record : " + record);
        super.commitRecord(record, metadata);
    }

    public StructsWithSchema pollingHierarchyTable(String headerTableName, String tableName, Struct keyStruct) {
        // ????????? ????????????????????? ???????????? ????????????.
        // ?????? ?????? ?????? ??????????????? ?????????
        final BulkTableQuerier querier = tableMap.get(tableName);

        // ??? row ??? ?????? struct ??? querier ??? headerStruct ????????? ???????????????.
        querier.headerStruct = keyStruct;

        // ????????????????????? ????????? ???????????? ????????? ????????????
        StructsWithSchema structsWithSchema;
        try {
            log.info("Checking for next block of results from {}", querier.toString());
            // ??? row ??? ?????? ??????????????? ????????? ???????????? ????????????.
            querier.maybeStartQuery(cachedConnectionProvider.getConnection(), headerTableName);

            int batchMaxRows = config.getInt(MultiTableSourceTaskConfig.BATCH_MAX_ROWS_CONFIG);

            // ????????? ????????? struct List ??? ?????? ??? ????????? ?????????????????? ????????? ?????? ??????????????? ????????? struct List
            // ?????? ????????? schema ??? ????????? ?????? ????????? ???????????? ???????????? transit_detail
            structsWithSchema = querier.extractStructsWithSchema(batchMaxRows);

            // resultSet ?????????
            // ????????? ???????????? ?????? ??????????????? ????????? ????????? ?????? ???????????? ????????? ???????????? ?????? ????????????
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

        // ????????? ????????? ????????? ????????? ?????? ????????????
        if (Objects.isNull(tableTree.get(tableName))) {
            return structsWithSchema;
        }

        Map<String, Schema> schemaMap = new LinkedHashMap<>();
        Map<Struct, Map<String, List<Struct>>> structs = new LinkedHashMap<>();
        // ??? ??????????????? ????????? struct ?????? keyStruct
        // ???????????? ??? struct ??? ???????????? ????????????
        // structWithSchema ??? ?????? ??????????????? keyStruct ?????? ????????? row ???
        for (Struct struct : structsWithSchema.getStruct()) {
            // ??? ?????? ?????? ???????????? ?????????.
            structs.put(struct, new LinkedHashMap<>());
            for (String childTableName : tableTree.get(tableName)) {
                // ?????????????????? ????????? ????????? ????????? ???????????? ???????????? ???????????????
                StructsWithSchema childStructWithSchema = pollingHierarchyTable(tableName, childTableName, struct);
                if (!childStructWithSchema.getStruct().isEmpty()) {
                    structs.get(struct).put(childTableName, childStructWithSchema.getStruct());
                }

                // ?????????????????? ???????????? map ??? ????????? ??????????????? structs ?????? ????????? ???????????????.
                if (!schemaMap.containsKey(childTableName)) {
                    schemaMap.put(childTableName, childStructWithSchema.getSchema());
                }
            }
        }

        SchemaBuilder schemaBuilder = SchemaBuilder.struct();
        schemaBuilder.field(tableName, structsWithSchema.getSchema());
        for (Map.Entry<String, Schema> value : schemaMap.entrySet()) {
            schemaBuilder.field(value.getKey(), SchemaBuilder.array(value.getValue()).optional().build());
        }
        schemaBuilder.optional();
        Schema middleSchema = schemaBuilder.build();

        List<Struct> middleStructList = new ArrayList<>();
        Iterator iterator1 = structs.entrySet().iterator();

        while (iterator1.hasNext()) {
            Struct middleStruct = new Struct(middleSchema);
            Map.Entry<Struct, Map<String, List<Struct>>> entry = (Map.Entry)iterator1.next();
            middleStruct.put(tableName, entry.getKey());

            Iterator iterator2 = entry.getValue().entrySet().iterator();
            while (iterator2.hasNext()) {
                Map.Entry<String, List<Struct>> entry1 = (Map.Entry)iterator2.next();
                middleStruct.put(entry1.getKey(), entry1.getValue());
            }
            middleStructList.add(middleStruct);
        }

        return new StructsWithSchema(middleSchema,middleStructList);
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
        final TableQuerier querier = tableQueue.peek();
        if (querier != null) {
            resetAndRequeueHead(querier);
        }
        closeResources();
    }

    private void resetAndRequeueHead(TableQuerier expectedHead) {
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
            if ((incrementalMode.equals(MultiTableSourceTaskConfig.MODE_INCREMENTING)
                    || incrementalMode.equals(MultiTableSourceTaskConfig.MODE_TIMESTAMP_INCREMENTING))
                    && incrementingOptional) {
                throw new ConnectException("Cannot make incremental queries using incrementing column "
                        + incrementingColumn + " on " + table + " because this column "
                        + "is nullable.");
            }
            if ((incrementalMode.equals(MultiTableSourceTaskConfig.MODE_TIMESTAMP)
                    || incrementalMode.equals(MultiTableSourceTaskConfig.MODE_TIMESTAMP_INCREMENTING))
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
