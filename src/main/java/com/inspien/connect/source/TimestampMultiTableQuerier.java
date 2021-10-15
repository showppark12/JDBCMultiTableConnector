package com.inspien.connect.source;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.source.OffsetProtocols;
import io.confluent.connect.jdbc.source.TimestampIncrementingCriteria;
import io.confluent.connect.jdbc.source.TimestampIncrementingCriteria.CriteriaValues;
import io.confluent.connect.jdbc.source.TimestampIncrementingOffset;
import io.confluent.connect.jdbc.util.ColumnDefinition;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.DateTimeUtils;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.io.IOException;
import java.util.*;

public class TimestampMultiTableQuerier extends TableQuerier implements CriteriaValues {
    private static final Logger log = LoggerFactory.getLogger(
            TimestampMultiTableQuerier.class
    );

    // timestamp increment
    protected final List<String> timestampColumnNames;
    protected TimestampIncrementingOffset offset;
    protected TimestampIncrementingCriteria criteria;
    protected final Map<String, String> partition;
    protected final String topic;
    private final List<ColumnId> timestampColumns;
    private String incrementingColumnName;
    private final long timestampDelay;
    private final TimeZone timeZone;
    protected Timestamp endTimeOffset;

    // timestamp
    private boolean exhaustedResultSet;
    private PendingRecord nextRecord;
    private Timestamp latestCommittableTimestamp;

    public TimestampMultiTableQuerier(
            DatabaseDialect dialect,
            String tableName,
            String topicName,
            List<String> timestampColumnNames,
            String incrementingColumnName,
            Map<String, Object> offsetMap,
            Long timestampDelay,
            TimeZone timeZone,
            String suffix,
            String dataHeaderQuery,
            String dataHeaderUpdateQuery,
            String dataHeaderCompleteQuery,
            String fk
    ) {
        super(dialect, tableName, topicName ,suffix, dataHeaderQuery, dataHeaderUpdateQuery, dataHeaderCompleteQuery, fk);
        this.timestampColumnNames = timestampColumnNames;
        this.offset = TimestampIncrementingOffset.fromMap(offsetMap);
        this.incrementingColumnName = incrementingColumnName;
        this.timestampColumns = new ArrayList<>();
        for (String timestampColumn : this.timestampColumnNames) {
            if (timestampColumn != null && !timestampColumn.isEmpty()) {
                timestampColumns.add(new ColumnId(tableId, timestampColumn));
            }
        }
        topic = topicName; // backward compatible
        partition = OffsetProtocols.sourcePartitionForProtocolV1(tableId);
        this.timestampDelay = timestampDelay;
        this.timeZone = timeZone;

        this.latestCommittableTimestamp = this.offset.getTimestampOffset();
        this.exhaustedResultSet = false;
        this.nextRecord = null;
    }

    public boolean next() throws SQLException {
        if (this.exhaustedResultSet && this.nextRecord == null) {
            return false;
        } else {
            if (this.nextRecord == null) {
                if (!this.resultSet.next()) {
                    this.exhaustedResultSet = true;
                    return false;
                }

                this.nextRecord = this.doExtractRecord();
            }

            if (!this.resultSet.next()) {
                this.exhaustedResultSet = true;
            }

            return true;
        }
    }

    @Override
    protected void createPreparedStatement(Connection db) throws SQLException {
        log.info("createPreparedStatement 시작");
        findDefaultAutoIncrementingColumn(db);

        // 멀티테이블 모드일 때 자동으로 fk를 포함한 쿼리를 만들기 위한 변수
        String pkTableName;
        String pkColumnName = null;
        String fkColumnName = null;

        // 현배 querier 객체의 모드가 멀티테이블 모드이고 현재 객체의 테이블이 header 테이블이 아닐때

        DatabaseMetaData databaseMetaData = db.getMetaData();

        // 포린키 데이터들을 받아와
        ResultSet foreignKeys = databaseMetaData.getImportedKeys(null, null, tableId.tableName());

        while (foreignKeys.next()) {
            pkTableName = foreignKeys.getString("PKTABLE_NAME");
            // 현재 디테일 테이블의 PK 테이블이 헤더테이블이 맞다면 나머지 변수도 할당받기
            if (pkTableName.equals(dataHeader)) {
                // 헤더테이블의 PK 컬럼네임
                pkColumnName = foreignKeys.getString("PKCOLUMN_NAME");
                // 헤더테이블과 연결된 FK 컬럼네임
                fkColumnName = foreignKeys.getString("FKCOLUMN_NAME");
                break;
            }
        }

        // IncrementingColumnName을 설정파일에서 받아왔다면 IncrementinColumn 객체를 만들어줘
        ColumnId incrementingColumn = null;
        if (incrementingColumnName != null && !incrementingColumnName.isEmpty()) {
            incrementingColumn = new ColumnId(tableId, incrementingColumnName);
        }
        ExpressionBuilder builder = dialect.expressionBuilder();

        // 헤더테이블이면 헤더테이블을 검색하기 위해서 받아온 쿼리문을 작성
        if (tableId.tableName().equals(dataHeader)) {
            builder.append("SELECT * FROM ");
            builder.append("(" + dataHeaderQuery + ") o");
        }

        // Criteria 객체를 만들어준다. 여기서 Incrementing Column 값이나 Timestamp offset등을 받아온다.
        criteria = dialect.criteriaFor(incrementingColumn, timestampColumns);

        // 멀티테이블 모드일 때

        // 헤더테이블만 timestamp mode나 incrementing mode의 영향을 받기 때문에 Where 조건을 한번 더 붙여준다.
        if (tableId.tableName().equals(dataHeader)) {
            builder.append(" WHERE ");
            Integer lastIndex = timestampColumns.size()-1;
            String timeStampColumnName = timestampColumns.get(lastIndex).name();

            builder.append("o.");
            builder.append(timeStampColumnName);

            builder.append(" > ? AND ");

            builder.append("o.");
            builder.append(timeStampColumnName);

            builder.append(" < ? ORDER BY ");

            builder.append("o.");
            builder.append(timeStampColumnName);

            builder.append(" ASC");
        }

        addSuffixIfPresent(builder);
        String queryString = builder.toString();
        recordQuery(queryString);
        log.trace("{} prepared SQL query: {}", this, queryString);
        stmt = dialect.createPreparedStatement(db, queryString);
    }

    @Override
    public void maybeStartQuery(Connection db, String dataHeader) throws SQLException {
        if (resultSet == null) {
            log.info("starting maybeStartQuery");
            this.dataHeader = dataHeader;
            this.db = db;
            stmt = getOrCreatePreparedStatement(db);
            resultSet = executeQuery();
            if (!Objects.isNull(this.dataHeaderUpdateQuery)) {
                this.updateBeforeExecuteQuery();
            }
            String schemaName = tableId != null ? tableId.tableName() : null;
            ResultSetMetaData metaData = resultSet.getMetaData();
            dialect.validateSpecificColumnTypes(metaData, timestampColumns);
            schemaMapping = SchemaMapping.create(schemaName, metaData, dialect);
        }
    }



    protected void updateBeforeExecuteQuery() throws SQLException {
        ExpressionBuilder beforebuilder = dialect.expressionBuilder();
        ExpressionBuilder afterBuilder = dialect.expressionBuilder();

        beforebuilder.append(dataHeaderUpdateQuery);
        afterBuilder.append(dataHeaderCompleteQuery);

        criteria.whereClause(beforebuilder);
        criteria.whereClause(afterBuilder);

        addSuffixIfPresent(beforebuilder);
        addSuffixIfPresent(afterBuilder);

        String beforeQueryString = beforebuilder.toString();
        String afterQueryString = afterBuilder.toString();

        updateStmt = dialect.createPreparedStatement(db, beforeQueryString);

        criteria.setQueryParameters(updateStmt, this);
        log.info("Statement to update execute: {}", stmt.toString());

        this.db.setAutoCommit(false);

        try {
            updateStmt.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
            this.db.rollback();
        } finally {
            updateStmt.close();
            updateStmt = dialect.createPreparedStatement(db, afterQueryString);
            criteria.setQueryParameters(updateStmt, this);
        }
        this.db.commit();

    }

    @Override
    protected void updateAfterExcuteQuery() throws SQLException {
        if (!Objects.isNull(this.dataHeaderUpdateQuery)) {
            log.info("Statement to update execute: {}", updateStmt.toString());
            try {
                updateStmt.executeUpdate();
            } finally {
                updateStmt.close();
                log.info("updateSTMT 어케됨 : " + updateStmt);
            }
        } else{
            log.info("업데이트 쿼리같은걸 하지 않는다.");
        }
    }

    @Override
    public StructsWithSchema extractStructsWithSchema(int batchMaxRows) throws SQLException {
        return null;
    }

    @Override
    protected ResultSet executeQuery() throws SQLException {
        log.info("stmt : " + stmt);
        log.info("this : " + this);
        criteria.setQueryParameters(stmt, this);
        this.exhaustedResultSet = false;
        log.info("Statement to execute: {}", stmt.toString());
        return stmt.executeQuery();
    }

    @Override
    public SourceRecord extractRecord() throws SQLException {
        if (nextRecord == null) {
            throw new IllegalStateException("No more records are available");
        }
        PendingRecord currentRecord = nextRecord;
        nextRecord = exhaustedResultSet ? null : doExtractRecord();

        if (nextRecord == null
                || canCommitTimestamp(currentRecord.timestamp(), nextRecord.timestamp())) {
            latestCommittableTimestamp = currentRecord.timestamp();
        }
        return currentRecord.record(latestCommittableTimestamp);
    }


    private PendingRecord doExtractRecord() {
        Struct record = fieldSetting();
        Timestamp timestamp = offset.getTimestampOffset();
        return new PendingRecord(partition, timestamp, topic, record.schema(), record);
    }

    private Struct fieldSetting() {
        Struct record = new Struct(schemaMapping.schema());
        for (SchemaMapping.FieldSetter setter : schemaMapping.fieldSetters()) {
            try {
                setter.setField(record, resultSet);
            } catch (IOException e) {
                log.warn("Error mapping fields into Connect record", e);
                throw new ConnectException(e);
            } catch (SQLException e) {
                log.warn("SQL error mapping fields into Connect record", e);
                throw new DataException(e);
            }
        }

        this.offset = criteria.extractValues(schemaMapping.schema(), record, offset);
        return record;
    }

    @Override
    public Timestamp beginTimestampValue() throws SQLException {
        log.info("beginTimestamp -> 가장 마지막에 받아온 데이터의 timestamp : " + offset.getTimestampOffset());
        return offset.getTimestampOffset();
    }

    @Override
    public Timestamp endTimestampValue() throws SQLException {
        final long currentDbTime = dialect.currentTimeOnDB(
                stmt.getConnection(),
                DateTimeUtils.getTimeZoneCalendar(timeZone)
        ).getTime();
        Timestamp timestamp = new Timestamp(currentDbTime - timestampDelay);
        log.info("endTimestampValue 현재시간 : " + timestamp);
        return timestamp;
    }

    @Override
    public Long lastIncrementedValue() throws SQLException {
        return offset.getIncrementingOffset();
    }

    private boolean canCommitTimestamp(Timestamp current, Timestamp next) {
        return current == null || next == null || current.before(next);
    }

    private void findDefaultAutoIncrementingColumn(Connection db) throws SQLException {
        // Default when unspecified uses an autoincrementing column
        if (incrementingColumnName != null && incrementingColumnName.isEmpty()) {
            // Find the first auto-incremented column ...
            for (ColumnDefinition defn : dialect.describeColumns(
                    db,
                    tableId.catalogName(),
                    tableId.schemaName(),
                    tableId.tableName(),
                    null).values()) {
                if (defn.isAutoIncrement()) {
                    incrementingColumnName = defn.id().name();
                    break;
                }
            }
        }
        // If still not found, query the table and use the result set metadata.
        // This doesn't work if the table is empty.
        if (incrementingColumnName != null && incrementingColumnName.isEmpty()) {
            log.debug("Falling back to describe '{}' table by querying {}", tableId, db);
            for (ColumnDefinition defn : dialect.describeColumnsByQuerying(db, tableId).values()) {
                if (defn.isAutoIncrement()) {
                    incrementingColumnName = defn.id().name();
                    break;
                }
            }
        }
    }


    private static class PendingRecord {
        private final Map<String, String> partition;
        private final Timestamp timestamp;
        private final String topic;
        private final Schema valueSchema;
        private final Object value;

        public PendingRecord(
                Map<String, String> partition,
                Timestamp timestamp,
                String topic,
                Schema valueSchema,
                Object value
        ) {
            this.partition = partition;
            this.timestamp = timestamp;
            this.topic = topic;
            this.valueSchema = valueSchema;
            this.value = value;
        }

        /**
         * @return the timestamp value for the row that generated this record
         */
        public Timestamp timestamp() {
            return timestamp;
        }

        /**
         * @param offsetTimestamp the timestamp to use for the record's offset; may be null
         * @return a {@link SourceRecord} whose source offset contains the provided timestamp
         */
        public SourceRecord record(Timestamp offsetTimestamp) {
            TimestampIncrementingOffset offset = new TimestampIncrementingOffset(offsetTimestamp, null);
            return new SourceRecord(
                    partition, offset.toMap(), topic, valueSchema, value
            );
        }
    }
}
