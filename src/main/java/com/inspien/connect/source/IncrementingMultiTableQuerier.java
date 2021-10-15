package com.inspien.connect.source;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.source.OffsetProtocols;
import io.confluent.connect.jdbc.source.TimestampIncrementingCriteria;
import io.confluent.connect.jdbc.source.TimestampIncrementingOffset;
import io.confluent.connect.jdbc.source.TimestampIncrementingTableQuerier;
import io.confluent.connect.jdbc.util.ColumnDefinition;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.DateTimeUtils;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.*;
import java.util.*;

public class IncrementingMultiTableQuerier extends TableQuerier implements TimestampIncrementingCriteria.CriteriaValues {
    private static final Logger log = LoggerFactory.getLogger(IncrementingMultiTableQuerier.class);
    protected final List<String> timestampColumnNames;
    protected TimestampIncrementingOffset offset;
    protected TimestampIncrementingCriteria criteria;
    protected final Map<String, String> partition;
    protected final String topic;
    private final List<ColumnId> timestampColumns;
    private String incrementingColumnName;
    private final long timestampDelay;
    private final TimeZone timeZone;
    private static String DATETIME = "datetime";

    public IncrementingMultiTableQuerier(
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
        super(dialect, tableName, topicName, suffix,dataHeaderQuery,dataHeaderUpdateQuery,dataHeaderCompleteQuery,fk);
        this.incrementingColumnName = incrementingColumnName;
        this.timestampColumnNames = timestampColumnNames != null ? timestampColumnNames : Collections.emptyList();
        this.timestampDelay = timestampDelay;
        this.offset = TimestampIncrementingOffset.fromMap(offsetMap);
        this.timestampColumns = new ArrayList();
        Iterator var11 = this.timestampColumnNames.iterator();

        while(var11.hasNext()) {
            String timestampColumn = (String)var11.next();
            if (timestampColumn != null && !timestampColumn.isEmpty()) {
                this.timestampColumns.add(new ColumnId(this.tableId, timestampColumn));
            }
        }

        this.topic = topicName;
        this.partition = OffsetProtocols.sourcePartitionForProtocolV1(this.tableId);
        this.timeZone = timeZone;
    }

    protected void createPreparedStatement(Connection db) throws SQLException {
        this.findDefaultAutoIncrementingColumn(db);
        ColumnId incrementingColumn = null;
        if (this.incrementingColumnName != null && !this.incrementingColumnName.isEmpty()) {
            incrementingColumn = new ColumnId(this.tableId, this.incrementingColumnName);
        }

        ExpressionBuilder builder = this.dialect.expressionBuilder();

        builder.append("SELECT * FROM ");
        builder.append("(" + dataHeaderQuery + ") o");

        this.criteria = this.dialect.criteriaFor(incrementingColumn, this.timestampColumns);
        this.criteria.whereClause(builder);
        this.addSuffixIfPresent(builder);
        String queryString = builder.toString();
        this.recordQuery(queryString);
        log.info("incrementing mode : " + queryString);
        log.debug("{} prepared SQL query: {}", this, queryString);
        this.stmt = this.dialect.createPreparedStatement(db, queryString);
    }

    @Override
    protected void updateAfterExcuteQuery() throws SQLException {

    }

    @Override
    public StructsWithSchema extractStructsWithSchema(int batchMaxRows) throws SQLException {
        return null;
    }

    public void maybeStartQuery(Connection db) throws SQLException, ConnectException {
        if (this.resultSet == null) {
            this.db = db;
            this.stmt = this.getOrCreatePreparedStatement(db);
            this.resultSet = this.executeQuery();
            String schemaName = this.tableId != null ? this.tableId.tableName() : null;
            ResultSetMetaData metadata = this.resultSet.getMetaData();
            this.dialect.validateSpecificColumnTypes(metadata, this.timestampColumns);
            this.schemaMapping = SchemaMapping.create(schemaName, metadata, this.dialect);
        }

    }

    private void findDefaultAutoIncrementingColumn(Connection db) throws SQLException {
        Iterator var2;
        ColumnDefinition defn;
        if (this.incrementingColumnName != null && this.incrementingColumnName.isEmpty()) {
            var2 = this.dialect.describeColumns(db, this.tableId.catalogName(), this.tableId.schemaName(), this.tableId.tableName(), (String)null).values().iterator();

            while(var2.hasNext()) {
                defn = (ColumnDefinition)var2.next();
                if (defn.isAutoIncrement()) {
                    this.incrementingColumnName = defn.id().name();
                    break;
                }
            }
        }

        if (this.incrementingColumnName != null && this.incrementingColumnName.isEmpty()) {
            log.debug("Falling back to describe '{}' table by querying {}", this.tableId, db);
            var2 = this.dialect.describeColumnsByQuerying(db, this.tableId).values().iterator();

            while(var2.hasNext()) {
                defn = (ColumnDefinition)var2.next();
                if (defn.isAutoIncrement()) {
                    this.incrementingColumnName = defn.id().name();
                    break;
                }
            }
        }

    }

    protected ResultSet executeQuery() throws SQLException {
        this.criteria.setQueryParameters(this.stmt, this);
        log.trace("Statement to execute: {}", this.stmt.toString());
        return this.stmt.executeQuery();
    }

    public SourceRecord extractRecord() throws SQLException {
        Struct record = new Struct(this.schemaMapping.schema());
        Iterator var2 = this.schemaMapping.fieldSetters().iterator();

        while(var2.hasNext()) {
            SchemaMapping.FieldSetter setter = (SchemaMapping.FieldSetter)var2.next();

            try {
                setter.setField(record, this.resultSet);
            } catch (IOException var5) {
                log.warn("Error mapping fields into Connect record", var5);
                throw new ConnectException(var5);
            } catch (SQLException var6) {
                log.warn("SQL error mapping fields into Connect record", var6);
                throw new DataException(var6);
            }
        }

        this.offset = this.criteria.extractValues(this.schemaMapping.schema(), record, this.offset);
        return new SourceRecord(this.partition, this.offset.toMap(), this.topic, record.schema(), record);
    }

    public Timestamp beginTimestampValue() {
        return this.offset.getTimestampOffset();
    }

    public Timestamp endTimestampValue() throws SQLException {
        long currentDbTime = this.dialect.currentTimeOnDB(this.stmt.getConnection(), DateTimeUtils.getTimeZoneCalendar(this.timeZone)).getTime();
        return new Timestamp(currentDbTime - this.timestampDelay);
    }

    public Long lastIncrementedValue() {
        return this.offset.getIncrementingOffset();
    }

    public String toString() {
        return "TimestampIncrementingTableQuerier";
    }
}
