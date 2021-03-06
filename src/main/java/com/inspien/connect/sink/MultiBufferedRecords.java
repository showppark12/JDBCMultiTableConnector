package com.inspien.connect.sink;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialect.StatementBinder;
import io.confluent.connect.jdbc.sink.*;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SchemaPair;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.TableDefinition;
import io.confluent.connect.jdbc.util.TableId;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;

public class MultiBufferedRecords {
    private static final Logger log = LoggerFactory.getLogger(MultiBufferedRecords.class);
    private final TableId tableId;
    private final JdbcSinkConfig config;
    private final DatabaseDialect dbDialect;
    private final DbStructure dbStructure;
    private final Connection connection;
    private List<SinkRecord> records = new ArrayList();
    private Schema keySchema;
    private Schema valueSchema;
    private RecordValidator recordValidator;
    private FieldsMetadata fieldsMetadata;
    private Statement updatePreparedStatement;
    private Statement deletePreparedStatement;
    private StatementBinder updateStatementBinder;
    private StatementBinder deleteStatementBinder;
    private boolean deletesInBatch = false;
    private String operation;

    public MultiBufferedRecords(JdbcSinkConfig config, TableId tableId, DatabaseDialect dbDialect, DbStructure dbStructure, Connection connection, String operation) {
        this.tableId = tableId;
        this.config = config;
        this.dbDialect = dbDialect;
        this.dbStructure = dbStructure;
        this.connection = connection;
        this.recordValidator = RecordValidator.create(config);
        this.operation = operation;
    }

    public List<SinkRecord> add(SinkRecord record, List<String> keyField) throws SQLException {
        this.recordValidator.validate(record);
        List<SinkRecord> flushed = new ArrayList();
        boolean schemaChanged = false;

        // ??????????????? ????????? ????????? ?????? keySchema ??? ????????? ???????????? keySchema ??? ?????? ?????????
        if (!Objects.equals(this.keySchema, record.keySchema())) {
            // ??????????????? ????????? keySchema ??? ????????? ???????????? keySchema ??? ????????????.
            this.keySchema = record.keySchema();
            schemaChanged = true;
        }

        // ????????? ???????????? ?????? ???????????? null ??????
        if (Objects.isNull(record.valueSchema())) {
            // ?????????????????? ??? ???????????? ????????????
            if (this.config.deleteEnabled) {
                // ??????????????? ???????????? ??? ??????
                this.deletesInBatch = true;
            }
        }
        // ?????????????????? ?????????????????? ?????? ???????????? ????????? ???????????? ?????????????????? ?????????
        else if (Objects.equals(this.valueSchema, record.valueSchema())) {
            // ?????????????????? ??????????????? ??????
            if (this.config.deleteEnabled && this.deletesInBatch) {
                flushed.addAll(this.flush());
            }
        }
        // ?????????????????? ?????????????????? ?????? ???????????? ????????? ???????????? ?????????????????? ?????? ????????? -> ?????? ??? ??????
        else {
            // ??????????????? ????????? ?????????????????? ????????? ???????????? ?????????????????? ????????????.
            this.valueSchema = record.valueSchema();
            schemaChanged = true;
        }

        // ?????? ???????????? ?????????????????? ????????? ????????????
        if (schemaChanged || this.updateStatementBinder == null) {
            flushed.addAll(this.flush());
            // ???????????? ???????????? ????????? ????????????
            SchemaPair schemaPair = new SchemaPair(record.keySchema(), record.valueSchema());

            //????????? PkField??? ??? ???????????????
            this.fieldsMetadata = FieldsMetadata.extract(this.tableId.tableName(), this.config.pkMode, keyField, this.config.fieldsWhitelist, schemaPair);
            log.info("tableID ????????? ??????????????? ??????? : " + this.tableId.toString());
            this.dbStructure.createOrAmendIfNecessary(this.config, this.connection, this.tableId, this.fieldsMetadata);

            // ????????? sql ???????????? ????????? ????????? ????????? ??????
            String insertSql = this.getInsertSql();
            String deleteSql = this.getDeleteSql();

            log.debug("{} sql: {} deleteSql: {} meta: {}", new Object[]{this.config.insertMode, insertSql, deleteSql, this.fieldsMetadata});

            // ?????? ??? ?????? stmt ??????????????? ?????? ??? ????????? ???????????? ????????? ????????? ????????????
            this.close();

            // ????????? ?????? stmt ??? ???????????? ????????????
            this.updatePreparedStatement = this.createPreparedStatement(this.connection);

//          ????????? pstmt??? binding?????? ??? ?????? ????????? ??????????????? ?????????????????? ????????? ?????? stmt??? ???????????? ?????????????????? ????????? ? ??? ?????????
//          ????????? ????????? replace????????? ?????? ??? ???
            this.updateStatementBinder = this.statementBinder(this.updatePreparedStatement, this.config.pkMode, schemaPair, this.fieldsMetadata, this.dbStructure.tableDefinition(this.connection, this.tableId), this.operation, insertSql);

            // ?????? ??? ????????? ??????
//            if (this.config.deleteEnabled && Objects.nonNull(deleteSql)) {
//                this.deletePreparedStatement = this.dbDialect.createPreparedStatement(this.connection, deleteSql);
//                this.deleteStatementBinder = this.dbDialect.statementBinder(this.deletePreparedStatement, this.config.pkMode, schemaPair, this.fieldsMetadata, this.dbStructure.tableDefinition(this.connection, this.tableId), this.config.insertMode, deleteSql);
//            }
        }

        if (Objects.isNull(record.value()) && this.config.deleteEnabled) {
            this.deletesInBatch = true;
        }

        // ????????? ??????????????? ?????? ???????????? ??????????????? ??? ????????? flush ?????? ????????? addBatch ?????????
        this.records.add(record);

        if (this.records.size() >= this.config.batchSize) {
            flushed.addAll(this.flush());
        }

        return flushed;
    }


    public Statement createPreparedStatement(Connection db) throws SQLException {
        log.trace("Creating a PreparedStatement '{}'");
        Statement stmt = db.createStatement();
        this.initializePreparedStatement(stmt);
        return stmt;
    }

    protected void initializePreparedStatement(Statement stmt) throws SQLException {
        if (this.config.batchSize > 0) {
            stmt.setFetchSize(this.config.batchSize);
        }
    }

    public StatementBinder statementBinder(Statement statement, JdbcSinkConfig.PrimaryKeyMode pkMode, SchemaPair schemaPair, FieldsMetadata fieldsMetadata, TableDefinition tabDef, String insertMode, String sql) {
        return new MultiStatementBinder(this.dbDialect, statement, pkMode, schemaPair, fieldsMetadata, tabDef ,insertMode, sql);
    }

    public List<SinkRecord> flush() throws SQLException {
        if (this.records.isEmpty()) {
            log.debug("Records is empty");
            return new ArrayList();
        } else {
            log.debug("Flushing {} buffered records", this.records.size());
            Iterator var1 = this.records.iterator();

            // ????????? ?????? ??????????????? record ??? ??? ?????? ?????? ????????? ?????? ??? record ?????? sql ??? ????????????
            while(true) {
                while(var1.hasNext()) {
                    SinkRecord record = (SinkRecord)var1.next();
                    if (Objects.isNull(record.value()) && Objects.nonNull(this.deleteStatementBinder)) {
                        this.deleteStatementBinder.bindRecord(record);
                    } else {
                        // upsert, update ??? ????????? ?????????????????? ????????? ??????
                        this.updateStatementBinder.bindRecord(record);
                    }
                }

                Optional<Long> totalUpdateCount = this.executeUpdates();
                long totalDeleteCount = this.executeDeletes();
                long expectedCount = this.updateRecordCount();

                if (totalUpdateCount.filter((total) -> {
                    return total != expectedCount;
                }).isPresent() && this.operation == JdbcSinkConfig.InsertMode.INSERT.toString()) {
                    throw new ConnectException(String.format("Update count (%d) did not sum up to total number of records inserted (%d)", totalUpdateCount.get(), expectedCount));
                }

                if (!totalUpdateCount.isPresent()) {
                    log.info("records:{} , but no count of the number of rows it affected is available", this.records.size());
                }

                List<SinkRecord> flushedRecords = this.records;
                this.records = new ArrayList();
                this.deletesInBatch = false;
                return flushedRecords;
            }
        }
    }

    private Optional<Long> executeUpdates() throws SQLException {
        Optional<Long> count = Optional.empty();
        int[] var2 = this.updatePreparedStatement.executeBatch();

        int var3 = var2.length;

        for(int var4 = 0; var4 < var3; ++var4) {
            int updateCount = var2[var4];

            if (updateCount != -2) {
                count = count.isPresent() ? count.map((total) -> {
                    return total + (long)updateCount;
                }) : Optional.of((long)updateCount);
            }
        }

        return count;
    }

    private long executeDeletes() throws SQLException {
        long totalDeleteCount = 0L;
        if (Objects.nonNull(this.deletePreparedStatement)) {
            int[] var3 = this.deletePreparedStatement.executeBatch();
            int var4 = var3.length;

            for(int var5 = 0; var5 < var4; ++var5) {
                int updateCount = var3[var5];
                if (updateCount != -2) {
                    totalDeleteCount += (long)updateCount;
                }
            }
        }

        return totalDeleteCount;
    }

    private long updateRecordCount() {
        return this.records.stream().filter((record) -> {
            return Objects.nonNull(record.value()) || !this.config.deleteEnabled;
        }).count();
    }

    public void close() throws SQLException {
        log.debug("Closing BufferedRecords with updatePreparedStatement: {} deletePreparedStatement: {}", this.updatePreparedStatement, this.deletePreparedStatement);
        if (Objects.nonNull(this.updatePreparedStatement)) {
            this.updatePreparedStatement.close();
            this.updatePreparedStatement = null;
        }

        if (Objects.nonNull(this.deletePreparedStatement)) {
            this.deletePreparedStatement.close();
            this.deletePreparedStatement = null;
        }

    }

    private String getInsertSql() throws SQLException {
        switch(this.operation) {
            case "INSERT":
                return this.dbDialect.buildInsertStatement(this.tableId, this.asColumns(this.fieldsMetadata.keyFieldNames), this.asColumns(this.fieldsMetadata.nonKeyFieldNames), this.dbStructure.tableDefinition(this.connection, this.tableId));
            case "UPSERT":
                if (this.fieldsMetadata.keyFieldNames.isEmpty()) {
                    throw new ConnectException(String.format("Write to table '%s' in UPSERT mode requires key field names to be known, check the primary key configuration", this.tableId));
                } else {
                    try {
                        return this.dbDialect.buildUpsertQueryStatement(this.tableId, this.asColumns(this.fieldsMetadata.keyFieldNames), this.asColumns(this.fieldsMetadata.nonKeyFieldNames), this.dbStructure.tableDefinition(this.connection, this.tableId));
                    } catch (UnsupportedOperationException var2) {
                        throw new ConnectException(String.format("Write to table '%s' in UPSERT mode is not supported with the %s dialect.", this.tableId, this.dbDialect.name()));
                    }
                }
            case "UPDATE":
                return this.dbDialect.buildUpdateStatement(this.tableId, this.asColumns(this.fieldsMetadata.keyFieldNames), this.asColumns(this.fieldsMetadata.nonKeyFieldNames), this.dbStructure.tableDefinition(this.connection, this.tableId));
            default:
                throw new ConnectException("Invalid insert mode");
        }
    }

    private String getDeleteSql() {
        String sql = null;
        if (this.config.deleteEnabled) {
            switch(this.config.pkMode) {
                case RECORD_KEY:
                    if (this.fieldsMetadata.keyFieldNames.isEmpty()) {
                        throw new ConnectException("Require primary keys to support delete");
                    }

                    try {
                        sql = this.dbDialect.buildDeleteStatement(this.tableId, this.asColumns(this.fieldsMetadata.keyFieldNames));
                        break;
                    } catch (UnsupportedOperationException var3) {
                        throw new ConnectException(String.format("Deletes to table '%s' are not supported with the %s dialect.", this.tableId, this.dbDialect.name()));
                    }
                default:
                    throw new ConnectException("Deletes are only supported for pk.mode record_key");
            }
        }

        return sql;
    }

    private Collection<ColumnId> asColumns(Collection<String> names) {
        return (Collection)names.stream().map((name) -> {
            return new ColumnId(this.tableId, name);
        }).collect(Collectors.toList());
    }
}
