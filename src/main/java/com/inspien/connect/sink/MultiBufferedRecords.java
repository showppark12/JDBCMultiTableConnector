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

        // 버퍼레코드 객체가 가지고 있는 keySchema 와 들어온 데이터의 keySchema 가 같지 않으면
        if (!Objects.equals(this.keySchema, record.keySchema())) {
            // 버퍼레코드 객체의 keySchema 를 들어온 데이터의 keySchema 로 바꿔준다.
            this.keySchema = record.keySchema();
            schemaChanged = true;
        }

        // 들어온 데이터의 밸류 스키마가 null 이고
        if (Objects.isNull(record.valueSchema())) {
            // 설정파일에서 뭐 삭제모드 어쩌구면
            if (this.config.deleteEnabled) {
                // 삭제모드로 바꿔주는 거 같고
                this.deletesInBatch = true;
            }
        }
        // 버퍼레코드에 저장되어있는 밸류 스키마와 들어온 데이터의 밸류스키마가 같으면
        else if (Objects.equals(this.valueSchema, record.valueSchema())) {
            // 설정파일에서 삭제모드고 하면
            if (this.config.deleteEnabled && this.deletesInBatch) {
                flushed.addAll(this.flush());
            }
        }
        // 버퍼레코드에 저장되어있는 밸류 스키마와 들어온 데이터의 밸류스키마가 같지 않으면 -> 이건 맨 처음
        else {
            // 버퍼레코드 객체의 밸류스키마에 들어온 데이터의 밸류스키마를 할당한다.
            this.valueSchema = record.valueSchema();
            schemaChanged = true;
        }

        // 최초 데이터가 들어왔을때만 이거를 해주는데
        if (schemaChanged || this.updateStatementBinder == null) {
            flushed.addAll(this.flush());
            // 필요한거 받아야할 애들은 받아주고
            SchemaPair schemaPair = new SchemaPair(record.keySchema(), record.valueSchema());

            //여기서 PkField를 다 받아오는데
            this.fieldsMetadata = FieldsMetadata.extract(this.tableId.tableName(), this.config.pkMode, keyField, this.config.fieldsWhitelist, schemaPair);
            log.info("tableID 똑바로 받아왔는데 이래? : " + this.tableId.toString());
            this.dbStructure.createOrAmendIfNecessary(this.config, this.connection, this.tableId, this.fieldsMetadata);

            // 여기서 sql 껍데기를 만들고 클래스 변수로 저장
            String insertSql = this.getInsertSql();
            String deleteSql = this.getDeleteSql();

            log.debug("{} sql: {} deleteSql: {} meta: {}", new Object[]{this.config.insertMode, insertSql, deleteSql, this.fieldsMetadata});

            // 이건 뭐 뒤에 stmt 만들기전에 미리 뭐 초기화 이런느낌 같던데 나중에 알아보자
            this.close();

            // 여기서 그냥 stmt 만 만들어줘 껍데기만
            this.updatePreparedStatement = this.createPreparedStatement(this.connection);

//          여기는 pstmt에 binding하는 곳 근데 우리는 바인더에서 바인드하는건 아니고 그냥 stmt를 만들껀데 이미만들어진 쿼리에 ? 를 우리의
//          우리의 밸류로 replace해주는 일을 할 것
            this.updateStatementBinder = this.statementBinder(this.updatePreparedStatement, this.config.pkMode, schemaPair, this.fieldsMetadata, this.dbStructure.tableDefinition(this.connection, this.tableId), this.operation, insertSql);

            // 요건 좀 나중에 보자
//            if (this.config.deleteEnabled && Objects.nonNull(deleteSql)) {
//                this.deletePreparedStatement = this.dbDialect.createPreparedStatement(this.connection, deleteSql);
//                this.deleteStatementBinder = this.dbDialect.statementBinder(this.deletePreparedStatement, this.config.pkMode, schemaPair, this.fieldsMetadata, this.dbStructure.tableDefinition(this.connection, this.tableId), this.config.insertMode, deleteSql);
//            }
        }

        if (Objects.isNull(record.value()) && this.config.deleteEnabled) {
            this.deletesInBatch = true;
        }

        // 여기선 버퍼레코드 마다 레코드만 더해놓으면 돼 나중에 flush 에서 돌면서 addBatch 할꺼야
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

            // 여기서 이제 저장해놓은 record 들 다 도는 건데 여기서 이제 각 record 별로 sql 을 만들꺼야
            while(true) {
                while(var1.hasNext()) {
                    SinkRecord record = (SinkRecord)var1.next();
                    if (Objects.isNull(record.value()) && Objects.nonNull(this.deleteStatementBinder)) {
                        this.deleteStatementBinder.bindRecord(record);
                    } else {
                        // upsert, update 면 여기서 레코드바인드 해주는 거지
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
