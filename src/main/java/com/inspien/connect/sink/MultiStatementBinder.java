package com.inspien.connect.sink;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialect.StatementBinder;
import io.confluent.connect.jdbc.sink.BufferedRecords;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.sink.PreparedStatementBinder;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SchemaPair;
import io.confluent.connect.jdbc.util.CachedConnectionProvider;
import io.confluent.connect.jdbc.util.ColumnDefinition;
import io.confluent.connect.jdbc.util.TableDefinition;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Objects;

public class MultiPrepareStatementBinder implements StatementBinder {
    private static final Logger log = LoggerFactory.getLogger(MultiPrepareStatementBinder.class);
    private final JdbcSinkConfig.PrimaryKeyMode pkMode;
    private final PreparedStatement statement;
    private final SchemaPair schemaPair;
    private final FieldsMetadata fieldsMetadata;
    private final JdbcSinkConfig.InsertMode insertMode;
    private final DatabaseDialect dialect;
    private final TableDefinition tabDef;
    private final Connection connection;

    /** @deprecated */
    @Deprecated
    public MultiPrepareStatementBinder(DatabaseDialect dialect, PreparedStatement statement, JdbcSinkConfig.PrimaryKeyMode pkMode, SchemaPair schemaPair, FieldsMetadata fieldsMetadata, JdbcSinkConfig.InsertMode insertMode) {
        this(dialect, statement, pkMode, schemaPair, fieldsMetadata, (TableDefinition)null, insertMode,null);
    }

    public MultiPrepareStatementBinder(DatabaseDialect dialect, PreparedStatement statement, JdbcSinkConfig.PrimaryKeyMode pkMode, SchemaPair schemaPair, FieldsMetadata fieldsMetadata, TableDefinition tabDef, JdbcSinkConfig.InsertMode insertMode,Connection connection) {
        this.dialect = dialect;
        this.pkMode = pkMode;
        this.statement = statement;
        this.schemaPair = schemaPair;
        this.fieldsMetadata = fieldsMetadata;
        this.insertMode = insertMode;
        this.tabDef = tabDef;
        this.connection = connection;
    }

    public void bindRecord(SinkRecord record) throws SQLException {
        Struct valueStruct = (Struct)record.value();
        boolean isDelete = Objects.isNull(valueStruct);
        int index0 = 1;
        if (isDelete) {
            this.bindKeyFields(record, index0);
        } else {
            int index;
            switch(this.insertMode) {
                case INSERT:
                case UPSERT:
                    index = this.bindKeyFields(record, index0);
                    this.bindNonKeyFields(record, valueStruct, index);
                    break;
                case UPDATE:
                    index = this.bindNonKeyFields(record, valueStruct, index0);
                    this.bindKeyFields(record, index);
                    break;
                default:
                    throw new AssertionError();
            }
        }
        log.info("statement 바꾸고 싶어  : " + this.statement.toString());

        this.statement.addBatch("select * from hh");
    }

    protected int bindKeyFields(SinkRecord record, int index) throws SQLException {
        Iterator var3;
        String fieldName;
        Field field;
        switch(this.pkMode) {
            case NONE:
                if (!this.fieldsMetadata.keyFieldNames.isEmpty()) {
                    throw new AssertionError();
                }
                break;
            case KAFKA:
                assert this.fieldsMetadata.keyFieldNames.size() == 3;

                this.bindField(index++, Schema.STRING_SCHEMA, record.topic(), (String)JdbcSinkConfig.DEFAULT_KAFKA_PK_NAMES.get(0));
                this.bindField(index++, Schema.INT32_SCHEMA, record.kafkaPartition(), (String)JdbcSinkConfig.DEFAULT_KAFKA_PK_NAMES.get(1));
                this.bindField(index++, Schema.INT64_SCHEMA, record.kafkaOffset(), (String)JdbcSinkConfig.DEFAULT_KAFKA_PK_NAMES.get(2));
                break;
            case RECORD_KEY:
                if (this.schemaPair.keySchema.type().isPrimitive()) {
                    assert this.fieldsMetadata.keyFieldNames.size() == 1;

                    this.bindField(index++, this.schemaPair.keySchema, record.key(), (String)this.fieldsMetadata.keyFieldNames.iterator().next());
                    break;
                } else {
                    var3 = this.fieldsMetadata.keyFieldNames.iterator();

                    while(var3.hasNext()) {
                        fieldName = (String)var3.next();
                        field = this.schemaPair.keySchema.field(fieldName);
                        this.bindField(index++, field.schema(), ((Struct)record.key()).get(field), fieldName);
                    }

                    return index;
                }
            case RECORD_VALUE:
                var3 = this.fieldsMetadata.keyFieldNames.iterator();

                while(var3.hasNext()) {
                    fieldName = (String)var3.next();
                    field = this.schemaPair.valueSchema.field(fieldName);
                    this.bindField(index++, field.schema(), ((Struct)record.value()).get(field), fieldName);
                }

                return index;
            default:
                throw new ConnectException("Unknown primary key mode: " + this.pkMode);
        }

        return index;
    }

    protected int bindNonKeyFields(SinkRecord record, Struct valueStruct, int index) throws SQLException {
        Iterator var4 = this.fieldsMetadata.nonKeyFieldNames.iterator();
        // 레코드의 필드 별로 다돌고 있어
        while(var4.hasNext()) {
            String fieldName = (String)var4.next();
            log.info("fieldName : " + fieldName);
            Field field = record.valueSchema().field(fieldName);
            String maybeDBOperation = null;
            try {
                // 근데 그 필드의 밸류가 오퍼레션일 수도 있고 그냥 데이터일수도 있는데
                maybeDBOperation = valueStruct.get(field).toString();
                // 만약에 오퍼레이션이면

            } catch (Exception e) {
                log.info("아닌애들은 여기로 ");
                this.bindField(index++, field.schema(), valueStruct.get(field), fieldName);
            }
            if (maybeDBOperation.startsWith("DBOP")) {
                // 여기서 Pstmt는 일단 기본적으로 새로만들어야해
                this.statement.setObject(index, maybeDBOperation.substring(5),2002);
                log.info("asdfasdf : "+this.statement.toString());
            } else {
                log.info("여긴 그냥 일반애들 ");
                this.bindField(index++, field.schema(), valueStruct.get(field), fieldName);
            }

        }

        return index;
    }

    /** @deprecated */
    @Deprecated
    protected void bindField(int index, Schema schema, Object value) throws SQLException {
        this.dialect.bindField(this.statement, index, schema, value);
    }

    protected void bindField(int index, Schema schema, Object value, String fieldName) throws SQLException {
        ColumnDefinition colDef = this.tabDef == null ? null : this.tabDef.definitionForColumn(fieldName);
        this.dialect.bindField(this.statement, index, schema, value, colDef);
    }
}
