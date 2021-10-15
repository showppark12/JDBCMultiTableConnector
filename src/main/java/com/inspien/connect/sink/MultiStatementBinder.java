package com.inspien.connect.sink;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialect.StatementBinder;
import io.confluent.connect.jdbc.sink.JdbcSinkConfig;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SchemaPair;
import io.confluent.connect.jdbc.util.TableDefinition;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Iterator;
import java.util.Objects;

public class MultiStatementBinder implements StatementBinder {
    private static final Logger log = LoggerFactory.getLogger(MultiStatementBinder.class);
    private final JdbcSinkConfig.PrimaryKeyMode pkMode;
    private final Statement statement;
    private final SchemaPair schemaPair;
    private final FieldsMetadata fieldsMetadata;
    private final String insertMode;
    private final DatabaseDialect dialect;
    private final TableDefinition tabDef;
    private final String sql;


    public MultiStatementBinder(DatabaseDialect dialect, Statement statement, JdbcSinkConfig.PrimaryKeyMode pkMode, SchemaPair schemaPair, FieldsMetadata fieldsMetadata, TableDefinition tabDef, String insertMode, String sql) {
        this.dialect = dialect;
        this.pkMode = pkMode;
        this.statement = statement;
        this.schemaPair = schemaPair;
        this.fieldsMetadata = fieldsMetadata;
        this.insertMode = insertMode;
        this.tabDef = tabDef;
        this.sql = sql;
    }

    public void bindRecord(SinkRecord record) throws SQLException {
        Struct valueStruct = (Struct)record.value();
        boolean isDelete = Objects.isNull(valueStruct);
        MultiReturn multiReturn = null;
        int index = 1;
        if (isDelete) {
            this.bindKeyFields(record, index, this.sql);
        } else {
            switch(this.insertMode) {
                case "INSERT":
                case "UPSERT":
                    multiReturn = this.bindKeyFields(record, index, this.sql);
                    multiReturn = this.bindNonKeyFields(record, valueStruct, multiReturn.getReturnIndex(), multiReturn.getReturnSql());
                    break;
                case "UPDATE":
                    multiReturn = this.bindNonKeyFields(record, valueStruct, index, this.sql);
                    this.bindKeyFields(record, multiReturn.getReturnIndex(), this.sql);
                    break;
                default:
                    throw new AssertionError();
            }
        }
        log.info("final sql : " + multiReturn.getReturnSql());
        this.statement.addBatch(multiReturn.getReturnSql());
    }

    protected MultiReturn bindKeyFields(SinkRecord record, int index, String sql) throws SQLException {
        Iterator var3;
        String fieldName;
        Field field;
        String finalSql = sql;
        switch(this.pkMode) {
            case NONE:
                if (!this.fieldsMetadata.keyFieldNames.isEmpty()) {
                    throw new AssertionError();
                }
                break;
            case KAFKA:
                // 지원하지 않음
//                assert this.fieldsMetadata.keyFieldNames.size() == 3;
//                this.bindField(index++, Schema.STRING_SCHEMA, record.topic(), (String)JdbcSinkConfig.DEFAULT_KAFKA_PK_NAMES.get(0));
//                this.bindField(index++, Schema.INT32_SCHEMA, record.kafkaPartition(), (String)JdbcSinkConfig.DEFAULT_KAFKA_PK_NAMES.get(1));
//                this.bindField(index++, Schema.INT64_SCHEMA, record.kafkaOffset(), (String)JdbcSinkConfig.DEFAULT_KAFKA_PK_NAMES.get(2));
                break;
            case RECORD_KEY:
                // 지원하지 않음
//                if (this.schemaPair.keySchema.type().isPrimitive()) {
//                    assert this.fieldsMetadata.keyFieldNames.size() == 1;
//
//                    this.bindField(index++, this.schemaPair.keySchema, record.key(), (String)this.fieldsMetadata.keyFieldNames.iterator().next());
//                    break;
//                } else {
//                    var3 = this.fieldsMetadata.keyFieldNames.iterator();
//
//                    while(var3.hasNext()) {
//                        fieldName = (String)var3.next();
//                        field = this.schemaPair.keySchema.field(fieldName);
//                        this.bindField(index++, field.schema(), ((Struct)record.key()).get(field), fieldName);
//                    }
//
//                    return new MultiReturn(index, finalSql);
//                }
                break;
            case RECORD_VALUE:
                var3 = this.fieldsMetadata.keyFieldNames.iterator();

                while(var3.hasNext()) {
                    fieldName = (String)var3.next();
                    field = this.schemaPair.valueSchema.field(fieldName);
                    log.info("field :"+field);
                    log.info("final sql : " + finalSql);

                    finalSql = this.replaceValue((Struct) record.value(), field, finalSql);
                }

                return new MultiReturn(index, finalSql);
            default:
                throw new ConnectException("Unknown primary key mode: " + this.pkMode);
        }
        return new MultiReturn(index, finalSql);
    }

    protected MultiReturn bindNonKeyFields(SinkRecord record, Struct valueStruct, int index, String sql) throws SQLException {
        Iterator var4 = this.fieldsMetadata.nonKeyFieldNames.iterator();
        String finalSql = sql;

        // 레코드의 필드 별로 다돌고 있어
        while(var4.hasNext()) {
            String fieldName = (String)var4.next();
            Field field = record.valueSchema().field(fieldName);
            finalSql = this.replaceValue(valueStruct, field, finalSql);
        }

        return new MultiReturn(index, finalSql);
    }

    private String replaceValue(Struct valueStruct, Field field, String sql) {
        String maybeDBOperation;
        // 근데 그 필드의 밸류가 오퍼레이션일 수도 있고 그냥 데이터일수도 있는데
        Object fieldValue = valueStruct.get(field);
        maybeDBOperation = fieldValue.toString();
        String finalSql = sql;
        // 만약에 디비 오퍼레이션이면
        if (maybeDBOperation.startsWith("DBOP")) {
            // DBOP 를 제거하고 오퍼레이션만 replace 해준다.
            finalSql = finalSql.replaceFirst("\\?", maybeDBOperation.substring(5));
        } else {
            // 디비 오퍼레이션이 아닌데 String이면 따옴표를 직접 붙여준다.
            if (fieldValue instanceof String) {
                finalSql = finalSql.replaceFirst("\\?", "\'" + maybeDBOperation + "\'");
            } else {
                finalSql = finalSql.replaceFirst("\\?", maybeDBOperation);
            }
        }

        return finalSql;
    }

    class MultiReturn {
        private int returnIndex;
        private String returnSql;

        public MultiReturn(int returnIndex, String returnSql) {
            this.returnIndex = returnIndex;
            this.returnSql = returnSql;
        }

        public int getReturnIndex() {
            return returnIndex;
        }

        public String getReturnSql() {
            return returnSql;
        }
    }

    // PKMODE 다른 모드 지원하려할 때 원래 방식 이해하기위해 잠시 남겨놓는다.
//    protected void bindField(int index, Schema schema, Object value, String fieldName) throws SQLException {
//        ColumnDefinition colDef = this.tabDef == null ? null : this.tabDef.definitionForColumn(fieldName);
//        this.dialect.bindField(this.statement, index, schema, value, colDef);
//    }

}
