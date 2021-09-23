package com.inspien.connect.source;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConstants;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class BulkTableQuerier extends TableQuerier{

    private static final Logger log = LoggerFactory.getLogger(BulkTableQuerier.class);

    public BulkTableQuerier(
            DatabaseDialect dialect,
            String tableName,
            String topicPrefix,
            String suffix,
            String dataHeader,
            String dataHeaderQuery,
            String dataHeaderUpdateQuery,
            String dataHeaderCompleteQuery
    ) {
        super(dialect, tableName, topicPrefix, suffix, dataHeader, dataHeaderQuery, dataHeaderUpdateQuery, dataHeaderCompleteQuery);
    }

    @Override
    protected void createPreparedStatement(Connection db) throws SQLException {
        // 멀티테이블 모드일 때 자동으로 fk를 포함한 쿼리를 만들기 위한 변수
        String pkTableName = null;
        String fkTableName = null;
        String pkColumnName = null;
        String fkColumnName = null;

        DatabaseMetaData databaseMetaData = db.getMetaData();

        // 포린키 데이터들을 받아와
        ResultSet foreignKeys = databaseMetaData.getImportedKeys(null, null, tableId.tableName());

        while(foreignKeys.next()){
            pkTableName = foreignKeys.getString("PKTABLE_NAME");
            // 현재 디테일 테이블의 PK테이블이 헤더테이블이 맞다면
            // 나머지 변수도 할당받기
            if (pkTableName.equals(dataHeader)) {
                fkTableName = foreignKeys.getString("FKTABLE_NAME");
                pkColumnName = foreignKeys.getString("PKCOLUMN_NAME");
                // 헤더테이블과 연결된 FK 컬럼네임
                fkColumnName = foreignKeys.getString("FKCOLUMN_NAME");
                break;
            }
        }
        log.info("pkTableName : " + pkTableName);
        log.info("fkTableName : " + fkTableName);
        log.info("pkColumnName : " + pkColumnName);
        log.info("fkColumnName : " + fkColumnName);

        ExpressionBuilder builder = dialect.expressionBuilder();

        if (tableId.tableName().equals(dataHeader)) {
            builder.append("SELECT * FROM ");
            builder.append("(" + dataHeaderQuery +") o");
        }
        else {
            builder.append("SELECT * FROM ");
            tableId.appendTo(builder, true);
            builder.append(" WHERE " + fkColumnName + "=" + headerStruct.get(pkColumnName));
        }

        addSuffixIfPresent(builder);

        String queryStr = builder.toString();

        recordQuery(queryStr);
        log.info("{} prepared SQL query: {}", this, queryStr);
        stmt = dialect.createPreparedStatement(db, queryStr);
    }

    @Override
    protected void updateAfterExcuteQuery() throws SQLException {

    }

    @Override
    public SourceRecord extractRecordInMultiMode(int batchMaxRows, Schema schema) throws SQLException {
        log.info("extractRecord 온거잖아");

        String name = tableId.tableName(); // backwards compatible
        final String topic;
        final Map<String, String> partition;

        partition = Collections.singletonMap(JdbcSourceConnectorConstants.TABLE_NAME_KEY, name);
        topic = topicPrefix + name;

        List<Struct> structs = new ArrayList<>();

        Struct finalStruct = new Struct(schema);

        log.info("finalStruct 그냥 만들면 구조 어떻게 되어있니?  : " + finalStruct);
        log.info("finalStruct 그냥 만들면 구조 어떻게 되어있니?  : " + finalStruct.schema());

        boolean hadNext = true;
        Struct currentStruct = null;

        while (hadNext = this.next()) {
            structs.add(doExtractStruct());
        }


        finalStruct.put("header", headerStruct);
        finalStruct.put("detail", structs);
        log.info("finalStruct : " + finalStruct);
        log.info("finalStruct : " + finalStruct.schema());

        return new SourceRecord(partition, null, topic, schema, finalStruct);
    }

    @Override
    protected ResultSet executeQuery() throws SQLException {
        return stmt.executeQuery();
    }

    private Struct doExtractStruct() throws SQLException {
        log.info("맨처음 next에서도 DO EXTRACT를 호출합니다");
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
        log.info("그래서 이제 record 보여줘" + record);
        return record;
    }


    @Override
    public SourceRecord extractRecord() throws SQLException {
        return null;
    }


}
