package com.inspien.connect.source;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.source.JdbcSourceConnectorConstants;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import org.apache.kafka.connect.data.Field;
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
import java.util.*;

public class BulkTableQuerier extends TableQuerier{

    private static final Logger log = LoggerFactory.getLogger(BulkTableQuerier.class);

    protected final String pk;

    public BulkTableQuerier(
            DatabaseDialect dialect,
            String tableName,
            String topicName,
            String suffix,
            String dataHeaderQuery,
            String dataHeaderUpdateQuery,
            String dataHeaderCompleteQuery,
            String pk,
            String fk
    ) {
        super(dialect, tableName, topicName, suffix, dataHeaderQuery, dataHeaderUpdateQuery, dataHeaderCompleteQuery, fk);
        this.pk=pk;
    }

    @Override
    protected void createPreparedStatement(Connection db) throws SQLException {
        ExpressionBuilder builder = dialect.expressionBuilder();

        if (tableId.tableName().equals(dataHeader)) {
            builder.append(dataHeaderQuery);
        } else {
            builder.append("SELECT * FROM ");
            tableId.appendTo(builder, true);
            builder.append(" WHERE " + fk + "=" + headerStruct.get(pk));
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
    public StructsWithSchema extractStructsWithSchema(int batchMaxRows) throws SQLException {
        List<Struct> structs = new ArrayList<>();

        while (this.next()) {
            structs.add(doExtractStruct());
        }

        return new StructsWithSchema(schemaMapping.schema(), structs);
    }

    @Override
    protected ResultSet executeQuery() throws SQLException {
        return stmt.executeQuery();
    }

    private Struct doExtractStruct() throws SQLException {
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
        return record;
    }

    @Override
    public SourceRecord extractRecord() throws SQLException {
        Struct record = new Struct(this.schemaMapping.schema());

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

        String topic;
        Map partition;
        String name = this.tableId.tableName();
        partition = Collections.singletonMap("table", name);
        topic = this.topicName + name;

        return new SourceRecord(partition, (Map)null, topic, record.schema(), record);
    }



}
