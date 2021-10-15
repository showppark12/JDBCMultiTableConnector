package com.inspien.connect.sink;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.util.CachedConnectionProvider;
import io.confluent.connect.jdbc.util.TableId;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.connect.jdbc.sink.*;

public class MultitableDbWriter {
    private static final Logger log = LoggerFactory.getLogger(MultitableDbWriter.class);
    private final JdbcSinkConfig config;
    private final DatabaseDialect dbDialect;
    private final DbStructure dbStructure;
    final CachedConnectionProvider cachedConnectionProvider;

    MultitableDbWriter(JdbcSinkConfig config, DatabaseDialect dbDialect, DbStructure dbStructure) {
        this.config = config;
        this.dbDialect = dbDialect;
        this.dbStructure = dbStructure;
        this.cachedConnectionProvider = this.connectionProvider(config.connectionAttempts, config.connectionBackoffMs);
    }

    protected CachedConnectionProvider connectionProvider(int maxConnAttempts, long retryBackoff) {
        return new CachedConnectionProvider(this.dbDialect, maxConnAttempts, retryBackoff) {
            protected void onConnect(Connection connection) throws SQLException {
                MultitableDbWriter.log.info("JdbcDbWriter Connected");
                connection.setAutoCommit(false);
            }
        };
    }

    void write(Collection<SinkRecord> records) throws SQLException, TableAlterOrCreateException {
        Connection connection = this.cachedConnectionProvider.getConnection();
        try {
            // 삽입 순서를 보장하기 위해서 HashMap 을 LinkedHashMap 으로 바꿔준다.
            Map<TableId, MultiBufferedRecords> bufferByTable = new LinkedHashMap<>();

            Iterator var4;
            SinkRecord record;
            TableId tableId;
            MultiBufferedRecords buffer;

            for(var4 = records.iterator(); var4.hasNext(); ) {
                record = (SinkRecord) var4.next();

                Integer kafkaPartition = record.kafkaPartition();
                Object key = record.key();
                Schema keySchema = record.keySchema();
                Long kafkaOffset = record.kafkaOffset();
                Struct data = (Struct) record.value();

                List<String> recordSequence = data.getArray("seq_order");
                for (String seq : recordSequence) {
                    Struct sequentialStruct = data.getStruct(seq);
                    String tableName = sequentialStruct.getString("table");
                    String operation = sequentialStruct.getString("operation");
                    List<Struct> recordList = sequentialStruct.getArray("data");
                    List<String> keyList = sequentialStruct.getArray("keys");

                    tableId = this.destinationTable(tableName);
                    log.info("dest : " + tableName);
                    log.info("destination : " + tableId);
                    log.info("RecordList : " + recordList.toString());

                    for (Struct detailRecord : recordList) {
                        buffer = bufferByTable.get(tableId);
                        if (buffer == null) {
                            buffer = new MultiBufferedRecords(this.config, tableId, this.dbDialect, this.dbStructure, connection, operation);
                            bufferByTable.put(tableId, buffer);
                        }
                        buffer.add(new SinkRecord(tableName, kafkaPartition, keySchema, key, detailRecord.schema(), detailRecord, kafkaOffset), keyList);
                    }
                }
            }

            var4 = bufferByTable.entrySet().iterator();

            // LinkedHashMap 이기 때문에 순서를 보장 받는다 그냥 그대로 로직을 수행하면 됨
            while(var4.hasNext()) {
                Entry<TableId, MultiBufferedRecords> entry = (Entry)var4.next();
                tableId = entry.getKey();
                buffer = entry.getValue();

                log.debug("Flushing records in JDBC Writer for table ID: {}", tableId);
                buffer.flush();
                buffer.close();
            }

            connection.commit();
        } catch (TableAlterOrCreateException var14) {
            TableAlterOrCreateException e = var14;
            try {
                connection.rollback();
                throw var14;
            } catch (SQLException var12) {
                e.addSuppressed(var12);
            } finally {
                ;
            }
            throw var14;
        } catch (SQLException var14){
            SQLException e = var14;
            try {
                connection.rollback();
                throw var14;
            } catch (SQLException var12) {
                e.addSuppressed(var12);
            } finally {
                ;
            }
            throw var14;
        }
    }

    void closeQuietly() {
        this.cachedConnectionProvider.close();
    }

    TableId destinationTable(String topic) {
        if (topic.isEmpty()) {
            throw new ConnectException(String.format("Destination table name for topic '%s' is empty using the format string '%s'", topic, this.config.tableNameFormat));
        } else {
            return this.dbDialect.parseTableIdentifier(topic);
        }
    }
}
