package com.inspien.connect.sink;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialects;
import io.confluent.connect.jdbc.sink.*;
import io.confluent.connect.jdbc.util.Version;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

public class MultiTableSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(MultiTableSinkTask.class);
    ErrantRecordReporter reporter;
    DatabaseDialect dialect;
    JdbcSinkConfig config;
    MultitableDbWriter writer;
    int remainingRetries;

    public MultiTableSinkTask() {
    }

    public void start(Map<String, String> props) {
        log.info("Starting JDBC Sink task");

        this.config = MultiTableSinkConfig.createJdbcSinkConfig(props);
        this.initWriter();
        this.remainingRetries = this.config.maxRetries;

        try {
            this.reporter = this.context.errantRecordReporter();
        } catch (NoClassDefFoundError | NoSuchMethodError var3) {
            this.reporter = null;
        }

    }

    void initWriter() {
        if (this.config.dialectName != null && !this.config.dialectName.trim().isEmpty()) {
            this.dialect = DatabaseDialects.create(this.config.dialectName, this.config);
        } else {
            this.dialect = DatabaseDialects.findBestFor(this.config.connectionUrl, this.config);
        }

        DbStructure dbStructure = new DbStructure(this.dialect);
        log.info("Initializing writer using SQL dialect: {}", this.dialect.getClass().getSimpleName());
        this.writer = new MultitableDbWriter(this.config, this.dialect, dbStructure);
    }

    public void put(Collection<SinkRecord> records) {
        if (!records.isEmpty()) {
            SinkRecord first = records.iterator().next();
            int recordsCount = records.size();
            log.debug("Received {} records. First record kafka coordinates:({}-{}-{}). Writing them to the database...", new Object[]{recordsCount, first.topic(), first.kafkaPartition(), first.kafkaOffset()});

            log.info("records 얼렁 까봐 : " + records);

            try {
                this.writer.write(records);
            } catch (TableAlterOrCreateException var10) {
                if (this.reporter == null) {
                    throw var10;
                }

                this.unrollAndRetry(records);
            } catch (SQLException var11) {
                log.warn("Write of {} records failed, remainingRetries={}", new Object[]{records.size(), this.remainingRetries, var11});
                int totalExceptions = 0;

                for(Iterator var6 = var11.iterator(); var6.hasNext(); ++totalExceptions) {
                    Throwable e = (Throwable)var6.next();
                }

                SQLException sqlAllMessagesException = this.getAllMessagesException(var11);
                if (this.remainingRetries > 0) {
                    this.writer.closeQuietly();
                    this.initWriter();
                    --this.remainingRetries;
                    this.context.timeout(this.config.retryBackoffMs);
                    throw new RetriableException(sqlAllMessagesException);
                }

                if (this.reporter == null) {
                    log.error("Failing task after exhausting retries; encountered {} exceptions on last write attempt. For complete details on each exception, please enable DEBUG logging.", totalExceptions);
                    int exceptionCount = 1;
                    Iterator var8 = var11.iterator();

                    while(var8.hasNext()) {
                        Throwable e = (Throwable)var8.next();
                        log.debug("Exception {}:", exceptionCount++, e);
                    }

                    throw new ConnectException(sqlAllMessagesException);
                }

                this.unrollAndRetry(records);
            }

            this.remainingRetries = this.config.maxRetries;
        }
    }

    private void unrollAndRetry(Collection<SinkRecord> records) {
        this.writer.closeQuietly();
        Iterator var2 = records.iterator();

        while(var2.hasNext()) {
            SinkRecord record = (SinkRecord)var2.next();

            try {
                this.writer.write(Collections.singletonList(record));
            } catch (TableAlterOrCreateException var6) {
                this.reporter.report(record, var6);
                this.writer.closeQuietly();
            } catch (SQLException var7) {
                SQLException sqlAllMessagesException = this.getAllMessagesException(var7);
                this.reporter.report(record, sqlAllMessagesException);
                this.writer.closeQuietly();
            }
        }

    }

    private SQLException getAllMessagesException(SQLException sqle) {
        String sqleAllMessages = "Exception chain:" + System.lineSeparator();

        Throwable e;
        for(Iterator var3 = sqle.iterator(); var3.hasNext(); sqleAllMessages = sqleAllMessages + e + System.lineSeparator()) {
            e = (Throwable)var3.next();
        }

        SQLException sqlAllMessagesException = new SQLException(sqleAllMessages);
        sqlAllMessagesException.setNextException(sqle);
        return sqlAllMessagesException;
    }

    public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
    }

    public void stop() {
        log.info("Stopping task");

        try {
            this.writer.closeQuietly();
        } finally {
            try {
                if (this.dialect != null) {
                    this.dialect.close();
                }
            } catch (Throwable var21) {
                log.warn("Error while closing the {} dialect: ", this.dialect.name(), var21);
            } finally {
                this.dialect = null;
            }

        }

    }

    public String version() {
        return Version.getVersion();
    }
}
