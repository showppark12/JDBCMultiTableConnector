/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.inspien.connect.source;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.util.ExpressionBuilder;
import io.confluent.connect.jdbc.util.TableId;

/**
 * TableQuerier executes queries against a specific table. Implementations handle different types
 * of queries: periodic bulk loading, incremental loads using auto incrementing IDs, incremental
 * loads using timestamps, etc.
 */
abstract class TableQuerier implements Comparable<TableQuerier> {

    private final Logger log = LoggerFactory.getLogger(TableQuerier.class);

    protected final DatabaseDialect dialect;
    protected final String topicName;
    protected final TableId tableId;
    protected final String suffix;

    protected final String fk;

    // Mutable state
    protected long lastUpdate;
    protected Connection db;
    protected PreparedStatement stmt;
    protected ResultSet resultSet;
    protected SchemaMapping schemaMapping;
    private String loggedQueryString;

    // 헤더테이블의 이름
    protected String dataHeader;
    // 헤더테이블을 SELECT 할 쿼리
    protected final String dataHeaderQuery;
    // 헤더테이블을 SELECT 하기전에 업데이트할 쿼리
    protected final String dataHeaderUpdateQuery;
    // 모든 과정이 끝나고 난뒤 헤더테이블을 업데이트할 쿼리
    protected final String dataHeaderCompleteQuery;

    // 업데이트를 위한 stmt 객체
    protected PreparedStatement updateStmt;
    // Detail Table들을 검색하기 위한 KEY값을 가지고있는 STRUCT
    protected Struct headerStruct;

    public TableQuerier(
            DatabaseDialect dialect,
            String tableName,
            String topicName,
            String suffix,
            String dataHeaderQuery,
            String dataHeaderUpdateQuery,
            String dataHeaderCompleteQuery,
            String fk
    ) {
        this.dialect = dialect;
        this.tableId = dialect.parseTableIdentifier(tableName);
        this.topicName = topicName;
        this.lastUpdate = 0;
        this.suffix = suffix;
        this.dataHeaderQuery = dataHeaderQuery;
        this.dataHeaderUpdateQuery = dataHeaderUpdateQuery;
        this.dataHeaderCompleteQuery = dataHeaderCompleteQuery;
        this.headerStruct = null;
        this.fk = fk;
    }

    public long getLastUpdate() {
        return lastUpdate;
    }

    public PreparedStatement getOrCreatePreparedStatement(Connection db) throws SQLException {
        if (stmt != null) {
            return stmt;
        }
        createPreparedStatement(db);
        return stmt;
    }

    protected abstract void createPreparedStatement(Connection db) throws SQLException;

    protected abstract void updateAfterExcuteQuery() throws SQLException;


    public boolean querying() {
        return resultSet != null;
    }

    public void maybeStartQuery(Connection db, String dataHeader) throws SQLException {
        if (resultSet == null) {
            this.db = db;
            this.dataHeader = dataHeader;
            stmt = getOrCreatePreparedStatement(db);
            resultSet = executeQuery();
            String schemaName = tableId != null ? tableId.tableName() : null; // backwards compatible
            schemaMapping = SchemaMapping.create(schemaName, resultSet.getMetaData(), dialect);
        }
    }

    public Schema getSchema() {
        return schemaMapping.schema();
    }

    public abstract StructsWithSchema extractStructsWithSchema(int batchMaxRows) throws SQLException;

    public static class StructsWithSchema {
        private Schema schema;
        private List<Struct> structs;

        public StructsWithSchema(Schema schema, List<Struct> structs) {
            this.schema = schema;
            this.structs = structs;
        }

        public Schema getSchema() {
            return schema;
        }

        public List<Struct> getStruct() {
            return structs;
        }
    }

    protected abstract ResultSet executeQuery() throws SQLException;

    public boolean next() throws SQLException {
        return resultSet.next();
    }

    public abstract SourceRecord extractRecord() throws SQLException;

    public void reset(long now) {
        closeResultSetQuietly();
        closeStatementQuietly();
        releaseLocksQuietly();
        // TODO: Can we cache this and quickly check that it's identical for the next query
        //     instead of constructing from scratch since it's almost always the same
        schemaMapping = null;
        lastUpdate = now;
    }

    private void releaseLocksQuietly() {
        if (db != null) {
            try {
                db.commit();
            } catch (SQLException e) {
                log.warn("Error while committing read transaction, database locks may still be held", e);
            }
        }
        db = null;
    }

    private void closeStatementQuietly() {
        if (stmt != null) {
            try {
                stmt.close();
            } catch (SQLException ignored) {
                // intentionally ignored
            }
        }
        stmt = null;
    }

    private void closeResultSetQuietly() {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException ignored) {
                // intentionally ignored
            }
        }
        resultSet = null;
    }

    protected void addSuffixIfPresent(ExpressionBuilder builder) {
        if (!this.suffix.isEmpty()) {
            builder.append(" ").append(suffix);
        }
    }

    protected void recordQuery(String query) {
        if (query != null && !query.equals(loggedQueryString)) {
            // For usability, log the statement at INFO level only when it changes
            log.info("Begin using SQL query: {}", query);
            loggedQueryString = query;
        }
    }

    @Override
    public int compareTo(TableQuerier other) {
        if (this.lastUpdate < other.lastUpdate) {
            return -1;
        } else if (this.lastUpdate > other.lastUpdate) {
            return 1;
        } else {
            return this.tableId.compareTo(other.tableId);
        }
    }
}
