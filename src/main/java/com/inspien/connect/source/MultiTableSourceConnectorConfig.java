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

import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.atomic.AtomicReference;

import io.confluent.connect.jdbc.util.DatabaseDialectRecommender;
import io.confluent.connect.jdbc.util.EnumRecommender;
import io.confluent.connect.jdbc.util.QuoteMethod;
import io.confluent.connect.jdbc.util.TimeZoneValidator;

import java.util.regex.Pattern;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Recommender;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Validator;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiTableSourceConnectorConfig extends AbstractConfig {

    private static final Logger LOG = LoggerFactory.getLogger(MultiTableSourceConnectorConfig.class);
    private static Pattern INVALID_CHARS = Pattern.compile("[^a-zA-Z0-9._-]");

    public static final String CONNECTION_PREFIX = "connection.";

    public static final String CONNECTION_URL_CONFIG = CONNECTION_PREFIX + "url";
    private static final String CONNECTION_URL_DOC =
            "JDBC connection URL.\n"
                    + "For example: ``jdbc:oracle:thin:@localhost:1521:orclpdb1``, "
                    + "``jdbc:mysql://localhost/db_name``, "
                    + "``jdbc:sqlserver://localhost;instance=SQLEXPRESS;"
                    + "databaseName=db_name``";
    private static final String CONNECTION_URL_DISPLAY = "JDBC URL";

    public static final String CONNECTION_USER_CONFIG = CONNECTION_PREFIX + "user";
    private static final String CONNECTION_USER_DOC = "JDBC connection user.";
    private static final String CONNECTION_USER_DISPLAY = "JDBC User";

    public static final String CONNECTION_PASSWORD_CONFIG = CONNECTION_PREFIX + "password";
    private static final String CONNECTION_PASSWORD_DOC = "JDBC connection password.";
    private static final String CONNECTION_PASSWORD_DISPLAY = "JDBC Password";

    public static final String CONNECTION_ATTEMPTS_CONFIG = CONNECTION_PREFIX + "attempts";
    public static final String CONNECTION_ATTEMPTS_DOC
            = "Maximum number of attempts to retrieve a valid JDBC connection. "
            + "Must be a positive integer.";
    public static final String CONNECTION_ATTEMPTS_DISPLAY = "JDBC connection attempts";
    public static final int CONNECTION_ATTEMPTS_DEFAULT = 3;

    public static final String CONNECTION_BACKOFF_CONFIG = CONNECTION_PREFIX + "backoff.ms";
    public static final String CONNECTION_BACKOFF_DOC
            = "Backoff time in milliseconds between connection attempts.";
    public static final String CONNECTION_BACKOFF_DISPLAY
            = "JDBC connection backoff in milliseconds";
    public static final long CONNECTION_BACKOFF_DEFAULT = 10000L;

    public static final String POLL_INTERVAL_MS_CONFIG = "poll.interval.ms";
    private static final String POLL_INTERVAL_MS_DOC = "Frequency in ms to poll for new data in "
            + "each table.";
    public static final int POLL_INTERVAL_MS_DEFAULT = 5000;
    private static final String POLL_INTERVAL_MS_DISPLAY = "Poll Interval (ms)";

    public static final String BATCH_MAX_ROWS_CONFIG = "batch.max.rows";
    private static final String BATCH_MAX_ROWS_DOC =
            "Maximum number of rows to include in a single batch when polling for new data. This "
                    + "setting can be used to limit the amount of data buffered internally in the connector.";
    public static final int BATCH_MAX_ROWS_DEFAULT = 100;
    private static final String BATCH_MAX_ROWS_DISPLAY = "Max Rows Per Batch";

    public static final String NUMERIC_PRECISION_MAPPING_CONFIG = "numeric.precision.mapping";
    private static final String NUMERIC_PRECISION_MAPPING_DOC =
            "Whether or not to attempt mapping NUMERIC values by precision to integral types. This "
                    + "option is now deprecated. A future version may remove it completely. Please use "
                    + "``numeric.mapping`` instead.";

    public static final boolean NUMERIC_PRECISION_MAPPING_DEFAULT = false;
    public static final String NUMERIC_MAPPING_CONFIG = "numeric.mapping";
    private static final String NUMERIC_PRECISION_MAPPING_DISPLAY = "Map Numeric Values By "
            + "Precision (deprecated)";

    private static final String NUMERIC_MAPPING_DOC =
            "Map NUMERIC values by precision and optionally scale to integral or decimal types.\n"
                    + "  * Use ``none`` if all NUMERIC columns are to be represented by Connect's DECIMAL "
                    + "logical type.\n"
                    + "  * Use ``best_fit`` if NUMERIC columns should be cast to Connect's INT8, INT16, "
                    + "INT32, INT64, or FLOAT64 based upon the column's precision and scale. This option may "
                    + "still represent the NUMERIC value as Connect DECIMAL if it cannot be cast to a native "
                    + "type without losing precision. For example, a NUMERIC(20) type with precision 20 would "
                    + "not be able to fit in a native INT64 without overflowing and thus would be retained as "
                    + "DECIMAL.\n"
                    + "  * Use ``best_fit_eager_double`` if in addition to the properties of ``best_fit`` "
                    + "described above, it is desirable to always cast NUMERIC columns with scale to Connect "
                    + "FLOAT64 type, despite potential of loss in accuracy.\n"
                    + "  * Use ``precision_only`` to map NUMERIC columns based only on the column's precision "
                    + "assuming that column's scale is 0.\n"
                    + "  * The ``none`` option is the default, but may lead to serialization issues with Avro "
                    + "since Connect's DECIMAL type is mapped to its binary representation, and ``best_fit`` "
                    + "will often be preferred since it maps to the most appropriate primitive type.";

    public static final String NUMERIC_MAPPING_DEFAULT = null;
    private static final String NUMERIC_MAPPING_DISPLAY = "Map Numeric Values, Integral "
            + "or Decimal, By Precision and Scale";

    private static final EnumRecommender NUMERIC_MAPPING_RECOMMENDER =
            EnumRecommender.in(NumericMapping.values());

    public static final String MODE_CONFIG = "mode";
    private static final String MODE_DOC =
            "The mode for updating a table each time it is polled. Options include:\n"
                    + "  * bulk: perform a bulk load of the entire table each time it is polled\n"
                    + "  * incrementing: use a strictly incrementing column on each table to "
                    + "detect only new rows. Note that this will not detect modifications or "
                    + "deletions of existing rows.\n"
                    + "  * timestamp: use a timestamp (or timestamp-like) column to detect new and modified "
                    + "rows. This assumes the column is updated with each write, and that values are "
                    + "monotonically incrementing, but not necessarily unique.\n"
                    + "  * timestamp+incrementing: use two columns, a timestamp column that detects new and "
                    + "modified rows and a strictly incrementing column which provides a globally unique ID for "
                    + "updates so each row can be assigned a unique stream offset.";
    private static final String MODE_DISPLAY = "Table Loading Mode";

    public static final String MODE_UNSPECIFIED = "";
    public static final String MODE_BULK = "bulk";
    public static final String MODE_TIMESTAMP = "timestamp";
    public static final String MODE_INCREMENTING = "incrementing";
    public static final String MODE_TIMESTAMP_INCREMENTING = "timestamp+incrementing";

    public static final String INCREMENTING_COLUMN_NAME_CONFIG = "incrementing.column.name";
    private static final String INCREMENTING_COLUMN_NAME_DOC =
            "The name of the strictly incrementing column to use to detect new rows. Any empty value "
                    + "indicates the column should be autodetected by looking for an auto-incrementing column. "
                    + "This column may not be nullable.";
    public static final String INCREMENTING_COLUMN_NAME_DEFAULT = "";
    private static final String INCREMENTING_COLUMN_NAME_DISPLAY = "Incrementing Column Name";

    public static final String TIMESTAMP_COLUMN_NAME_CONFIG = "timestamp.column.name";
    private static final String TIMESTAMP_COLUMN_NAME_DOC =
            "Comma separated list of one or more timestamp columns to detect new or modified rows using "
                    + "the COALESCE SQL function. Rows whose first non-null timestamp value is greater than the "
                    + "largest previous timestamp value seen will be discovered with each poll. At least one "
                    + "column should not be nullable.";
    public static final String TIMESTAMP_COLUMN_NAME_DEFAULT = "";
    private static final String TIMESTAMP_COLUMN_NAME_DISPLAY = "Timestamp Column Name";

    public static final String TIMESTAMP_INITIAL_CONFIG = "timestamp.initial";
    public static final Long TIMESTAMP_INITIAL_DEFAULT = null;
    public static final Long TIMESTAMP_INITIAL_CURRENT = Long.valueOf(-1);
    public static final String TIMESTAMP_INITIAL_DOC =
            "The epoch timestamp used for initial queries that use timestamp criteria. "
                    + "Use -1 to use the current time. If not specified, all data will be retrieved.";
    public static final String TIMESTAMP_INITIAL_DISPLAY = "Unix time value of initial timestamp";

    public static final String TABLE_POLL_INTERVAL_MS_CONFIG = "table.poll.interval.ms";
    private static final String TABLE_POLL_INTERVAL_MS_DOC =
            "Frequency in ms to poll for new or removed tables, which may result in updated task "
                    + "configurations to start polling for data in added tables or stop polling for data in "
                    + "removed tables.";
    public static final long TABLE_POLL_INTERVAL_MS_DEFAULT = 60 * 1000;
    private static final String TABLE_POLL_INTERVAL_MS_DISPLAY
            = "Metadata Change Monitoring Interval (ms)";

    public static final String HEADER_TABLE_CONFIG = "data.header";
    private static final String HEADER_TABLE_CONFIG_DOC =
            "header of dataset. ";
    public static final String HEADER_TABLE_CONFIG_DEFAULT = null;
    private static final String HEADER_TABLE_CONFIG_DISPLAY = "Data Header";

    public static final String HEADER_TABLE_QUERY_CONFIG = "data.header.query";
    private static final String HEADER_TABLE_QUERY_CONFIG_DOC =
            "query of header of dataset. ";
    public static final String HEADER_TABLE_QUERY_CONFIG_DEFAULT = null;
    private static final String HEADER_TABLE_QUERY_CONFIG_DISPLAY = "Data Header Query";

    public static final String HEADER_TABLE_UPDATE_QUERY_CONFIG = "data.header.update.query";
    private static final String HEADER_TABLE_UPDATE_QUERY_CONFIG_DOC =
            "update query of header of dataset. ";
    public static final String HEADER_TABLE_UPDATE_QUERY_CONFIG_DEFAULT = null;
    private static final String HEADER_TABLE_UPDATE_QUERY_CONFIG_DISPLAY = "Data Header UPDATE Query";

    public static final String HEADER_TABLE_COMPLETE_QUERY_CONFIG = "data.header.complete.query";
    private static final String HEADER_TABLE_COMPLETE_QUERY_CONFIG_DOC =
            "complete query of header of dataset. ";
    public static final String HEADER_TABLE_COMPLETE_QUERY_CONFIG_DEFAULT = null;
    private static final String HEADER_TABLE_COMPLETE_QUERY_CONFIG_DISPLAY = "Data Header COMPLETE Query";

    public static final String TABLE_WHITELIST_CONFIG = "table.whitelist";
    private static final String TABLE_WHITELIST_DOC =
            "List of tables to include in copying. If specified, ``table.blacklist`` may not be set. "
                    + "Use a comma-separated list to specify multiple tables "
                    + "(for example, ``table.whitelist: \"User, Address, Email\"``).";
    public static final String TABLE_WHITELIST_DEFAULT = "";
    private static final String TABLE_WHITELIST_DISPLAY = "Table Whitelist";

    public static final String TABLE_KEY_CONFIG = "table.key";
    private static final String TABLE_KEY_DOC = "key of each tables";
    public static final String TABLE_KEY_DEFAULT = "";
    private static final String TABLE_KEY_DISPLAY = "Table key list";

    public static final String TABLE_HIERARCHY_CONFIG = "table.hierarchy";
    private static final String TABLE_HIERARCHY_DOC = "hierarchy of table";
    public static final String TABLE_HIERARCHY_DEFAULT = "";
    private static final String TABLE_HIERARCHY_DISPLAY = "Table Hierarchy";

    public static final String SCHEMA_PATTERN_CONFIG = "schema.pattern";
    private static final String SCHEMA_PATTERN_DOC =
            "Schema pattern to fetch table metadata from the database.\n"
                    + "  * ``\"\"`` retrieves those without a schema.\n"
                    + "  * null (default) indicates that the schema name is not used to narrow the search and "
                    + "that all table metadata is fetched, regardless of the schema.";
    private static final String SCHEMA_PATTERN_DISPLAY = "Schema pattern";
    public static final String SCHEMA_PATTERN_DEFAULT = null;

    public static final String CATALOG_PATTERN_CONFIG = "catalog.pattern";
    private static final String CATALOG_PATTERN_DOC =
            "Catalog pattern to fetch table metadata from the database.\n"
                    + "  * ``\"\"`` retrieves those without a catalog \n"
                    + "  * null (default) indicates that the schema name is not used to narrow the search and "
                    + "that all table metadata is fetched, regardless of the catalog.";
    private static final String CATALOG_PATTERN_DISPLAY = "Schema pattern";
    public static final String CATALOG_PATTERN_DEFAULT = null;

    public static final String TOPIC_NAME_CONFIG = "topic.name";
    private static final String TOPIC_NAME_DOC = "Topic name";
    private static final String TOPIC_NAME_DISPLAY = "Topic Name";

    public static final String VALIDATE_NON_NULL_CONFIG = "validate.non.null";
    private static final String VALIDATE_NON_NULL_DOC =
            "By default, the JDBC connector will validate that all incrementing and timestamp tables "
                    + "have NOT NULL set for the columns being used as their ID/timestamp. If the tables don't,"
                    + " JDBC connector will fail to start. Setting this to false will disable these checks.";
    public static final boolean VALIDATE_NON_NULL_DEFAULT = true;
    private static final String VALIDATE_NON_NULL_DISPLAY = "Validate Non Null";

    public static final String TIMESTAMP_DELAY_INTERVAL_MS_CONFIG = "timestamp.delay.interval.ms";
    private static final String TIMESTAMP_DELAY_INTERVAL_MS_DOC =
            "How long to wait after a row with certain timestamp appears before we include it in the "
                    + "result. You may choose to add some delay to allow transactions with earlier timestamp to"
                    + " complete. The first execution will fetch all available records (i.e. starting at "
                    + "timestamp 0) until current time minus the delay. Every following execution will get data"
                    + " from the last time we fetched until current time minus the delay.";
    public static final long TIMESTAMP_DELAY_INTERVAL_MS_DEFAULT = 0;
    private static final String TIMESTAMP_DELAY_INTERVAL_MS_DISPLAY = "Delay Interval (ms)";

    public static final String DB_TIMEZONE_CONFIG = "db.timezone";
    public static final String DB_TIMEZONE_DEFAULT = "UTC";
    private static final String DB_TIMEZONE_CONFIG_DOC =
            "Name of the JDBC timezone used in the connector when "
                    + "querying with time-based criteria. Defaults to UTC.";
    private static final String DB_TIMEZONE_CONFIG_DISPLAY = "DB time zone";

    public static final String QUOTE_SQL_IDENTIFIERS_CONFIG = "quote.sql.identifiers";
    public static final String QUOTE_SQL_IDENTIFIERS_DEFAULT = QuoteMethod.ALWAYS.name().toString();
    public static final String QUOTE_SQL_IDENTIFIERS_DOC =
            "When to quote table names, column names, and other identifiers in SQL statements. "
                    + "For backward compatibility, the default is ``always``.";
    public static final String QUOTE_SQL_IDENTIFIERS_DISPLAY = "Quote Identifiers";

    public static final String QUERY_SUFFIX_CONFIG = "query.suffix";
    public static final String QUERY_SUFFIX_DEFAULT = "";
    public static final String QUERY_SUFFIX_DOC =
            "Suffix to append at the end of the generated query.";
    public static final String QUERY_SUFFIX_DISPLAY = "Query suffix";

    private static final EnumRecommender QUOTE_METHOD_RECOMMENDER =
            EnumRecommender.in(QuoteMethod.values());

    public static final String DATABASE_GROUP = "Database";
    public static final String MODE_GROUP = "Mode";
    public static final String CONNECTOR_GROUP = "Connector";

    private static final Recommender MODE_DEPENDENTS_RECOMMENDER =  new ModeDependentsRecommender();


    public static final String TABLE_TYPE_DEFAULT = "TABLE";
    public static final String TABLE_TYPE_CONFIG = "table.types";
    private static final String TABLE_TYPE_DOC =
            "By default, the JDBC connector will only detect tables with type TABLE from the source "
                    + "Database. This config allows a command separated list of table types to extract. Options"
                    + " include:\n"
                    + "  * TABLE\n"
                    + "  * VIEW\n"
                    + "  * SYSTEM TABLE\n"
                    + "  * GLOBAL TEMPORARY\n"
                    + "  * LOCAL TEMPORARY\n"
                    + "  * ALIAS\n"
                    + "  * SYNONYM\n"
                    + "  In most cases it only makes sense to have either TABLE or VIEW.";
    private static final String TABLE_TYPE_DISPLAY = "Table Types";

    public static ConfigDef baseConfigDef() {
        ConfigDef config = new ConfigDef();
        addDatabaseOptions(config);
        addModeOptions(config);
        addConnectorOptions(config);
        return config;
    }

    private static final void addDatabaseOptions(ConfigDef config) {
        int orderInGroup = 0;
        config.define(
                CONNECTION_URL_CONFIG,
                Type.STRING,
                Importance.HIGH,
                CONNECTION_URL_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.LONG,
                CONNECTION_URL_DISPLAY,
                Arrays.asList(TABLE_WHITELIST_CONFIG)
        ).define(
                CONNECTION_USER_CONFIG,
                Type.STRING,
                null,
                Importance.HIGH,
                CONNECTION_USER_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.LONG,
                CONNECTION_USER_DISPLAY
        ).define(
                CONNECTION_PASSWORD_CONFIG,
                Type.PASSWORD,
                null,
                Importance.HIGH,
                CONNECTION_PASSWORD_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.SHORT,
                CONNECTION_PASSWORD_DISPLAY
        ).define(
                CONNECTION_ATTEMPTS_CONFIG,
                Type.INT,
                CONNECTION_ATTEMPTS_DEFAULT,
                ConfigDef.Range.atLeast(1),
                Importance.LOW,
                CONNECTION_ATTEMPTS_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.SHORT,
                CONNECTION_ATTEMPTS_DISPLAY
        ).define(
                CONNECTION_BACKOFF_CONFIG,
                Type.LONG,
                CONNECTION_BACKOFF_DEFAULT,
                Importance.LOW,
                CONNECTION_BACKOFF_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.SHORT,
                CONNECTION_BACKOFF_DISPLAY
        ).define(
                HEADER_TABLE_CONFIG,
                Type.STRING,
                HEADER_TABLE_CONFIG_DEFAULT,
                Importance.LOW,
                HEADER_TABLE_CONFIG_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.SHORT,
                HEADER_TABLE_CONFIG_DISPLAY
        ).define(
                HEADER_TABLE_QUERY_CONFIG,
                Type.STRING,
                HEADER_TABLE_QUERY_CONFIG_DEFAULT,
                Importance.LOW,
                HEADER_TABLE_QUERY_CONFIG_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.SHORT,
                HEADER_TABLE_QUERY_CONFIG_DISPLAY
        ).define(
                HEADER_TABLE_UPDATE_QUERY_CONFIG,
                Type.STRING,
                HEADER_TABLE_UPDATE_QUERY_CONFIG_DEFAULT,
                Importance.LOW,
                HEADER_TABLE_UPDATE_QUERY_CONFIG_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.SHORT,
                HEADER_TABLE_UPDATE_QUERY_CONFIG_DISPLAY
        ).define(
                HEADER_TABLE_COMPLETE_QUERY_CONFIG,
                Type.STRING,
                HEADER_TABLE_COMPLETE_QUERY_CONFIG_DEFAULT,
                Importance.LOW,
                HEADER_TABLE_COMPLETE_QUERY_CONFIG_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.SHORT,
                HEADER_TABLE_COMPLETE_QUERY_CONFIG_DISPLAY
        ).define(
                TABLE_WHITELIST_CONFIG,
                Type.LIST,
                TABLE_WHITELIST_DEFAULT,
                Importance.MEDIUM,
                TABLE_WHITELIST_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.LONG,
                TABLE_WHITELIST_DISPLAY
        ).define(
                TABLE_KEY_CONFIG,
                Type.STRING,
                TABLE_KEY_DEFAULT,
                Importance.MEDIUM,
                TABLE_KEY_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.LONG,
                TABLE_KEY_DISPLAY
        ).define(
                TABLE_HIERARCHY_CONFIG,
                Type.STRING,
                TABLE_HIERARCHY_DEFAULT,
                Importance.MEDIUM,
                TABLE_HIERARCHY_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.LONG,
                TABLE_HIERARCHY_DISPLAY
        ).define(
                CATALOG_PATTERN_CONFIG,
                Type.STRING,
                CATALOG_PATTERN_DEFAULT,
                Importance.MEDIUM,
                CATALOG_PATTERN_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.SHORT,
                CATALOG_PATTERN_DISPLAY
        ).define(
                SCHEMA_PATTERN_CONFIG,
                Type.STRING,
                SCHEMA_PATTERN_DEFAULT,
                Importance.HIGH,
                SCHEMA_PATTERN_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.SHORT,
                SCHEMA_PATTERN_DISPLAY
        ).define(
                NUMERIC_PRECISION_MAPPING_CONFIG,
                Type.BOOLEAN,
                NUMERIC_PRECISION_MAPPING_DEFAULT,
                Importance.LOW,
                NUMERIC_PRECISION_MAPPING_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.SHORT,
                NUMERIC_PRECISION_MAPPING_DISPLAY
        ).define(
                NUMERIC_MAPPING_CONFIG,
                Type.STRING,
                NUMERIC_MAPPING_DEFAULT,
                NUMERIC_MAPPING_RECOMMENDER,
                Importance.LOW,
                NUMERIC_MAPPING_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.SHORT,
                NUMERIC_MAPPING_DISPLAY,
                NUMERIC_MAPPING_RECOMMENDER
        );
    }

    private static final void addModeOptions(ConfigDef config) {
        int orderInGroup = 0;
        config.define(
                MODE_CONFIG,
                Type.STRING,
                MODE_UNSPECIFIED,
                ConfigDef.ValidString.in(
                        MODE_UNSPECIFIED,
                        MODE_BULK,
                        MODE_TIMESTAMP,
                        MODE_INCREMENTING,
                        MODE_TIMESTAMP_INCREMENTING
                ),
                Importance.HIGH,
                MODE_DOC,
                MODE_GROUP,
                ++orderInGroup,
                Width.MEDIUM,
                MODE_DISPLAY,
                Arrays.asList(
                        INCREMENTING_COLUMN_NAME_CONFIG,
                        TIMESTAMP_COLUMN_NAME_CONFIG,
                        VALIDATE_NON_NULL_CONFIG
                )
        ).define(
                INCREMENTING_COLUMN_NAME_CONFIG,
                Type.STRING,
                INCREMENTING_COLUMN_NAME_DEFAULT,
                Importance.MEDIUM,
                INCREMENTING_COLUMN_NAME_DOC,
                MODE_GROUP,
                ++orderInGroup,
                Width.MEDIUM,
                INCREMENTING_COLUMN_NAME_DISPLAY,
                MODE_DEPENDENTS_RECOMMENDER
        ).define(
                TIMESTAMP_COLUMN_NAME_CONFIG,
                Type.LIST,
                TIMESTAMP_COLUMN_NAME_DEFAULT,
                Importance.MEDIUM,
                TIMESTAMP_COLUMN_NAME_DOC,
                MODE_GROUP,
                ++orderInGroup,
                Width.MEDIUM,
                TIMESTAMP_COLUMN_NAME_DISPLAY,
                MODE_DEPENDENTS_RECOMMENDER
        ).define(
                TIMESTAMP_INITIAL_CONFIG,
                Type.LONG,
                TIMESTAMP_INITIAL_DEFAULT,
                Importance.LOW,
                TIMESTAMP_INITIAL_DOC,
                MODE_GROUP,
                ++orderInGroup,
                Width.MEDIUM,
                TIMESTAMP_INITIAL_DISPLAY,
                MODE_DEPENDENTS_RECOMMENDER
        ).define(
                VALIDATE_NON_NULL_CONFIG,
                Type.BOOLEAN,
                VALIDATE_NON_NULL_DEFAULT,
                Importance.LOW,
                VALIDATE_NON_NULL_DOC,
                MODE_GROUP,
                ++orderInGroup,
                Width.SHORT,
                VALIDATE_NON_NULL_DISPLAY,
                MODE_DEPENDENTS_RECOMMENDER
        ).define(
                QUOTE_SQL_IDENTIFIERS_CONFIG,
                Type.STRING,
                QUOTE_SQL_IDENTIFIERS_DEFAULT,
                Importance.MEDIUM,
                QUOTE_SQL_IDENTIFIERS_DOC,
                MODE_GROUP,
                ++orderInGroup,
                Width.MEDIUM,
                QUOTE_SQL_IDENTIFIERS_DISPLAY,
                QUOTE_METHOD_RECOMMENDER
        ).define(
                QUERY_SUFFIX_CONFIG,
                Type.STRING,
                QUERY_SUFFIX_DEFAULT,
                Importance.LOW,
                QUERY_SUFFIX_DOC,
                MODE_GROUP,
                ++orderInGroup,
                Width.MEDIUM,
                QUERY_SUFFIX_DISPLAY);
    }

    private static final void addConnectorOptions(ConfigDef config) {
        int orderInGroup = 0;
        config.define(
                TABLE_TYPE_CONFIG,
                Type.LIST,
                TABLE_TYPE_DEFAULT,
                Importance.LOW,
                TABLE_TYPE_DOC,
                CONNECTOR_GROUP,
                ++orderInGroup,
                Width.MEDIUM,
                TABLE_TYPE_DISPLAY
        ).define(
                POLL_INTERVAL_MS_CONFIG,
                Type.INT,
                POLL_INTERVAL_MS_DEFAULT,
                Importance.HIGH,
                POLL_INTERVAL_MS_DOC,
                CONNECTOR_GROUP,
                ++orderInGroup,
                Width.SHORT,
                POLL_INTERVAL_MS_DISPLAY
        ).define(
                BATCH_MAX_ROWS_CONFIG,
                Type.INT,
                BATCH_MAX_ROWS_DEFAULT,
                Importance.LOW,
                BATCH_MAX_ROWS_DOC,
                CONNECTOR_GROUP,
                ++orderInGroup,
                Width.SHORT,
                BATCH_MAX_ROWS_DISPLAY
        ).define(
                TABLE_POLL_INTERVAL_MS_CONFIG,
                Type.LONG,
                TABLE_POLL_INTERVAL_MS_DEFAULT,
                Importance.LOW,
                TABLE_POLL_INTERVAL_MS_DOC,
                CONNECTOR_GROUP,
                ++orderInGroup,
                Width.SHORT,
                TABLE_POLL_INTERVAL_MS_DISPLAY
        ).define(
                TOPIC_NAME_CONFIG,
                Type.STRING,
                "",
                new Validator() {
                    @Override
                    public void ensureValid(final String name, final Object value) {
                        if (value == null) {
                            throw new ConfigException(name, value, "Topic name must not be null.");
                        }

                        String trimmed = ((String) value).trim();

                        if (trimmed.length() > 249) {
                            throw new ConfigException(name, value,
                                    "Topic name length must not exceed 249 chars");
                        }

                        if (INVALID_CHARS.matcher(trimmed).find()) {
                            throw new ConfigException(name, value,
                                    "Topic name must not contain any character other than "
                                            + "ASCII alphanumerics, '.', '_' and '-'.");
                        }
                    }
                },
                Importance.HIGH,
                TOPIC_NAME_DOC,
                CONNECTOR_GROUP,
                ++orderInGroup,
                Width.MEDIUM,
                TOPIC_NAME_DISPLAY
        ).define(
                TIMESTAMP_DELAY_INTERVAL_MS_CONFIG,
                Type.LONG,
                TIMESTAMP_DELAY_INTERVAL_MS_DEFAULT,
                Importance.HIGH,
                TIMESTAMP_DELAY_INTERVAL_MS_DOC,
                CONNECTOR_GROUP,
                ++orderInGroup,
                Width.MEDIUM,
                TIMESTAMP_DELAY_INTERVAL_MS_DISPLAY
        ).define(
                DB_TIMEZONE_CONFIG,
                Type.STRING,
                DB_TIMEZONE_DEFAULT,
                TimeZoneValidator.INSTANCE,
                Importance.MEDIUM,
                DB_TIMEZONE_CONFIG_DOC,
                CONNECTOR_GROUP,
                ++orderInGroup,
                Width.MEDIUM,
                DB_TIMEZONE_CONFIG_DISPLAY);
    }

    public static final ConfigDef CONFIG_DEF = baseConfigDef();

    public MultiTableSourceConnectorConfig(Map<String, ?> props) {
        super(CONFIG_DEF, props);
        String mode = getString(MultiTableSourceConnectorConfig.MODE_CONFIG);
        if (mode.equals(MultiTableSourceConnectorConfig.MODE_UNSPECIFIED)) {
            throw new ConfigException("Query mode must be specified");
        }
    }

    public String topicName() {
        return getString(MultiTableSourceTaskConfig.TOPIC_NAME_CONFIG).trim();
    }

    /**
     * A recommender that caches values returned by a delegate, where the cache remains valid for a
     * specified duration and as long as the configuration remains unchanged.
     */
    static class CachingRecommender implements Recommender {

        private final Time time;
        private final long cacheDurationInMillis;
        private final AtomicReference<CachedRecommenderValues> cachedValues
                = new AtomicReference<>(new CachedRecommenderValues());
        private final Recommender delegate;

        public CachingRecommender(Recommender delegate, Time time, long cacheDurationInMillis) {
            this.delegate = delegate;
            this.time = time;
            this.cacheDurationInMillis = cacheDurationInMillis;
        }

        @Override
        public List<Object> validValues(String name, Map<String, Object> config) {
            List<Object> results = cachedValues.get().cachedValue(config, time.milliseconds());
            if (results != null) {
                LOG.debug("Returning cached table names: {}", results);
                return results;
            }
            LOG.trace("Fetching table names");
            results = delegate.validValues(name, config);
            LOG.debug("Caching table names: {}", results);
            long expireTime = time.milliseconds() + cacheDurationInMillis;
            cachedValues.set(new CachedRecommenderValues(config, results, expireTime));
            return results;
        }

        @Override
        public boolean visible(String name, Map<String, Object> config) {
            return true;
        }
    }

    static class CachedRecommenderValues {
        private final Map<String, Object> lastConfig;
        private final List<Object> results;
        private final long expiryTimeInMillis;

        public CachedRecommenderValues() {
            this(null, null, 0L);
        }

        public CachedRecommenderValues(
                Map<String, Object> lastConfig,
                List<Object> results, long expiryTimeInMillis) {
            this.lastConfig = lastConfig;
            this.results = results;
            this.expiryTimeInMillis = expiryTimeInMillis;
        }

        public List<Object> cachedValue(Map<String, Object> config, long currentTimeInMillis) {
            if (currentTimeInMillis < expiryTimeInMillis
                    && lastConfig != null && lastConfig.equals(config)) {
                return results;
            }
            return null;
        }
    }

    private static class ModeDependentsRecommender implements Recommender {

        @Override
        public List<Object> validValues(String name, Map<String, Object> config) {
            return new LinkedList<>();
        }

        @Override
        public boolean visible(String name, Map<String, Object> config) {
            String mode = (String) config.get(MODE_CONFIG);
            switch (mode) {
                case MODE_BULK:
                    return false;
                case MODE_TIMESTAMP:
                    return name.equals(TIMESTAMP_COLUMN_NAME_CONFIG) || name.equals(VALIDATE_NON_NULL_CONFIG);
                case MODE_INCREMENTING:
                    return name.equals(INCREMENTING_COLUMN_NAME_CONFIG)
                            || name.equals(VALIDATE_NON_NULL_CONFIG);
                case MODE_TIMESTAMP_INCREMENTING:
                    return name.equals(TIMESTAMP_COLUMN_NAME_CONFIG)
                            || name.equals(INCREMENTING_COLUMN_NAME_CONFIG)
                            || name.equals(VALIDATE_NON_NULL_CONFIG);
                case MODE_UNSPECIFIED:
                    throw new ConfigException("Query mode must be specified");
                default:
                    throw new ConfigException("Invalid mode: " + mode);
            }
        }
    }

    public enum NumericMapping {
        NONE,
        PRECISION_ONLY,
        BEST_FIT,
        BEST_FIT_EAGER_DOUBLE;

        private static final Map<String, NumericMapping> reverse = new HashMap<>(values().length);
        static {
            for (NumericMapping val : values()) {
                reverse.put(val.name().toLowerCase(Locale.ROOT), val);
            }
        }

        public static NumericMapping get(String prop) {
            // not adding a check for null value because the recommender/validator should catch those.
            return reverse.get(prop.toLowerCase(Locale.ROOT));
        }

        public static NumericMapping get(MultiTableSourceConnectorConfig config) {
            String newMappingConfig = config.getString(MultiTableSourceConnectorConfig.NUMERIC_MAPPING_CONFIG);
            // We use 'null' as default to be able to check the old config if the new one is unset.
            if (newMappingConfig != null) {
                return get(config.getString(MultiTableSourceConnectorConfig.NUMERIC_MAPPING_CONFIG));
            }
            if (config.getBoolean(MultiTableSourceTaskConfig.NUMERIC_PRECISION_MAPPING_CONFIG)) {
                return NumericMapping.PRECISION_ONLY;
            }
            return NumericMapping.NONE;
        }
    }

    protected MultiTableSourceConnectorConfig(ConfigDef subclassConfigDef, Map<String, String> props) {
        super(subclassConfigDef, props);
    }

    public NumericMapping numericMapping() {
        return NumericMapping.get(this);
    }

    public TimeZone timeZone() {
        String dbTimeZone = getString(MultiTableSourceTaskConfig.DB_TIMEZONE_CONFIG);
        return TimeZone.getTimeZone(ZoneId.of(dbTimeZone));
    }

    public static void main(String[] args) {
        System.out.println(CONFIG_DEF.toEnrichedRst());
    }
}
