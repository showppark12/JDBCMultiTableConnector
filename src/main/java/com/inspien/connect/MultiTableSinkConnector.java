package com.inspien.connect;

import com.inspien.connect.sink.MultiTableSinkConfig;
import com.inspien.connect.sink.MultiTableSinkTask;
import io.confluent.connect.jdbc.util.Version;
import org.apache.kafka.common.config.Config;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MultiTableSinkConnector extends SinkConnector {
    private static final Logger log = LoggerFactory.getLogger(MultiTableSinkConnector.class);
    private Map<String, String> configProps;

    public MultiTableSinkConnector() {
    }

    public Class<? extends Task> taskClass() {
        return MultiTableSinkTask.class;
    }

    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList(maxTasks);

        for(int i = 0; i < maxTasks; ++i) {
            configs.add(this.configProps);
        }

        return configs;
    }

    public void start(Map<String, String> props) {
        this.configProps = props;
    }

    public void stop() {
    }

    public ConfigDef config() {
        return MultiTableSinkConfig.CONFIG_DEF;
    }

    public Config validate(Map<String, String> connectorConfigs) {
        return super.validate(connectorConfigs);
    }

    public String version() {
        return Version.getVersion();
    }
}
