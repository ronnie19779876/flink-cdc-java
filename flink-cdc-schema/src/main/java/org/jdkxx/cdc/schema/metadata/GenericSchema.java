package org.jdkxx.cdc.schema.metadata;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.catalog.exceptions.TableNotExistException;
import org.jdkxx.cdc.common.options.SourceOptions;
import org.jdkxx.cdc.schema.metadata.table.TablePath;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public abstract class GenericSchema implements Serializable {
    protected Map<TablePath, TableMetadata> tables = new HashMap<>();
    protected Configuration configuration;
    protected transient HikariDataSource dataSource;

    public void open(Configuration parameters) {
        this.configuration = new Configuration(parameters);
        this.dataSource = openDataSource();
    }

    public TableMetadata getTableMetadata(TablePath path) throws TableNotExistException {
        return tables.entrySet().stream()
                .filter(entry -> StringUtils.equalsIgnoreCase(entry.getKey().getFullName(), path.getFullName()))
                .map(Map.Entry::getValue)
                .findFirst()
                .orElseThrow(() -> new TableNotExistException(path.getDatabaseName(), path.toObjectPath()));
    }

    protected HikariDataSource openDataSource() {
        Properties properties = new Properties();
        properties.setProperty("jdbcUrl", getUrlString(this.configuration));
        properties.setProperty("driverClassName", "com.mysql.cj.jdbc.Driver");
        properties.setProperty("dataSource.user", this.configuration.get(SourceOptions.USERNAME));
        properties.setProperty("dataSource.password", this.configuration.get(SourceOptions.PASSWORD));
        properties.setProperty("dataSource.maximumPoolSize", "3");
        properties.put("dataSource.remarks", "true");
        properties.put("dataSource.useInformationSchema", "true");
        HikariConfig config = new HikariConfig(properties);
        return new HikariDataSource(config);
    }

    protected String getUrlString(Configuration configuration) {
        return String.format("jdbc:mysql://%s:%s/",
                configuration.get(SourceOptions.HOSTNAME),
                configuration.get(SourceOptions.PORT));
    }

    public static abstract class Builder<T extends GenericSchema> {
        protected Configuration configuration;

        public Builder<T> withConfiguration(Configuration configuration) {
            this.configuration = configuration;
            return this;
        }

        public abstract T build();
    }
}
