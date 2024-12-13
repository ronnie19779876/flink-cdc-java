package org.jdkxx.cdc.schema.test;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.flink.configuration.Configuration;
import org.jdkxx.cdc.common.options.SourceOptions;
import org.jdkxx.cdc.schema.metadata.TableMetadata;
import org.jdkxx.cdc.schema.metadata.mysql.MySqlSchema;
import org.jdkxx.cdc.schema.metadata.table.TablePath;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

@Slf4j
public class MySqlSchemaTestCase {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(SourceOptions.HOSTNAME, "www.jdkxx.org");
        configuration.set(SourceOptions.PORT, 23306);
        configuration.set(SourceOptions.USERNAME, "");
        configuration.set(SourceOptions.PASSWORD, "");
        configuration.set(SourceOptions.DATABASE, "dtsdb");
        configuration.set(SourceOptions.TABLE_LIST, Arrays.asList("employees", "mysql_type_fields"));

        TableMetadata metadata = null;
        StopWatch watch = StopWatch.createStarted();
        try {
            MySqlSchema schema = MySqlSchema.builder()
                    .withConfiguration(configuration)
                    .build();
            metadata = schema.getTableMetadata(TablePath.of("dtsdb", "mysql_type_fields"));
        } finally {
            watch.stop();
            log.info("read MySQL schema cost {} ms, metadata: {}",
                    watch.getTime(TimeUnit.MILLISECONDS), Objects.nonNull(metadata));
        }
    }
}
