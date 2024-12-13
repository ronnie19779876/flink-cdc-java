package org.jdkxx.cdc.connectors.tidb.source.config;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.tikv.common.ConfigUtils;
import org.tikv.common.TiConfiguration;

public class TiDBSourceOptions {
    private TiDBSourceOptions() {
    }

    public static final ConfigOption<String> TIKV_PD_ADDRESS = ConfigOptions
            .key("tikv.pd.addresses")
            .stringType()
            .defaultValue("localhost:2379");

    public static final ConfigOption<Long> TIKV_GRPC_TIMEOUT =
            ConfigOptions.key(ConfigUtils.TIKV_GRPC_TIMEOUT)
                    .longType()
                    .noDefaultValue()
                    .withDescription("TiKV GRPC timeout in ms");

    public static final ConfigOption<Long> TIKV_GRPC_SCAN_TIMEOUT =
            ConfigOptions.key(ConfigUtils.TIKV_GRPC_SCAN_TIMEOUT)
                    .longType()
                    .noDefaultValue()
                    .withDescription("TiKV GRPC scan timeout in ms");

    public static final ConfigOption<Integer> TIKV_BATCH_GET_CONCURRENCY =
            ConfigOptions.key(ConfigUtils.TIKV_BATCH_GET_CONCURRENCY)
                    .intType()
                    .noDefaultValue()
                    .withDescription("TiKV GRPC batch get concurrency");

    public static final ConfigOption<Integer> TIKV_BATCH_SCAN_CONCURRENCY =
            ConfigOptions.key(ConfigUtils.TIKV_BATCH_SCAN_CONCURRENCY)
                    .intType()
                    .noDefaultValue()
                    .withDescription("TiKV GRPC batch scan concurrency");

    public static final String BINLOG_SPLIT_ID = "tidb-binlog-split";

    public static TiConfiguration getTiConf(final Configuration configuration) {
        String address = configuration.getOptional(TIKV_PD_ADDRESS).orElse(TIKV_PD_ADDRESS.defaultValue());
        final TiConfiguration tiConf = TiConfiguration.createDefault(address);
        configuration.getOptional(TIKV_GRPC_TIMEOUT).ifPresent(tiConf::setTimeout);
        configuration.getOptional(TIKV_GRPC_SCAN_TIMEOUT).ifPresent(tiConf::setScanTimeout);
        configuration.getOptional(TIKV_BATCH_GET_CONCURRENCY).ifPresent(tiConf::setBatchGetConcurrency);
        configuration.getOptional(TIKV_BATCH_SCAN_CONCURRENCY).ifPresent(tiConf::setBatchScanConcurrency);
        return tiConf;
    }
}
