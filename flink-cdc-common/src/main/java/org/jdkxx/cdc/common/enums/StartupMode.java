package org.jdkxx.cdc.common.enums;

public enum StartupMode {
    INITIAL("initial"),

    EARLIEST_OFFSET("earliest"),

    LATEST_OFFSET("latest"),

    SPECIFIC_OFFSETS("offset"),

    TIMESTAMP("timestamp"),

    SNAPSHOT("snapshot"),

    UNKNOWN("unknown");

    private final String code;

    StartupMode(String code) {
        this.code = code;
    }

    public static StartupMode of(String code) {
        for (StartupMode startupMode : StartupMode.values()) {
            if (startupMode.code.equalsIgnoreCase(code)) {
                return startupMode;
            }
        }
        return UNKNOWN;
    }
}
