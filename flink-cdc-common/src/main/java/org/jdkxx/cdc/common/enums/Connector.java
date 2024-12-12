package org.jdkxx.cdc.common.enums;

public enum Connector {
    MySQL("MySQL"),
    Oracle("Oracle"),
    TiDB("TiDB"),
    Doris("Doris"),
    HBase("HBase"),
    Unknown("Unknown");

    private final String code;

    Connector(String code) {
        this.code = code;
    }

    public static Connector of(String code) {
        Connector[] enums = Connector.values();
        for (Connector e : enums) {
            if (e.code.equalsIgnoreCase(code)) {
                return e;
            }
        }
        return Unknown;
    }
}
