package org.jdkxx.cdc.connectors.tidb.schema;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class TiTablePartition implements Serializable {
    private long id;
    private String name;
    private PartitionType type;

    public enum PartitionType {
        RangePartition,
        HashPartition,
        ListPartition,
        InvalidPartition,
    }
}
