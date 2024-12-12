/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.mysql.source.split;

import org.apache.flink.cdc.connectors.mysql.source.offset.BinlogOffset;
import org.apache.flink.table.types.logical.RowType;

import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges.TableChange;

import java.util.HashMap;
import java.util.Map;

/**
 * A kind of {@link MySqlSnapshotSplit} without table schema information, it is useful to reduce
 * memory usage in JobManager.
 */
public class MySqlSchemalessSnapshotSplit extends MySqlSnapshotSplit {

    public MySqlSchemalessSnapshotSplit(
            TableId tableId,
            String splitId,
            RowType splitKeyType,
            Object[] splitStart,
            Object[] splitEnd,
            BinlogOffset highWatermark,
            int rows) {
        super(
                tableId,
                splitId,
                splitKeyType,
                splitStart,
                splitEnd,
                highWatermark,
                new HashMap<>(1),
                rows);
    }

    /**
     * Converts current {@link MySqlSchemalessSnapshotSplit} to {@link MySqlSnapshotSplit} with
     * given table schema information.
     */
    public final MySqlSnapshotSplit toMySqlSnapshotSplit(TableChange tableSchema) {
        Map<TableId, TableChange> tableSchemas = new HashMap<>();
        tableSchemas.put(getTableId(), tableSchema);
        return new MySqlSnapshotSplit(
                getTableId(),
                splitId(),
                getSplitKeyType(),
                getSplitStart(),
                getSplitEnd(),
                getHighWatermark(),
                tableSchemas,
                getRows());
    }
}
