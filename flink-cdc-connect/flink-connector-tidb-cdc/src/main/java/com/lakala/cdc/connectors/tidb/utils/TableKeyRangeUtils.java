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

package com.lakala.cdc.connectors.tidb.utils;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava31.com.google.common.collect.TreeMultiset;
import org.apache.flink.util.Preconditions;
import com.lakala.cdc.connectors.tidb.schema.TiTableMetadata;
import com.lakala.cdc.connectors.tidb.schema.TiTablePartition;
import org.tikv.common.key.RowKey;
import org.tikv.common.util.KeyRangeUtils;
import org.tikv.kvproto.Coprocessor.KeyRange;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * Utils to obtain the keyRange of table.
 */
public class TableKeyRangeUtils {
    public static KeyRange getTableKeyRange(final TiTableMetadata table, final int num, final int idx) {
        Preconditions.checkArgument(idx >= 0 && idx < num, "Illegal value of idx");
        return getTableKeyRanges(table, num).get(idx);
    }

    public static KeyRange getTableKeyRange(final TiTableMetadata table, final int num) {
        return getTableKeyRanges(table, num).getFirst();
    }

    public static KeyRange getTableKeyRange(final TiTableMetadata table) {
        return getTableKeyRanges(table, 1).getFirst();
    }

    public static boolean isRecordKey(final byte[] key) {
        return key[9] == '_' && key[10] == 'r';
    }

    private static List<KeyRange> getTableKeyRanges(final TiTableMetadata table, final int num) {
        Preconditions.checkArgument(num > 0, "Illegal value of num");

        TreeMultiset<Long> set = getTableIdRange(table);

        if (num == 1) {
            KeyRange keyRange = KeyRangeUtils.makeCoprocRange(
                    RowKey.createMin(set.firstEntry().getElement()).toByteString(),
                    RowKey.createBeyondMax(set.lastEntry().getElement()).toByteString());
            return ImmutableList.of(keyRange);
        }

        final long delta =
                BigInteger.valueOf(Long.MAX_VALUE)
                        .subtract(BigInteger.valueOf(Long.MIN_VALUE + 1))
                        .divide(BigInteger.valueOf(num))
                        .longValueExact();
        final ImmutableList.Builder<KeyRange> builder = ImmutableList.builder();

        for (int i = 0; i < num; i++) {
            final RowKey startKey =
                    (i == 0)
                            ? RowKey.createMin(set.firstEntry().getElement())
                            : RowKey.toRowKey(set.lastEntry().getElement(), Long.MIN_VALUE + delta * i);
            final RowKey endKey =
                    (i == num - 1)
                            ? RowKey.createBeyondMax(set.firstEntry().getElement())
                            : RowKey.toRowKey(set.lastEntry().getElement(), Long.MIN_VALUE + delta * (i + 1));
            builder.add(
                    KeyRangeUtils.makeCoprocRange(startKey.toByteString(), endKey.toByteString()));
        }
        return builder.build();
    }

    private static TreeMultiset<Long> getTableIdRange(final TiTableMetadata table) {
        TreeMultiset<Long> set = TreeMultiset.create();
        if (table.hasPartitions()) {
            List<Long> partitionIds = table.getPartitions().stream()
                    .sorted(Comparator.comparingLong(TiTablePartition::getId))
                    .map(TiTablePartition::getId).toList();
            set.addAll(partitionIds);
        } else {
            set.add(table.getId());
        }
        return set;
    }
}
