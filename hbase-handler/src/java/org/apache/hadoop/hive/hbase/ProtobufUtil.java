/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.hbase;

import static org.apache.hadoop.hbase.protobuf.ProtobufUtil.toConsistency;
import static org.apache.hadoop.hbase.protobuf.ProtobufUtil.toDurability;
import static org.apache.hadoop.hbase.protobuf.ProtobufUtil.toReadType;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.util.Map;
import java.util.NavigableSet;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Consistency;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.PackagePrivateFieldAccessor;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.TimeRange;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.Column;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos.MutationProto.MutationType;
import org.apache.hadoop.hbase.protobuf.generated.FilterProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos.NameBytesPair;


final class ProtobufUtil {

  static MutationProto toMutationNoData(final MutationType type, final Mutation mutation,
      final MutationProto.Builder builder, long nonce) throws IOException {
    getMutationBuilderAndSetCommonFields(type, mutation, builder);
    builder.setAssociatedCellCount(mutation.size());
    if (mutation instanceof Increment) {
      setTimeRange(builder, ((Increment) mutation).getTimeRange());
    }
    if (nonce != HConstants.NO_NONCE) {
      builder.setNonce(nonce);
    }
    return builder.build();
  }

  public static ClientProtos.Scan toScan(final Scan scan) throws IOException {
    ClientProtos.Scan.Builder scanBuilder = ClientProtos.Scan.newBuilder();
    scanBuilder.setCacheBlocks(scan.getCacheBlocks());
    if (scan.getBatch() > 0) {
      scanBuilder.setBatchSize(scan.getBatch());
    }
    if (scan.getMaxResultSize() > 0) {
      scanBuilder.setMaxResultSize(scan.getMaxResultSize());
    }
    if (scan.isSmall()) {
      scanBuilder.setSmall(scan.isSmall());
    }
    if (scan.getAllowPartialResults()) {
      scanBuilder.setAllowPartialResults(scan.getAllowPartialResults());
    }
    Boolean loadColumnFamiliesOnDemand = scan.getLoadColumnFamiliesOnDemandValue();
    if (loadColumnFamiliesOnDemand != null) {
      scanBuilder.setLoadColumnFamiliesOnDemand(loadColumnFamiliesOnDemand);
    }
    scanBuilder.setMaxVersions(scan.getMaxVersions());
    for (Map.Entry<byte[], TimeRange> cftr : scan.getColumnFamilyTimeRange().entrySet()) {
      HBaseProtos.ColumnFamilyTimeRange.Builder b = HBaseProtos.ColumnFamilyTimeRange.newBuilder();
      b.setColumnFamily(ByteString.copyFrom(cftr.getKey()));
      b.setTimeRange(timeRangeToProto(cftr.getValue()));
      scanBuilder.addCfTimeRange(b);
    }
    TimeRange timeRange = scan.getTimeRange();
    if (!timeRange.isAllTime()) {
      HBaseProtos.TimeRange.Builder timeRangeBuilder =
        HBaseProtos.TimeRange.newBuilder();
      timeRangeBuilder.setFrom(timeRange.getMin());
      timeRangeBuilder.setTo(timeRange.getMax());
      scanBuilder.setTimeRange(timeRangeBuilder.build());
    }
    Map<String, byte[]> attributes = scan.getAttributesMap();
    if (!attributes.isEmpty()) {
      NameBytesPair.Builder attributeBuilder = NameBytesPair.newBuilder();
      for (Map.Entry<String, byte[]> attribute: attributes.entrySet()) {
        attributeBuilder.setName(attribute.getKey());
        attributeBuilder.setValue(ByteString.copyFrom(attribute.getValue()));
        scanBuilder.addAttribute(attributeBuilder.build());
      }
    }
    byte[] startRow = scan.getStartRow();
    if (startRow != null && startRow.length > 0) {
      scanBuilder.setStartRow(ByteString.copyFrom(startRow));
    }
    byte[] stopRow = scan.getStopRow();
    if (stopRow != null && stopRow.length > 0) {
      scanBuilder.setStopRow(ByteString.copyFrom(stopRow));
    }
    if (scan.hasFilter()) {
      scanBuilder.setFilter(toFilter(scan.getFilter()));
    }
    if (scan.hasFamilies()) {
      Column.Builder columnBuilder = Column.newBuilder();
      for (Map.Entry<byte[],NavigableSet<byte []>>
          family: scan.getFamilyMap().entrySet()) {
        columnBuilder.setFamily(ByteString.copyFrom(family.getKey()));
        NavigableSet<byte []> qualifiers = family.getValue();
        columnBuilder.clearQualifier();
        if (qualifiers != null && qualifiers.size() > 0) {
          for (byte [] qualifier: qualifiers) {
            columnBuilder.addQualifier(ByteString.copyFrom(qualifier));
          }
        }
        scanBuilder.addColumn(columnBuilder.build());
      }
    }
    if (scan.getMaxResultsPerColumnFamily() >= 0) {
      scanBuilder.setStoreLimit(scan.getMaxResultsPerColumnFamily());
    }
    if (scan.getRowOffsetPerColumnFamily() > 0) {
      scanBuilder.setStoreOffset(scan.getRowOffsetPerColumnFamily());
    }
    if (scan.isReversed()) {
      scanBuilder.setReversed(scan.isReversed());
    }
    if (scan.getConsistency() == Consistency.TIMELINE) {
      scanBuilder.setConsistency(toConsistency(scan.getConsistency()));
    }
    if (scan.getCaching() > 0) {
      scanBuilder.setCaching(scan.getCaching());
    }
    long mvccReadPoint = PackagePrivateFieldAccessor.getMvccReadPoint(scan);
    if (mvccReadPoint > 0) {
      scanBuilder.setMvccReadPoint(mvccReadPoint);
    }
    if (!scan.includeStartRow()) {
      scanBuilder.setIncludeStartRow(false);
    }
    if (scan.includeStopRow()) {
      scanBuilder.setIncludeStopRow(true);
    }
    if (scan.getReadType() != Scan.ReadType.DEFAULT) {
      scanBuilder.setReadType(toReadType(scan.getReadType()));
    }
    return scanBuilder.build();
  }

  private static MutationProto.Builder getMutationBuilderAndSetCommonFields(final MutationType type,
      final Mutation mutation, MutationProto.Builder builder) {
    builder.setRow(ByteString.copyFrom(mutation.getRow()));
    builder.setMutateType(type);
    builder.setDurability(toDurability(mutation.getDurability()));
    builder.setTimestamp(mutation.getTimeStamp());
    Map<String, byte[]> attributes = mutation.getAttributesMap();
    if (!attributes.isEmpty()) {
      NameBytesPair.Builder attributeBuilder = NameBytesPair.newBuilder();
      for (Map.Entry<String, byte[]> attribute: attributes.entrySet()) {
        attributeBuilder.setName(attribute.getKey());
        attributeBuilder.setValue(ByteString.copyFrom(attribute.getValue()));
        builder.addAttribute(attributeBuilder.build());
      }
    }
    return builder;
  }

  private static void setTimeRange(final MutationProto.Builder builder, final TimeRange timeRange) {
    if (!timeRange.isAllTime()) {
      HBaseProtos.TimeRange.Builder timeRangeBuilder = HBaseProtos.TimeRange.newBuilder();
      timeRangeBuilder.setFrom(timeRange.getMin());
      timeRangeBuilder.setTo(timeRange.getMax());
      builder.setTimeRange(timeRangeBuilder.build());
    }
  }

  private static HBaseProtos.TimeRange.Builder timeRangeToProto(TimeRange timeRange) {
    HBaseProtos.TimeRange.Builder timeRangeBuilder = HBaseProtos.TimeRange.newBuilder();
    timeRangeBuilder.setFrom(timeRange.getMin());
    timeRangeBuilder.setTo(timeRange.getMax());
    return timeRangeBuilder;
  }

  private static FilterProtos.Filter toFilter(Filter filter) throws IOException {
    FilterProtos.Filter.Builder builder = FilterProtos.Filter.newBuilder();
    builder.setName(filter.getClass().getName());
    builder.setSerializedFilter(ByteString.copyFrom(filter.toByteArray()));
    return builder.build();
  }

  private ProtobufUtil() {}
}
