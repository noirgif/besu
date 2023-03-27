/*
 * Copyright ConsenSys AG.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package org.hyperledger.besu.plugin.services.storage.rocksdb.faultinjection;

import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBMetricsFactory;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDbSegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBConfiguration;
import org.hyperledger.besu.plugin.services.storage.rocksdb.segmented.RocksDBColumnarKeyValueStorage;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.codec.binary.Hex;

public class RocksDBFaultyColumnarKeyValueStorage extends RocksDBColumnarKeyValueStorage {
  private int faultCounter = 0;
  private RocksDBFaultInjectionConfig faultInjectionConfig;

  public RocksDBFaultyColumnarKeyValueStorage(
      final RocksDBConfiguration configuration,
      final List<SegmentIdentifier> segments,
      final MetricsSystem metricsSystem,
      final RocksDBMetricsFactory rocksDBMetricsFactory)
      throws StorageException {
    super(configuration, segments, List.of(), metricsSystem, rocksDBMetricsFactory);

    faultInjectionConfig = new RocksDBFaultInjectionConfig(false, 0);
  }

  public RocksDBFaultyColumnarKeyValueStorage(
      final RocksDBConfiguration configuration,
      final List<SegmentIdentifier> segments,
      final List<SegmentIdentifier> ignorableSegments,
      final MetricsSystem metricsSystem,
      final RocksDBMetricsFactory rocksDBMetricsFactory)
      throws StorageException {
    super(configuration, segments, ignorableSegments, metricsSystem, rocksDBMetricsFactory);

    faultInjectionConfig = new RocksDBFaultInjectionConfig(false, 0);
  }

  public void setFaultInjectionConfig(RocksDBFaultInjectionConfig config) {
    if (config != null) {
      faultInjectionConfig = config;
    }
  }

  public boolean injectFault(final RocksDbSegmentIdentifier segment, final byte[] key) {
    if (!faultInjectionConfig.isFaultInjectionEnabled()) {
      return false;
    } else {
      faultCounter++;
      return (faultCounter == faultInjectionConfig.getFaultInjectionCount());
    }
  }

  @Override
  public Optional<byte[]> get(final RocksDbSegmentIdentifier segment, final byte[] key)
      throws StorageException {
    Optional<byte[]> result = super.get(segment, key);
    if (injectFault(segment, key)) {
      String segmentName = "";
      // Find the segment name to print, to avoid getting the atomic reference
      for (Map.Entry<String, RocksDbSegmentIdentifier> e : columnHandlesByName.entrySet()) {
        if (e.getValue().equals(segment)) {
          segmentName = e.getKey();
        }
      }
      // Since any RocksDBException is wrapped in a StorageException, we throw one directly
      // TODO: see if similar exceptions can be handled by the caller, or does the caller
      // distinguish between different RocksDBExceptions?
      throw new StorageException(
          "Injected fault on get" + segmentName + " " + Hex.encodeHexString(key));
    }
    return result;
  }
}
