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

import org.hyperledger.besu.plugin.services.BesuConfiguration;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.plugin.services.exception.StorageException;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.plugin.services.storage.SegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBKeyValueStorageFactory;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBMetricsFactory;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDbSegmentIdentifier;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBFactoryConfiguration;
import org.hyperledger.besu.plugin.services.storage.rocksdb.unsegmented.RocksDBKeyValueStorage;
import org.hyperledger.besu.services.kvstore.SegmentedKeyValueStorageAdapter;
import org.hyperledger.besu.plugin.services.storage.rocksdb.faultinjection.RocksDBFaultInjectionConfig;
import org.hyperledger.besu.plugin.services.storage.rocksdb.faultinjection.RocksDBFaultyColumnarKeyValueStorage;

import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** The Rocks db key value storage factory. */
public class RocksDBFaultyKeyValueStorageFactory extends RocksDBKeyValueStorageFactory {

  private static final int DEFAULT_VERSION = 1;
  private RocksDBFaultyColumnarKeyValueStorage segmentedStorage;
  private final RocksDBFaultInjectionConfig faultInjectionConfig;

  /**
   * Instantiates a new RocksDb key value storage factory.
   *
   * @param configuration the configuration
   * @param segments the segments
   * @param ignorableSegments the ignorable segments
   * @param defaultVersion the default version
   * @param rocksDBMetricsFactory the rocks db metrics factory
   */
  public RocksDBFaultyKeyValueStorageFactory(
      final Supplier<RocksDBFactoryConfiguration> configuration,
      final List<SegmentIdentifier> segments,
      final List<SegmentIdentifier> ignorableSegments,
      final int defaultVersion,
      final RocksDBMetricsFactory rocksDBMetricsFactory,
      final RocksDBFaultInjectionConfig faultInjectionConfig) {
    super(configuration, segments, ignorableSegments, defaultVersion, rocksDBMetricsFactory);
    this.faultInjectionConfig = faultInjectionConfig;
  }

  /**
   * Instantiates a new RocksDb key value storage factory.
   *
   * @param configuration the configuration
   * @param segments the segments
   * @param defaultVersion the default version
   * @param rocksDBMetricsFactory the rocks db metrics factory
   */
  public RocksDBFaultyKeyValueStorageFactory(
      final Supplier<RocksDBFactoryConfiguration> configuration,
      final List<SegmentIdentifier> segments,
      final int defaultVersion,
      final RocksDBMetricsFactory rocksDBMetricsFactory,
      final RocksDBFaultInjectionConfig faultInjectionConfig) {
    this(
        configuration,
        segments,
        List.of(),
        defaultVersion,
        rocksDBMetricsFactory,
        faultInjectionConfig);
  }

  /**
   * Instantiates a new Rocks db key value storage factory.
   *
   * @param configuration the configuration
   * @param segments the segments
   * @param ignorableSegments the ignorable segments
   * @param rocksDBMetricsFactory the rocks db metrics factory
   */
  public RocksDBFaultyKeyValueStorageFactory(
      final Supplier<RocksDBFactoryConfiguration> configuration,
      final List<SegmentIdentifier> segments,
      final List<SegmentIdentifier> ignorableSegments,
      final RocksDBMetricsFactory rocksDBMetricsFactory,
      final RocksDBFaultInjectionConfig faultInjectionConfig) {
    this(
        configuration,
        segments,
        ignorableSegments,
        DEFAULT_VERSION,
        rocksDBMetricsFactory,
        faultInjectionConfig);
  }

  /**
   * Instantiates a new Rocks db key value storage factory.
   *
   * @param configuration the configuration
   * @param segments the segments
   * @param rocksDBMetricsFactory the rocks db metrics factory
   */
  public RocksDBFaultyKeyValueStorageFactory(
      final Supplier<RocksDBFactoryConfiguration> configuration,
      final List<SegmentIdentifier> segments,
      final RocksDBMetricsFactory rocksDBMetricsFactory,
      final RocksDBFaultInjectionConfig faultInjectionConfig) {
    this(
        configuration,
        segments,
        List.of(),
        DEFAULT_VERSION,
        rocksDBMetricsFactory,
        faultInjectionConfig);
  }

  @Override
  public KeyValueStorage create(
      final SegmentIdentifier segment,
      final BesuConfiguration commonConfiguration,
      final MetricsSystem metricsSystem)
      throws StorageException {

    if (requiresInit()) {
      init(commonConfiguration);
    }

    // It's probably a good idea for the creation logic to be entirely dependent on the database
    // version. Introducing intermediate booleans that represent database properties and dispatching
    // creation logic based on them is error prone.
    switch (databaseVersion) {
      case 0:
        {
          segmentedStorage = null;
          if (unsegmentedStorage == null) {
            unsegmentedStorage =
                new RocksDBKeyValueStorage(
                    rocksDBConfiguration, metricsSystem, rocksDBMetricsFactory);
          }
          return unsegmentedStorage;
        }
      case 1:
      case 2:
        {
          unsegmentedStorage = null;
          if (segmentedStorage == null) {
            final List<SegmentIdentifier> segmentsForVersion =
                segments.stream()
                    .filter(segmentId -> segmentId.includeInDatabaseVersion(databaseVersion))
                    .collect(Collectors.toList());

            segmentedStorage =
                new RocksDBFaultyColumnarKeyValueStorage(
                    rocksDBConfiguration,
                    segmentsForVersion,
                    ignorableSegments,
                    metricsSystem,
                    rocksDBMetricsFactory);

            segmentedStorage.setFaultInjectionConfig(faultInjectionConfig);
          }
          final RocksDbSegmentIdentifier rocksSegment =
              segmentedStorage.getSegmentIdentifierByName(segment);
          return new SegmentedKeyValueStorageAdapter<>(
              segment, segmentedStorage, () -> segmentedStorage.takeSnapshot(rocksSegment));
        }
      default:
        {
          throw new IllegalStateException(
              String.format(
                  "Developer error: A supported database version (%d) was detected but there is no associated creation logic.",
                  databaseVersion));
        }
    }
  }
}
