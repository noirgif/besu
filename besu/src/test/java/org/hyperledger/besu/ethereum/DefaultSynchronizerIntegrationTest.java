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

package org.hyperledger.besu.ethereum;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.hyperledger.besu.controller.BesuController.DATABASE_PATH;

import org.hyperledger.besu.cli.config.EthNetworkConfig;
import org.hyperledger.besu.cli.config.NetworkName;
import org.hyperledger.besu.controller.BesuController;
import org.hyperledger.besu.controller.BesuControllerBuilder;
import org.hyperledger.besu.crypto.KeyPairSecurityModule;
import org.hyperledger.besu.crypto.KeyPairUtil;
import org.hyperledger.besu.crypto.NodeKey;
import org.hyperledger.besu.ethereum.core.MiningParameters;
import org.hyperledger.besu.ethereum.eth.EthProtocolConfiguration;
import org.hyperledger.besu.ethereum.eth.sync.SyncMode;
import org.hyperledger.besu.ethereum.eth.sync.SynchronizerConfiguration;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPoolConfiguration;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueStorageProvider;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueStorageProviderBuilder;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.metrics.MetricsSystemFactory;
import org.hyperledger.besu.metrics.ObservableMetricsSystem;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.metrics.prometheus.MetricsConfiguration;
import org.hyperledger.besu.plugin.data.EnodeURL;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBMetricsFactory;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBFactoryConfiguration;
import org.hyperledger.besu.plugin.services.storage.rocksdb.faultinjection.RocksDBFaultInjectionConfig;
import org.hyperledger.besu.plugin.services.storage.rocksdb.faultinjection.RocksDBFaultyKeyValueStorageFactory;
import org.hyperledger.besu.services.BesuConfigurationImpl;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Clock;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class DefaultSynchronizerIntegrationTest {
  @TempDir private static Path tempData;
  private static Path originalData;
  private static KeyValueStorageProvider keyValueStorageProvider;
  private static BesuController besuController;

  @BeforeAll
  public static void setup() throws IOException {
    // copy the database if exists to the temporary directory
    originalData = Paths.get("/dev/shm/besuDataDir");
    tempData = Paths.get("/dev/shm/besuDataDirTemp");

    Files.walkFileTree(
        originalData,
        new SimpleFileVisitor<Path>() {
          @Override
          public FileVisitResult preVisitDirectory(final Path dir, final BasicFileAttributes attrs)
              throws IOException {
            Path targetPath = tempData.resolve(originalData.relativize(dir));
            if (!Files.exists(targetPath)) {
              Files.createDirectory(targetPath);
            }
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs)
              throws IOException {
            Files.copy(file, tempData.resolve(originalData.relativize(file)), REPLACE_EXISTING);
            return FileVisitResult.CONTINUE;
          }
        });

    // create a storage provider, that is a factory for the RocksDB
    keyValueStorageProvider =
        new KeyValueStorageProviderBuilder()
            .withStorageFactory(
                new RocksDBFaultyKeyValueStorageFactory(
                    () ->
                        new RocksDBFactoryConfiguration(
                            1024 /* MAX_OPEN_FILES */,
                            4 /* MAX_BACKGROUND_COMPACTIONS */,
                            4 /* BACKGROUND_THREAD_COUNT */,
                            8388608 /* CACHE_CAPACITY */,
                            false),
                    Arrays.asList(KeyValueSegmentIdentifier.values()),
                    2,
                    RocksDBMetricsFactory.PUBLIC_ROCKS_DB_METRICS,
                    new RocksDBFaultInjectionConfig(false, 1)))
            .withCommonConfiguration(
                new BesuConfigurationImpl(tempData, tempData.resolve(DATABASE_PATH)))
            .withMetricsSystem(new NoOpMetricsSystem())
            .build();

    final NetworkName network = NetworkName.SEPOLIA;
    final ObservableMetricsSystem metricsSystem =
        MetricsSystemFactory.create(MetricsConfiguration.builder().build());
    final List<EnodeURL> bootnodes = List.of();
    final EthNetworkConfig.Builder networkConfigBuilder =
        new EthNetworkConfig.Builder(EthNetworkConfig.getNetworkConfig(network))
            .setBootNodes(bootnodes);
    final EthNetworkConfig ethNetworkConfig = networkConfigBuilder.build();
    final int maxPeers = 25;

    BesuControllerBuilder builder =
        new BesuController.Builder()
            .fromEthNetworkConfig(ethNetworkConfig, Collections.emptyMap(), SyncMode.FAST);

    besuController =
        builder
            .synchronizerConfiguration(new SynchronizerConfiguration.Builder().build())
            .dataDirectory(tempData)
            .miningParameters(new MiningParameters.Builder().build())
            .privacyParameters(null)
            .nodeKey(new NodeKey(new KeyPairSecurityModule(KeyPairUtil.loadKeyPair(tempData))))
            .metricsSystem(metricsSystem)
            .transactionPoolConfiguration(TransactionPoolConfiguration.DEFAULT)
            .ethProtocolConfiguration(EthProtocolConfiguration.defaultConfig())
            .clock(Clock.systemUTC())
            .isRevertReasonEnabled(false)
            .storageProvider(keyValueStorageProvider)
            .gasLimitCalculator(GasLimitCalculator.constant())
            .pkiBlockCreationConfiguration(Optional.empty())
            .evmConfiguration(EvmConfiguration.DEFAULT)
            .maxPeers(maxPeers)
            .build();

    // TODO: set up peer
    // BesuController peerBesuController = new MergeBesuControllerBuilder().build();

    // TODO: trace healing
  }

  @Test
  public void testHealWorldState() {
    // corrupt the world state and then call healWorldState
    // Some random location, will try others if it doesn't work
    Bytes corruptLocation = Bytes.fromHexString("0x000102030405");

    // TODO: have to figure out how to corrupt the world state
    besuController.getSynchronizer().healWorldState(Optional.empty(), corruptLocation);
  }
}
