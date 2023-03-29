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

import org.hyperledger.besu.controller.BesuController;
import org.hyperledger.besu.controller.MergeBesuControllerBuilder;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueStorageProvider;
import org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueStorageProviderBuilder;
import org.hyperledger.besu.metrics.noop.NoOpMetricsSystem;
import org.hyperledger.besu.plugin.services.BesuConfiguration;
import org.hyperledger.besu.plugin.services.storage.rocksdb.RocksDBMetricsFactory;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.RocksDBFactoryConfiguration;
import org.hyperledger.besu.plugin.services.storage.rocksdb.faultinjection.RocksDBFaultInjectionConfig;
import org.hyperledger.besu.plugin.services.storage.rocksdb.faultinjection.RocksDBFaultyKeyValueStorageFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
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
            Files.copy(file, tempData.resolve(originalData.relativize(file)));
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
                new BesuConfiguration() {
                  @Override
                  public Path getStoragePath() {
                    return new File(tempData.toString() + File.pathSeparator + "database")
                        .toPath();
                  }

                  @Override
                  public Path getDataPath() {
                    return tempData;
                  }

                  @Override
                  public int getDatabaseVersion() {
                    return 2;
                  }
                })
            .withMetricsSystem(new NoOpMetricsSystem())
            .build();

    // set the storage controller to fault

    besuController =
        new MergeBesuControllerBuilder()
            .dataDirectory(tempData)
            .storageProvider(keyValueStorageProvider)
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
