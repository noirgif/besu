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

public class RocksDBFaultInjectionConfig {
  private final boolean enableFaultInjection;
  private final int faultInjectionCount;

  public RocksDBFaultInjectionConfig(final boolean enableFaultInjection, final int faultInjectionCount) {
    this.enableFaultInjection = enableFaultInjection;
    this.faultInjectionCount = faultInjectionCount;
  }

  public boolean isFaultInjectionEnabled() {
    return enableFaultInjection;
  }

  public int getFaultInjectionCount() {
    return faultInjectionCount;
  }
}
