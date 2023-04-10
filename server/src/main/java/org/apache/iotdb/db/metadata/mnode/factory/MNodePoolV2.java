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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.metadata.mnode.factory;

import org.apache.iotdb.db.metadata.mnode.IMNode;

import java.util.List;
import java.util.function.Supplier;

public class MNodePoolV2<N extends IMNode> {
  private final Supplier<N> creator;
  private final int size;
  private final MNodePartition<N>[] partitions;
  private final int partitionNum;

  public MNodePoolV2(Supplier<N> creator, int size, int partitionNum) {
    this.creator = creator;
    this.size = size;
    this.partitions = new MNodePartition[partitionNum];
    this.partitionNum = partitionNum;
    for (int i = 0; i < partitionNum; i++) {
      partitions[i] = new MNodePartition<>(size, creator);
    }
  }

  private MNodePartition<N> getPartition() {
    int partition = (int) (Thread.currentThread().getId() % this.partitionNum);
    return partitions[partition];
  }

  public N borrowObject() throws Exception {
    return getPartition().borrowObject();
  }

  public void returnObject(N node) {
    getPartition().returnObject(node);
  }

  public void returnObjects(List<N> nodes) {
    MNodePartition<N> partition = getPartition();
    for (N node : nodes) {
      partition.returnObject(node);
    }
  }
}
