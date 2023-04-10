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

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.function.Supplier;

public class MNodePartition<N extends IMNode> {
  private final BlockingQueue<N> queue;
  private final int maxSize;
  private final Supplier<N> creator;

  public MNodePartition(int maxSize, Supplier<N> creator) {
    this.queue = new ArrayBlockingQueue<>(maxSize);
    this.maxSize = maxSize;
    this.creator = creator;
  }

  public N borrowObject() {
    N node = queue.poll();
    if (node == null) {
      node = creator.get();
    }
    return node;
  }

  public synchronized int increaseObjects(int delta) {
    int cnt;
    for (cnt = 0; cnt < delta; cnt++) {
      if (!queue.offer(creator.get())) {
        break;
      }
    }
    return cnt;
  }

  public boolean returnObject(N node) {
    return queue.offer(node);
  }
}
