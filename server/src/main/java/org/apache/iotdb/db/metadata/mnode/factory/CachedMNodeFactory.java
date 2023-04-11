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

import org.apache.iotdb.db.metadata.mnode.EntityMNode;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.InternalMNode;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

public class CachedMNodeFactory extends MNodeFactory {

  private final MNodePool<MeasurementMNode> measurementMNodePool;
  private final MNodePool<EntityMNode> entityMNodePool;
  private final MNodePool<InternalMNode> internalMNodePool;

  public CachedMNodeFactory() {

    measurementMNodePool = new MNodePool<>(MeasurementMNode::new, 1000, 7);
    entityMNodePool = new MNodePool<>(EntityMNode::new, 100, 7);
    internalMNodePool = new MNodePool<>(InternalMNode::new, 100, 7);
  }

  @Override
  public EntityMNode createEntityMNode(IMNode parent, String name) {
    try {
      EntityMNode node = entityMNodePool.borrowObject();
      node.init(parent, name);
      return node;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    //    return new EntityMNode(parent, name);
  }

  @Override
  public MeasurementMNode createMeasurementMNode(
      IMNode parent, String name, IMeasurementSchema schema, String alias) {
    try {
      MeasurementMNode node = measurementMNodePool.borrowObject();
      node.init(parent, name, schema, alias);
      return node;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    //        return new MeasurementMNode(parent, name, schema, alias);
  }

  @Override
  public InternalMNode createInternalMNode(IMNode parent, String name) {
    try {
      InternalMNode node = internalMNodePool.borrowObject();
      node.init(parent, name);
      return node;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    //    return new InternalMNode(parent, name);
  }

  public void returnMNode(IMNode node) {
    if (node instanceof EntityMNode) {
      EntityMNode entityMNode = (EntityMNode) node;
      entityMNode.clear();
      entityMNodePool.returnObject(entityMNode);
    } else if (node instanceof InternalMNode) {
      InternalMNode internalMNode = (InternalMNode) node;
      internalMNode.clear();
      internalMNodePool.returnObject(internalMNode);
    } else if (node instanceof MeasurementMNode) {
      MeasurementMNode measurementMNode = (MeasurementMNode) node;
      measurementMNode.clear();
      measurementMNodePool.returnObject(measurementMNode);
    }
  }
}
