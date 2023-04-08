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

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

public class CachedMNodeFactory extends MNodeFactory {

  private final MNodePool<MeasurementMNode> measurementMNodePool;
  private final MNodePool<EntityMNode> entityMNodePool;
  private final MNodePool<InternalMNode> internalMNodePool;

  public CachedMNodeFactory() {
    //    int capacity =
    //        (int) (IoTDBDescriptor.getInstance().getConfig().getAllocateMemoryForSchemaRegion() *
    // 0.25);
    //    int measurementMNodeSize = new CachedMNodeSizeEstimator().getEmptyMeasurementNodeSize();
    //    int entityNodeSize = new CachedMNodeSizeEstimator().getEmptyEntityNodeSize();
    //    int internalNodeSize = new CachedMNodeSizeEstimator().getEmptyInternalNodeSize();
    //    int measurementProportion = 10000000;
    //    int entityProportion = 10000;
    //    int internalProportion = 1;

    int internalNum = 1;
    int entityNum = 1;
    int measurementNum = 10000;

    GenericObjectPoolConfig<MeasurementMNode> measurementConfig = new GenericObjectPoolConfig<>();
    measurementConfig.setMaxIdle((int) measurementNum);
    measurementConfig.setMinIdle(0);
    measurementConfig.setMaxTotal(Integer.MAX_VALUE);
    GenericObjectPoolConfig<EntityMNode> entityConfig = new GenericObjectPoolConfig<>();
    entityConfig.setMaxIdle((int) entityNum);
    entityConfig.setMinIdle(0);
    entityConfig.setMaxTotal(Integer.MAX_VALUE);
    GenericObjectPoolConfig<InternalMNode> internalConfig = new GenericObjectPoolConfig<>();
    internalConfig.setMaxIdle((int) internalNum);
    internalConfig.setMinIdle(0);
    internalConfig.setMaxTotal(Integer.MAX_VALUE);

    measurementMNodePool = new MNodePool<>(MeasurementMNode::new, measurementConfig);
    entityMNodePool = new MNodePool<>(EntityMNode::new, entityConfig);
    internalMNodePool = new MNodePool<>(InternalMNode::new, internalConfig);
  }

  @Override
  public EntityMNode createEntityMNode(IMNode parent, String name) {
    //    try {
    //      EntityMNode node = entityMNodePool.borrowObject();
    //      node.init(parent, name);
    //      return node;
    //    } catch (Exception e) {
    //      throw new RuntimeException(e);
    //    }
    return new EntityMNode(parent, name);
  }

  @Override
  public MeasurementMNode createMeasurementMNode(
      IMNode parent, String name, IMeasurementSchema schema, String alias) {
    //    try {
    //      MeasurementMNode node = measurementMNodePool.borrowObject();
    //      node.init(parent, name, schema, alias);
    //      return node;
    //    } catch (Exception e) {
    //      throw new RuntimeException(e);
    //    }
    return new MeasurementMNode(parent, name, schema, alias);
  }

  @Override
  public InternalMNode createInternalMNode(IMNode parent, String name) {
    //    try {
    //      InternalMNode node = internalMNodePool.borrowObject();
    //      node.init(parent, name);
    //      return node;
    //    } catch (Exception e) {
    //      throw new RuntimeException(e);
    //    }
    return new InternalMNode(parent, name);
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
