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

public class MemMNodeFactory extends MNodeFactory {
  @Override
  public EntityMNode createEntityMNode(IMNode parent, String name) {
    return new EntityMNode(parent, name);
  }

  @Override
  public MeasurementMNode createMeasurementMNode(
      IMNode parent, String name, IMeasurementSchema schema, String alias) {
    return new MeasurementMNode(parent, name, schema, alias);
  }

  @Override
  public InternalMNode createInternalMNode(IMNode parent, String name) {
    return new InternalMNode(parent, name);
  }
}
