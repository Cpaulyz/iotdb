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

package org.apache.iotdb.db.queryengine.plan.statement.metadata;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.queryengine.plan.statement.component.OrderByComponent;
import org.apache.iotdb.db.queryengine.plan.statement.component.WhereCondition;

import java.util.Collections;
import java.util.List;

/**
 * SHOW DEVICES statement.
 *
 * <p>Here is the syntax definition:
 *
 * <p>SHOW DEVICES [pathPattern] [WITH DATABASE] [LIMIT limit] [OFFSET offset]
 */
public class ShowDevicesStatement extends ShowStatement {

  private final PartialPath pathPattern;
  private boolean hasSgCol;
  private SchemaFilter schemaFilter;
  private WhereCondition timeCondition;
  private OrderByComponent orderByComponent = null;

  public ShowDevicesStatement(PartialPath pathPattern) {
    super();
    this.pathPattern = pathPattern;
  }

  public SchemaFilter getSchemaFilter() {
    return schemaFilter;
  }

  public void setSchemaFilter(SchemaFilter schemaFilter) {
    this.schemaFilter = schemaFilter;
  }

  public PartialPath getPathPattern() {
    return pathPattern;
  }

  public void setSgCol(boolean hasSgCol) {
    this.hasSgCol = hasSgCol;
  }

  public boolean hasSgCol() {
    return hasSgCol;
  }

  public void setTimeCondition(WhereCondition timeCondition) {
    this.timeCondition = timeCondition;
  }

  public WhereCondition getTimeCondition() {
    return timeCondition;
  }

  public boolean hasTimeCondition() {
    return timeCondition != null;
  }

  public void setOrderByComponent(OrderByComponent orderByComponent) {
    this.orderByComponent = orderByComponent;
  }

  public OrderByComponent getOrderByComponent() {
    return orderByComponent;
  }

  public boolean hasOrderByComponent() {
    return orderByComponent != null;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitShowDevices(this, context);
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.singletonList(pathPattern);
  }
}
