/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.confignode.service.thrift;

import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.service.AbstractThriftServiceThread;
import org.apache.iotdb.commons.service.metric.enums.Metric;
import org.apache.iotdb.commons.service.metric.enums.Tag;
import org.apache.iotdb.metrics.AbstractMetricService;
import org.apache.iotdb.metrics.metricsets.IMetricSet;
import org.apache.iotdb.metrics.utils.MetricLevel;
import org.apache.iotdb.metrics.utils.MetricType;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.Objects;

public class ConfigNodeRPCServiceMetrics implements IMetricSet {
  private AbstractThriftServiceThread thriftServiceThread;

  public ConfigNodeRPCServiceMetrics(AbstractThriftServiceThread thriftServiceThread) {
    this.thriftServiceThread = thriftServiceThread;
  }

  @Override
  public void bindTo(AbstractMetricService metricService) {
    metricService.createAutoGauge(
        Metric.THRIFT_ACTIVE_THREADS.toString(),
        MetricLevel.IMPORTANT,
        thriftServiceThread,
        AbstractThriftServiceThread::getActiveThreadCount,
        Tag.NAME.toString(),
        ThreadName.CONFIGNODE_RPC_SERVICE.getName());
    ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
    metricService.createAutoGauge(
        "confignode_rpc_processor_threads_live_threads",
        MetricLevel.IMPORTANT,
        threadBean,
        ConfigNodeRPCServiceMetrics::getAllThreadStateCount);
    try {
      threadBean.getAllThreadIds();
      for (Thread.State state : Thread.State.values()) {
        metricService.createAutoGauge(
            "confignode_rpc_processor_threads_states_threads",
            MetricLevel.IMPORTANT,
            threadBean,
            (bean) -> getThreadStateCount(bean, state),
            "state",
            getStateTagValue(state));
      }
    } catch (Error error) {
      // An error will be thrown for unsupported operations
      // e.g. SubstrateVM does not support getAllThreadIds
    }
  }

  @Override
  public void unbindFrom(AbstractMetricService metricService) {
    metricService.remove(
        MetricType.AUTO_GAUGE,
        Metric.THRIFT_ACTIVE_THREADS.toString(),
        Tag.NAME.toString(),
        ThreadName.CONFIGNODE_RPC_SERVICE.getName());
    ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();

    metricService.remove(MetricType.AUTO_GAUGE, "confignode_rpc_processor_threads_live_threads");

    try {
      threadBean.getAllThreadIds();
      for (Thread.State state : Thread.State.values()) {
        metricService.remove(
            MetricType.AUTO_GAUGE,
            "confignode_rpc_processor_threads_states_threads",
            "state",
            getStateTagValue(state));
      }
    } catch (Error error) {
      // An error will be thrown for unsupported operations
      // e.g. SubstrateVM does not support getAllThreadIds
    }
  }

  // VisibleForTesting
  static long getThreadStateCount(ThreadMXBean threadBean, Thread.State state) {
    return Arrays.stream(threadBean.getThreadInfo(threadBean.getAllThreadIds()))
        .filter(
            threadInfo ->
                threadInfo.getThreadName().contains(ThreadName.CONFIGNODE_RPC_PROCESSOR.getName()))
        .filter(threadInfo -> threadInfo != null && threadInfo.getThreadState() == state)
        .count();
  }

  // VisibleForTesting
  static long getAllThreadStateCount(ThreadMXBean threadBean) {
    return Arrays.stream(threadBean.getThreadInfo(threadBean.getAllThreadIds()))
        .filter(
            threadInfo ->
                threadInfo.getThreadName().contains(ThreadName.CONFIGNODE_RPC_PROCESSOR.getName()))
        .count();
  }

  private static String getStateTagValue(Thread.State state) {
    return state.name().toLowerCase().replace("_", "-");
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    ConfigNodeRPCServiceMetrics that = (ConfigNodeRPCServiceMetrics) o;
    return Objects.equals(thriftServiceThread, that.thriftServiceThread);
  }

  @Override
  public int hashCode() {
    return Objects.hash(thriftServiceThread);
  }
}
