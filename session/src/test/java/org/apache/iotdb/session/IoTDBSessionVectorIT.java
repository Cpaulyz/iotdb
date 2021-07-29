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
package org.apache.iotdb.session;

import org.apache.iotdb.db.conf.IoTDBConstant;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * use session interface to IT for vector timeseries insert and select Black-box Testing
 */
public class IoTDBSessionVectorIT {
    private static final String ROOT_SG1_D1_VECTOR1 = "root.sg_1.d1.vector";

    private Session session;

    //    @Before
    public void setUp() throws Exception {
        System.setProperty(IoTDBConstant.IOTDB_CONF, "src/test/resources/");
        EnvironmentUtils.closeStatMonitor();
        TSFileDescriptor.getInstance().getConfig().setMaxDegreeOfIndexNode(3);
        ChunkCache.CACHE_VECTOR_ENABLE = true;
        EnvironmentUtils.envSetUp();
        session = new Session("127.0.0.1", 6667, "root", "root");
        session.open();
    }

    //    @After
    public void tearDown() throws Exception {
        session.close();
        EnvironmentUtils.cleanEnv();
    }

    @Test
    public void alignedTest() {
        try {
            setUp();
//            for (int row = 10; row < 10000; row+=10) {
//                for (int col = 10; col < 10000; col+=10) {
            int col = 10;
            System.out.println("====================" + col + "======================");
            insertTabletWithAlignedTimeseriesMethod(10, col);
            session.executeNonQueryStatement("flush");
//                    ChunkCache.getInstance().clear();
            selectTest("select * from root.sg_1.d1");
            session.deleteStorageGroup("root.sg_1");
//                    ChunkCache.getInstance().clear();
//                    ChunkCache.CACHE_VECTOR_ENABLE = false;
//                    selectTest("select * from root.sg_1.d1");
//                }
//            }
            session.deleteStorageGroup("root.sg_1");
            tearDown();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private SessionDataSet selectTest(String sql)
            throws StatementExecutionException, IoTDBConnectionException {
        SessionDataSet dataSet = session.executeQueryStatement(sql);
        dataSet.closeOperationHandle();
        return dataSet;
    }

    /**
     * Method 1 for insert tablet with aligned timeseries
     */
    private void insertTabletWithAlignedTimeseriesMethod(int rowNum, int columnNum)
            throws IoTDBConnectionException, StatementExecutionException {
        // The schema of measurements of one device
        // only measurementId and data type in MeasurementSchema take effects in Tablet
        List<IMeasurementSchema> schemaList = new ArrayList<>();
        String[] measurements = new String[columnNum];
        TSDataType[] dataTypes = new TSDataType[columnNum];
        for (int i = 0; i < columnNum; i++) {
            measurements[i] = "s" + i;
            dataTypes[i] = TSDataType.INT64;
        }
        schemaList.add(
                new VectorMeasurementSchema(
                        "vector",
                        measurements,
                        dataTypes));

        Tablet tablet = new Tablet(ROOT_SG1_D1_VECTOR1, schemaList);
        tablet.setAligned(true);
        long timestamp = 0;

        for (long row = 0; row < rowNum; row++) {
            int rowIndex = tablet.rowSize++;
            tablet.addTimestamp(rowIndex, timestamp);
            for (int i = 0; i < columnNum; i++) {
                tablet.addValue(
                        schemaList.get(0).getValueMeasurementIdList().get(i), rowIndex, row * 10L + i);
            }
            if (tablet.rowSize == tablet.getMaxRowNumber()) {
                session.insertTablet(tablet, true);
                tablet.reset();
            }
            timestamp++;
        }

        if (tablet.rowSize != 0) {
            session.insertTablet(tablet);
            tablet.reset();
        }
    }
}