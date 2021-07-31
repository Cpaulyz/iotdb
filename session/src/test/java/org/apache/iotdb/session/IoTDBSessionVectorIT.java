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

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/**
 * use session interface to IT for vector timeseries insert and select Black-box Testing
 */
public class IoTDBSessionVectorIT {
    private static final String ROOT_SG1_D1_VECTOR1 = "root.sg_1.d1.vector";

    private Session session;

    @Before
    public void setUp() throws Exception {
        System.setProperty(IoTDBConstant.IOTDB_CONF, "src/test/resources/");
        EnvironmentUtils.closeStatMonitor();
        EnvironmentUtils.envSetUp();
        session = new Session("127.0.0.1", 6667, "root", "root");
        session.open();
    }

    @After
    public void tearDown() throws Exception {
        session.close();
        EnvironmentUtils.cleanEnv();
    }

    @Test
    public void badCacheTest() {
        int rowNum = 10000;
        String sql = "select * from root.sg_1.d1.vector.s0";
        try {
            ArrayList<ArrayList<String>> csvData = new ArrayList<>();
            ArrayList<String> header = new ArrayList<>();
            header.add("columnNum");
            header.add("non-cache");
            header.add("cache");
            header.add("diff");
            csvData.add(header);

            insertTabletWithAlignedTimeseriesMethod(rowNum, 1);
            session.executeNonQueryStatement("flush");
            selectTest("select * from root.sg_1.d1");
            session.deleteStorageGroup("root.sg_1");

            csvData.add(testSelectTime(rowNum,1,sql));
            for (int col = 50; col <= 1000; col += 50) {
                String tmpSql = sql+",root.sg_1.d1.vector.s"+(col-1);
               csvData.add(testSelectTime(rowNum,col,tmpSql));
            }
            saveToFile("bad_cache.csv",csvData);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void allCacheTest() {
        int rowNum = 10000;
        String sql = "select * from root.sg_1.d1";
        try {
            ArrayList<ArrayList<String>> csvData = new ArrayList<>();
            ArrayList<String> header = new ArrayList<>();
            header.add("columnNum");
            header.add("non-cache");
            header.add("cache");
            header.add("diff");
            csvData.add(header);

            insertTabletWithAlignedTimeseriesMethod(rowNum, 1);
            session.executeNonQueryStatement("flush");
            selectTest("select * from root.sg_1.d1");
            session.deleteStorageGroup("root.sg_1");

            csvData.add(testSelectTime(rowNum,1,sql));
            for (int col = 50; col <= 500; col += 50) {
               csvData.add(testSelectTime(rowNum,col,sql));
            }
            saveToFile("all_cache.csv",csvData);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private ArrayList<String> testSelectTime(int rowNum,int colNum,String sql) throws StatementExecutionException, IoTDBConnectionException {
        ArrayList<String> line = new ArrayList<>();
        line.add(colNum+"");
        System.out.println("====================" + colNum + "======================");
        insertTabletWithAlignedTimeseriesMethod(rowNum, colNum);
        session.executeNonQueryStatement("flush");
        ChunkCache.CACHE_VECTOR_ENABLE = false;
        long startTime = System.currentTimeMillis();
        selectTest(sql);
        long endTime = System.currentTimeMillis();
        line.add(String.valueOf((endTime - startTime)));
        System.out.println("no cache time: " + (endTime - startTime));
        ChunkCache.CACHE_VECTOR_ENABLE = true;
        startTime = System.currentTimeMillis();
        selectTest(sql);
        endTime = System.currentTimeMillis();
        line.add(String.valueOf((endTime - startTime)));
        System.out.println("cache time: " + (endTime - startTime));
        line.add(String.valueOf(Integer.parseInt(line.get(2))-Integer.parseInt(line.get(1))));
        session.deleteStorageGroup("root.sg_1");
        return line;
    }

    private void saveToFile(String fileName,ArrayList<ArrayList<String>> csvData){
        File csvOutputFile = new File(fileName);
        try (PrintWriter pw = new PrintWriter(csvOutputFile)) {
            for(ArrayList<String> line:csvData){
                pw.println(line.stream().collect(Collectors.joining(",")));
            }
        }catch (Exception e){
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