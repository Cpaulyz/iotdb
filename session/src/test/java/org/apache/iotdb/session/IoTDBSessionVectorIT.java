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

import org.apache.iotdb.db.engine.cache.ChunkCache;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;

import org.junit.*;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/** use session interface to IT for vector timeseries insert and select Black-box Testing */
public class IoTDBSessionVectorIT {
  private static final String ROOT_SG1_D1_VECTOR1 = "root.sg_2.d1.vector";

  private Session session;

  private static int rowNum = 10000;
  private static int colNum = 100;
  private static int startTest = 1;

  @Before
  public void setUp() throws Exception {
    //    System.setProperty(IoTDBConstant.IOTDB_CONF, "src/test/resources/");
    //    EnvironmentUtils.closeStatMonitor();
    //    EnvironmentUtils.envSetUp();
    session = new Session("127.0.0.1", 6667, "root", "root");
    //    session = new Session("192.168.41.132", 6667, "root", "root");
    session.open();
  }

  @After
  public void tearDown() throws Exception {
    //        session.deleteStorageGroup("root.sg_1");
    session.close();
    //        EnvironmentUtils.cleanEnv();
  }

  @Test
  public void createTimeSeries() throws StatementExecutionException, IoTDBConnectionException {
    System.out.println("insert" + colNum);
    insertTabletWithAlignedTimeseriesMethod(rowNum, colNum);
    session.executeNonQueryStatement("flush");
  }

  @Test
  public void hybridCacheTest() {
    String sql = "select * from root.sg_1.d1.vector.s" + getIndexString(1, colNum);
    String allSql = "select * from root.sg_1.d1.vector.s" + getIndexString(1, colNum);
    int time = 1;
    try {
      for (int i = 0; i < time; i++) {
        ArrayList<ArrayList<String>> allCsvData = new ArrayList<>();
        ArrayList<ArrayList<String>> badCsvData = new ArrayList<>();
        ArrayList<String> header = new ArrayList<>();
        header.add("columnNum");
        header.add("non-cache");
        header.add("cache");
        header.add("diff");
        allCsvData.add(header);
        badCsvData.add(header);

        selectTest(sql);

        allCsvData.add(testSelectTime(1, sql));
        for (int col = 2; col <= colNum; col++) {
          allSql += ", root.sg_1.d1.vector.s" + getIndexString(col, colNum);
          allCsvData.add(testSelectTime(col, allSql));
          String badSql = sql + ", root.sg_1.d1.vector.s" + getIndexString(col, colNum);
          badCsvData.add(testSelectTime(col, badSql));
        }
        saveToFile("all_cache_" + i + ".csv", allCsvData);
        saveToFile("bad_cache_" + i + ".csv", badCsvData);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void badCacheTest() {
    //        String sql = String.format("select * from
    // %s.s%s",generatePath(colNum),getIndexString(1,colNum));
    int time = 1;
    try {
      for (int i = 0; i < time; i++) {
        ArrayList<ArrayList<String>> csvData = new ArrayList<>();
        ArrayList<String> header = new ArrayList<>();
        header.add("columnNum");
        header.add("non-cache");
        header.add("cache");
        header.add("diff");
        csvData.add(header);
        //                selectTest(sql);
        //                csvData.add(testSelectTime(1,sql));
        //                for (int col = 2; col <= colNum; col ++) {
        //                    String tmpSql = sql+",
        // root.sg_1.d1.vector.s"+getIndexString(col,colNum);
        //                    csvData.add(testSelectTime(col,tmpSql));
        //                }
        for (int col = 2; col <= colNum; col++) {
          System.out.println("====================" + col + "======================");
          String sql =
              String.format("select * from %s.s%s", ROOT_SG1_D1_VECTOR1, getIndexString(1, col));
          sql += String.format(", %s.s%s", ROOT_SG1_D1_VECTOR1, getIndexString(col, col));
          csvData.add(testSelectTime(col, sql));
        }
        saveToFile("bad_cache_" + i + ".csv", csvData);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void allCacheTest() {
    //        String sql = "select * from root.sg_1.d1.vector.s"+getIndexString(1,colNum);
    int time = 1;
    try {
      for (int i = 0; i < time; i++) {
        ArrayList<ArrayList<String>> csvData = new ArrayList<>();
        ArrayList<String> header = new ArrayList<>();
        header.add("columnNum");
        header.add("non-cache");
        header.add("cache");
        header.add("diff");
        csvData.add(header);
        //                selectTest(sql);
        //                csvData.add(testSelectTime(1, sql));
        for (int col = startTest; col <= colNum; col++) {
          System.out.println("====================" + col + "======================");
          String sql =
              String.format("select * from %s.s%s", ROOT_SG1_D1_VECTOR1, getIndexString(1, col));
          for (int j = 2; j <= col; j++) {
            sql += String.format(", %s.s%s", ROOT_SG1_D1_VECTOR1, getIndexString(j, col));
          }
          //                    int finalCol = col;
          //                    String finalSql = sql;
          //                    Thread t = new Thread(new Runnable(){
          //                        public void run(){
          //                            testSelectTime(finalCol, finalSql);
          //                        }});
          //                    t.start();
          csvData.add(testSelectTime(col, sql));
        }
        saveToFile("all_cache_" + i + ".csv", csvData);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private ArrayList<String> testSelectTime(int colNum, String sql) {
    ArrayList<String> line = new ArrayList<>();
    try {
      line.add(colNum + "");
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
      line.add(String.valueOf(Integer.parseInt(line.get(2)) - Integer.parseInt(line.get(1))));
    } catch (Exception e) {
      e.printStackTrace();
    }
    return line;
  }

  private void saveToFile(String fileName, ArrayList<ArrayList<String>> csvData) {
    File csvOutputFile = new File(fileName);
    try (PrintWriter pw = new PrintWriter(csvOutputFile)) {
      for (ArrayList<String> line : csvData) {
        pw.println(line.stream().collect(Collectors.joining(",")));
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private SessionDataSet selectTest(String sql)
      throws StatementExecutionException, IoTDBConnectionException {
    SessionDataSet dataSet = session.executeQueryStatement(sql);
    System.out.println(dataSet.getColumnNames());
    dataSet.closeOperationHandle();
    return dataSet;
  }

  /** Method 1 for insert tablet with aligned timeseries */
  private void insertTabletWithAlignedTimeseriesMethod(int rowNum, int columnNum)
      throws IoTDBConnectionException, StatementExecutionException {
    // The schema of measurements of one device
    // only measurementId and data type in MeasurementSchema take effects in Tablet
    List<IMeasurementSchema> schemaList = new ArrayList<>();
    String[] measurements = new String[columnNum];
    TSDataType[] dataTypes = new TSDataType[columnNum];
    for (int i = 0; i < columnNum; i++) {
      measurements[i] = "s" + getIndexString(i + 1, columnNum);
      dataTypes[i] = TSDataType.INT64;
    }
    schemaList.add(new VectorMeasurementSchema("vector", measurements, dataTypes));

    Tablet tablet = new Tablet(ROOT_SG1_D1_VECTOR1, schemaList);
    tablet.setAligned(true);
    long timestamp = 0;

    for (long row = 0; row < rowNum; row++) {
      int rowIndex = tablet.rowSize++;
      tablet.addTimestamp(rowIndex, timestamp);
      for (int i = 0; i < columnNum; i++) {
        tablet.addValue(schemaList.get(0).getSubMeasurementsList().get(i), rowIndex, row * 10L + i);
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

  private String getIndexString(int index, int total) {
    StringBuilder sb = new StringBuilder(String.valueOf(index));
    String totalStr = String.valueOf(total);
    while (sb.length() < totalStr.length()) {
      sb.insert(0, "0");
    }
    return sb.toString();
  }
}
