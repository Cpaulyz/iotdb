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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

/** use session interface to IT for vector timeseries insert and select Black-box Testing */
public class IoTDBVectorReadOptimize2 {
  private static final String ROOT_SG1_D1_VECTOR1 = "root.sg_2.d1.vector";

  private Session session;

  private static int rowNum = 1000000;
  private static int colNum = 1000;
  private static int startTest = 10;

  @Before
  public void setUp() throws Exception {
    //    System.setProperty(IoTDBConstant.IOTDB_CONF, "src/test/resources/");
    //    EnvironmentUtils.closeStatMonitor();
    //    EnvironmentUtils.envSetUp();
        session = new Session("127.0.0.1", 6667, "root", "root");
//    session = new Session("192.168.130.37", 6667, "root", "root");
    session.open();
  }

  @After
  public void tearDown() throws Exception {
    //        session.deleteStorageGroup("root.sg_1");
    session.close();
    //            EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testResult() throws StatementExecutionException, IoTDBConnectionException {
    SessionDataSet dataSet = session.executeQueryStatement("select s0001, s0002, s0003, s0004, s0005, s0006, s0007, s0008, s0009, s0010, s0011, s0012, s0013, s0014, s0015, s0016, s0017, s0018, s0019, s0020, s0021, s0022, s0023, s0024, s0025, s0026, s0027, s0028, s0029, s0030, s0031, s0032, s0033, s0034, s0035, s0036, s0037, s0038, s0039, s0040, s0041, s0042, s0043, s0044, s0045, s0046, s0047, s0048, s0049, s0050, s0051, s0052, s0053, s0054, s0055, s0056, s0057, s0058, s0059, s0060, s0061, s0062, s0063, s0064, s0065, s0066, s0067, s0068, s0069, s0070, s0071, s0072, s0073, s0074, s0075, s0076, s0077, s0078, s0079, s0080, s0081, s0082, s0083, s0084, s0085, s0086, s0087, s0088, s0089, s0090, s0091, s0092, s0093, s0094, s0095, s0096, s0097, s0098, s0099, s0100, s0101, s0102, s0103, s0104, s0105, s0106, s0107, s0108, s0109, s0110, s0111, s0112, s0113, s0114, s0115, s0116, s0117, s0118, s0119, s0120, s0121, s0122, s0123, s0124, s0125, s0126, s0127, s0128, s0129, s0130, s0131, s0132, s0133, s0134, s0135, s0136, s0137, s0138, s0139, s0140, s0141, s0142, s0143, s0144, s0145, s0146, s0147, s0148, s0149, s0150, s0151, s0152, s0153, s0154, s0155, s0156, s0157, s0158, s0159, s0160, s0161, s0162, s0163, s0164, s0165, s0166, s0167, s0168, s0169, s0170, s0171, s0172, s0173, s0174, s0175, s0176, s0177, s0178, s0179, s0180, s0181, s0182, s0183, s0184, s0185, s0186, s0187, s0188, s0189, s0190, s0191, s0192, s0193, s0194, s0195, s0196, s0197, s0198, s0199, s0200, s0201, s0202, s0203, s0204, s0205, s0206, s0207, s0208, s0209, s0210, s0211, s0212, s0213, s0214, s0215, s0216, s0217, s0218, s0219, s0220, s0221, s0222, s0223, s0224, s0225, s0226, s0227, s0228, s0229, s0230, s0231, s0232, s0233, s0234, s0235, s0236, s0237, s0238, s0239, s0240, s0241, s0242, s0243, s0244, s0245, s0246, s0247, s0248, s0249, s0250, s0251, s0252, s0253, s0254, s0255, s0256, s0257, s0258, s0259, s0260, s0261, s0262, s0263, s0264, s0265, s0266, s0267, s0268, s0269, s0270, s0271, s0272, s0273, s0274, s0275, s0276, s0277, s0278, s0279, s0280, s0281, s0282, s0283, s0284, s0285, s0286, s0287, s0288, s0289, s0290, s0291, s0292, s0293, s0294, s0295, s0296, s0297, s0298, s0299, s0300, s0301, s0302, s0303, s0304, s0305, s0306, s0307, s0308, s0309, s0310, s0311, s0312, s0313, s0314, s0315, s0316, s0317, s0318, s0319, s0320, s0321, s0322, s0323, s0324, s0325, s0326, s0327, s0328, s0329, s0330, s0331, s0332, s0333, s0334, s0335, s0336, s0337, s0338, s0339, s0340, s0341, s0342, s0343, s0344, s0345, s0346, s0347, s0348, s0349, s0350, s0351, s0352, s0353, s0354, s0355, s0356, s0357, s0358, s0359, s0360, s0361, s0362, s0363, s0364, s0365, s0366, s0367, s0368, s0369, s0370, s0371, s0372, s0373, s0374, s0375, s0376, s0377, s0378, s0379, s0380, s0381, s0382, s0383, s0384, s0385, s0386, s0387, s0388, s0389, s0390, s0391, s0392, s0393, s0394, s0395, s0396, s0397, s0398, s0399, s0400, s0401, s0402, s0403, s0404, s0405, s0406, s0407, s0408, s0409, s0410, s0411, s0412, s0413, s0414, s0415, s0416, s0417, s0418, s0419, s0420, s0421, s0422, s0423, s0424, s0425, s0426, s0427, s0428, s0429, s0430, s0431, s0432, s0433, s0434, s0435, s0436, s0437, s0438, s0439, s0440, s0441, s0442, s0443, s0444, s0445, s0446, s0447, s0448, s0449, s0450, s0451, s0452, s0453, s0454, s0455, s0456, s0457, s0458, s0459, s0460, s0461, s0462, s0463, s0464, s0465, s0466, s0467, s0468, s0469, s0470, s0471, s0472, s0473, s0474, s0475, s0476, s0477, s0478, s0479, s0480, s0481, s0482, s0483, s0484, s0485, s0486, s0487, s0488, s0489, s0490, s0491, s0492, s0493, s0494, s0495, s0496, s0497, s0498, s0499, s0500, s0501, s0502, s0503, s0504, s0505, s0506, s0507, s0508, s0509, s0510, s0511, s0512, s0513, s0514, s0515, s0516, s0517, s0518, s0519, s0520, s0521, s0522, s0523, s0524, s0525, s0526, s0527, s0528, s0529, s0530, s0531, s0532, s0533, s0534, s0535, s0536, s0537, s0538, s0539, s0540, s0541, s0542, s0543, s0544, s0545, s0546, s0547, s0548, s0549, s0550, s0551, s0552, s0553, s0554, s0555, s0556, s0557, s0558, s0559, s0560, s0561, s0562, s0563, s0564, s0565, s0566, s0567, s0568, s0569, s0570, s0571, s0572, s0573, s0574, s0575, s0576, s0577, s0578, s0579, s0580, s0581, s0582, s0583, s0584, s0585, s0586, s0587, s0588, s0589, s0590, s0591, s0592, s0593, s0594, s0595, s0596, s0597, s0598, s0599, s0600, s0601, s0602, s0603, s0604, s0605, s0606, s0607, s0608, s0609, s0610, s0611, s0612, s0613, s0614, s0615, s0616, s0617, s0618, s0619, s0620, s0621, s0622, s0623, s0624, s0625, s0626, s0627, s0628, s0629, s0630, s0631, s0632, s0633, s0634, s0635, s0636, s0637, s0638, s0639, s0640, s0641, s0642, s0643, s0644, s0645, s0646, s0647, s0648, s0649, s0650, s0651, s0652, s0653, s0654, s0655, s0656, s0657, s0658, s0659, s0660, s0661, s0662, s0663, s0664, s0665, s0666, s0667, s0668, s0669, s0670, s0671, s0672, s0673, s0674, s0675, s0676, s0677, s0678, s0679, s0680, s0681, s0682, s0683, s0684, s0685, s0686, s0687, s0688, s0689, s0690, s0691, s0692, s0693, s0694, s0695, s0696, s0697, s0698, s0699, s0700, s0701, s0702, s0703, s0704, s0705, s0706, s0707, s0708, s0709, s0710, s0711, s0712, s0713, s0714, s0715, s0716, s0717, s0718, s0719, s0720, s0721, s0722, s0723, s0724, s0725, s0726, s0727, s0728, s0729, s0730, s0731, s0732, s0733, s0734, s0735, s0736, s0737, s0738, s0739, s0740, s0741, s0742, s0743, s0744, s0745, s0746, s0747, s0748, s0749, s0750, s0751, s0752, s0753, s0754, s0755, s0756, s0757, s0758, s0759, s0760, s0761, s0762, s0763, s0764, s0765, s0766, s0767, s0768, s0769, s0770 from root.sg_2.d1.vector");
    dataSet.closeOperationHandle();
  }

  @Test
  public void createTimeSeries() throws StatementExecutionException, IoTDBConnectionException {
    System.out.println("insert" + colNum);
    insertTabletWithAlignedTimeseriesMethod(rowNum, colNum);
    session.executeNonQueryStatement("flush");
  }

  @Test
  public void badCacheTest() {
    int time = 4;
    try {
      for (int i = 0; i < time; i++) {
        ArrayList<ArrayList<String>> csvData = new ArrayList<>();
        ArrayList<String> header = new ArrayList<>();
        header.add("columnNum");
        header.add("cache");
        header.add("non-cache");
        header.add("diff");
        csvData.add(header);
        for (int col = 2; col <= colNum; col++) {
          System.out.println("====================" + col + "======================");
          String sql = String.format("select s" + getIndexString(1, colNum));
          sql += String.format(", s%s", getIndexString(col, colNum));
          sql += " from " + ROOT_SG1_D1_VECTOR1;
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
    int time = 4;
    try {
      for (int i = 0; i < time; i++) {
        ArrayList<ArrayList<String>> csvData = new ArrayList<>();
        ArrayList<String> header = new ArrayList<>();
        header.add("columnNum");
//        header.add("cache");
        header.add("noncache");
        csvData.add(header);
        for (int col = startTest; col <= colNum; col+=10) {
          System.out.println("====================" + col + "======================");
          String sql = String.format("select s" + getIndexString(1, colNum));
          for (int j = 2; j <= col; j++) {
            sql += String.format(", s%s", getIndexString(j, colNum));
          }
          sql += " from " + ROOT_SG1_D1_VECTOR1;
          csvData.add(testSelectTime(col, sql));
        }
//        saveToFile("all_cached_" + i + ".csv", csvData);
        saveToFile("all_noncached_" + i + ".csv", csvData);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private ArrayList<String> testSelectTime(int colNum, String sql) {
    ArrayList<String> line = new ArrayList<>();
    try {
      line.add(colNum + "");
      long startTime = System.currentTimeMillis();
      selectTest(sql);
      long endTime = System.currentTimeMillis();
      line.add(String.valueOf((endTime - startTime)));
      System.out.println("time: " + (endTime - startTime));
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
    //    System.out.println(dataSet.getColumnNames());
    assert (dataSet.getColumnNames().size() > 1);
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
