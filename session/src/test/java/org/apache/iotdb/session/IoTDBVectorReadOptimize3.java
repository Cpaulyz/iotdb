package org.apache.iotdb.session;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionDataSetWrapper;
import org.apache.iotdb.session.pool.SessionPool;

import java.io.File;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class IoTDBVectorReadOptimize3 {

    private static final String ROOT_SG1_D1_VECTOR1 = "root.sg_2.d1.vector";
    private static int rowNum = 1000000;
    private static int colNum = 1000;
    private static int startTest = 10;
    private static int threadCount = 4;
    private static List<Session> sessionList;

    public static void main(String[] args)
            throws StatementExecutionException, IoTDBConnectionException, InterruptedException {
        sessionList = new ArrayList<>();
        for (int i = 0; i < threadCount; i++) {
            Session session = new Session("192.168.130.37", 6667, "root", "root");
            session.open();
            sessionList.add(session);
        }
        for (int i = 0; i < threadCount; i++) {
            int finalI = i;
            new Thread(new Runnable() {
                public void run() {
                    randomCacheTest(finalI);
                }
            }).start();
        }
    }

    public static void randomCacheTest(int i) {
        try {
            ArrayList<ArrayList<String>> csvData = new ArrayList<>();
            ArrayList<String> header = new ArrayList<>();
            header.add("columnNum");
            header.add("cache");
            //        header.add("noncache");
            csvData.add(header);
            Random random = new Random();
            for (int time = 0; time < 1000; time++) {
                int col = random.nextInt(1000) + 1;
                String sql = String.format("select s" + getIndexString(1, colNum));
                for (int j = 2; j <= col; j++) {
                    sql += String.format(", s%s", getIndexString(j, colNum));
                }
                sql += " from " + ROOT_SG1_D1_VECTOR1;
                csvData.add(testSelectTime(col, sql, i));
            }
            //        saveToFile("all_evict_cached_" + i + ".csv", csvData);
            //        saveToFile("all_evict_noncached_" + i + ".csv", csvData);
            saveToFile("random_cached" + i + ".csv", csvData);
            //                saveToFile("all_noncached_" + i + ".csv", csvData);
            sessionList.get(i).close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static void saveToFile(String fileName, ArrayList<ArrayList<String>> csvData) {
        File csvOutputFile = new File(fileName);
        try (PrintWriter pw = new PrintWriter(csvOutputFile)) {
            for (ArrayList<String> line : csvData) {
                pw.println(line.stream().collect(Collectors.joining(",")));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    private static ArrayList<String> testSelectTime(int colNum, String sql, int threadId) {
        ArrayList<String> line = new ArrayList<>();
        try {
            line.add(colNum + "");
            long startTime = System.currentTimeMillis();
            selectTest(sql, threadId);
            long endTime = System.currentTimeMillis();
            line.add(String.valueOf((endTime - startTime)));
            System.out.println(threadId+" : "+ "====================" +colNum + "======================");
            System.out.println(threadId+" : time: " + (endTime - startTime));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return line;
    }

    private static void selectTest(String sql, int threadId)
            throws StatementExecutionException, IoTDBConnectionException {
        SessionDataSet dataSet = sessionList.get(threadId).executeQueryStatement(sql);
        //    System.out.println(dataSet.getColumnNames());
        assert (dataSet.getColumnNames().size() > 1);
        dataSet.closeOperationHandle();
    }


    private static String getIndexString(int index, int total) {
        StringBuilder sb = new StringBuilder(String.valueOf(index));
        String totalStr = String.valueOf(total);
        while (sb.length() < totalStr.length()) {
            sb.insert(0, "0");
        }
        return sb.toString();
    }
}
