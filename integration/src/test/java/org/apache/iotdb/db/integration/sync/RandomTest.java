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
package org.apache.iotdb.db.integration.sync;

import org.apache.iotdb.jdbc.Config;

import org.apache.commons.lang.RandomStringUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class RandomTest {

  private static int time = 0;
  private static final String[] sqls =
      new String[] {
        "SET STORAGE GROUP TO root.sg0",
        "CREATE TIMESERIES root.sg0.d0.s0 WITH DATATYPE=FLOAT, ENCODING=RLE",
        "CREATE TIMESERIES root.sg0.d0.s1 WITH DATATYPE=INT32, ENCODING=Gorilla",
        "CREATE TIMESERIES root.sg0.d0.s2 WITH DATATYPE=INT64",
        "CREATE TIMESERIES root.sg0.d0.s3 WITH DATATYPE=BOOLEAN",
        "CREATE TIMESERIES root.sg0.d0.s4 WITH DATATYPE=TEXT",
        "SET STORAGE GROUP TO root.sg1",
        "create aligned timeseries root.sg1.d1(s0 FLOAT encoding=RLE, s1 INT32 encoding=Gorilla compression=SNAPPY, s2 INT64, s3 BOOLEAN, s4 TEXT)",
      };

  private static List<String> generate() {
    Random rd = new Random();
    List<String> res = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      int sg = rd.nextInt(2);
      int s = rd.nextInt(5);
      String value = "";
      switch (s) {
        case 0:
          value = String.valueOf(rd.nextFloat());
          break;
        case 1:
        case 2:
          value = String.valueOf(rd.nextInt());
          break;
        case 3:
          value = String.valueOf(rd.nextBoolean());
          break;
        case 4:
          value = "'" + RandomStringUtils.randomAlphanumeric(5) + "'";
          break;
      }
      String sql = String.format("insert into root.sg%d.d%d(time,s%d)", sg, sg, s);
      if (sg == 1) {
        sql += " aligned";
      }
      sql += String.format(" values(%d,%s)", time++, value);
      res.add(sql);
    }
    res.add("flush;");
    return res;
  }

  public static void insertData() throws ClassNotFoundException {
    Class.forName(Config.JDBC_DRIVER_NAME);
    try (Connection connection =
            DriverManager.getConnection(
                Config.IOTDB_URL_PREFIX + "127.0.0.1:6667/", "root", "root");
        Statement statement = connection.createStatement()) {
      // create aligned and non-aligned time series
      for (String sql : sqls) {
        statement.execute(sql);
      }
      for (int i = 0; i < 6000; i++) {
        for (String sql : generate()) {
          statement.execute(sql);
        }
      }

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void main(String[] args) throws ClassNotFoundException {
    insertData();
  }
}
