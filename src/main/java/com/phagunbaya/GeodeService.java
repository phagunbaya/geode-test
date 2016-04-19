package com.phagunbaya;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;

/**
 * Created by phagunbaya on 12/04/16.
 */

public class GeodeService {

  /**
   * Purpose is to read data from region
   *
   * @param region
   * @param limit
   * @return
   * @throws Exception
   */
  public String get(String region, Long limit) throws Exception {
    if(limit == null)
      limit = 100L;

    /*
     * Uses /v1/{region}{?limit} API to read data
     */
    String url = "http://localhost:8000/gemfire-api/v1/"+region+"?limit="+limit;

    URL obj = new URL(url);
    HttpURLConnection con = (HttpURLConnection) obj.openConnection();
    con.setRequestMethod("GET");
    con.setRequestProperty("Accept", "application/json");
    con.setRequestProperty("Content-Type", "application/json; charset=UTF-8");

    int responseCode = con.getResponseCode();
    //System.out.println("Response Code : " + responseCode);

    BufferedReader in = new BufferedReader(
        new InputStreamReader(con.getInputStream()));
    String inputLine;
    StringBuffer response = new StringBuffer();

    while ((inputLine = in.readLine()) != null) {
      response.append(inputLine);
    }
    in.close();

    if(responseCode == 200) {
      return response.toString();
    }
    else {
      System.out.println("Unable to read from database : ");
      System.out.println(response);
      throw new Exception("Unable to read from database");
    }
  }

  /**
   * Purpose is to store JSON stringified data in region
   *
   * @param region
   * @param id
   * @param data
   * @return
   * @throws Exception
   */

  public String store(String region, String id, String data) throws Exception {
    if(id == null) {
      id = ""+Math.random();
    }

    /*
     * Uses /v1/{region}?key=* API to store data against a corresponding key
     */

    String url = "http://localhost:8000/gemfire-api/v1/"+region+"?key="+id;
    URL obj = new URL(url);
    HttpURLConnection con = (HttpURLConnection) obj.openConnection();
    con.setRequestMethod("POST");
    con.setRequestProperty("Accept", "application/json");
    con.setRequestProperty("Content-Type", "application/json; charset=UTF-8");

    con.setDoOutput(true);
    DataOutputStream wr = new DataOutputStream(con.getOutputStream());
    wr.writeBytes(data);
    wr.flush();
    wr.close();

    int responseCode = con.getResponseCode();
    //System.out.println("Response Code : " + responseCode);

    BufferedReader in = new BufferedReader(
        new InputStreamReader(con.getInputStream()));
    String inputLine;
    StringBuffer response = new StringBuffer();

    while ((inputLine = in.readLine()) != null) {
      response.append(inputLine);
    }
    in.close();

    if(responseCode == 201) {
      return data;
    }
    else {
      System.out.println("Unable to store in database : ");
      System.out.println(response);
      throw new Exception("Unable to store in database");
    }
  }

  /**
   * Purpose is to perform adhoc queries on region
   * @param query
   * @return (JSON.stringified list)
   * @throws Exception
   */
  public String query(String query) throws Exception {

    /*
     * Uses /v1/queries/adhoc?q=* API to perform queries directly on region
     */

    String url = "http://localhost:8000/gemfire-api/v1/queries/adhoc?q="+query;

    URL obj = new URL(url);
    HttpURLConnection con = (HttpURLConnection) obj.openConnection();
    con.setRequestMethod("GET");
    con.setRequestProperty("Accept", "application/json");
    con.setRequestProperty("Content-Type", "application/json; charset=UTF-8");

    int responseCode = con.getResponseCode();
    //System.out.println("Response Code : " + responseCode);

    BufferedReader in = new BufferedReader(
        new InputStreamReader(con.getInputStream()));
    String inputLine;
    StringBuffer response = new StringBuffer();

    while ((inputLine = in.readLine()) != null) {
      response.append(inputLine);
    }
    in.close();

    if(responseCode == 200) {
      return response.toString();
    }
    else {
      System.out.println("Unable to query database : ");
      System.out.println(response);
      throw new Exception("Unable to query database");
    }
  }

  /**
   * Purpose of this function is to store the dataframe in geode of format as below :
   *
   * | time | thing  | signal1 | signal2 | signal3 |
   * -----------------------------------------------
   * | 1234 | thingA | 2.3     | 3.4     | label1  |
   * -----------------------------------------------
   * | 1235 | thingB | 3.667   | 4       | label2  |
   * -----------------------------------------------
   * | 1236 | thingA | 4.56    | 7.8     | label3  |
   * -----------------------------------------------
   *
   * NOTE : `time` column is of type Long
   *        `thing` column is of type String
   *        `signal1` column is of type Double
   *        `signal2` column is of type Double
   *        `signal3` column is of type String
   *
   *
   * @param sparkContext
   * @param region
   * @param data
   * @param partitionBy similar to secondary indexes. For test can assume `thing` column as partition id.
   * @throws Exception
   */

  public void storeDataframe(JavaSparkContext sparkContext, String region, DataFrame data, List<String> partitionBy) throws Exception {
    Row[] rows = data.cache().collect();
    String[] columns = data.columns();
    for (Row row : rows) {
      JSONObject json = new JSONObject();
      for (String column : columns) {
        json.append(column, row.getAs(column));
      }

      /*
       * Todo: Find out cleaner way to handle the storage
       * Storing each row in dataframe as single row in dataframe gives ability to perform search queries like :
       * 1. select * from /region where time > 1234 and time < 1987
       * 2. select * from /region where time > 1234 and time < 1987 and thing = 'thingA'
       */

      String id = ""+Math.random();
      String url = "http://localhost:8000/gemfire-api/v1/"+region+"?key="+id;
      URL obj = new URL(url);
      HttpURLConnection con = (HttpURLConnection) obj.openConnection();
      con.setRequestMethod("POST");
      con.setRequestProperty("Accept", "application/json");
      con.setRequestProperty("Content-Type", "application/json; charset=UTF-8");

      con.setDoOutput(true);
      DataOutputStream wr = new DataOutputStream(con.getOutputStream());
      wr.writeBytes(json.toString());
      wr.flush();
      wr.close();

      int responseCode = con.getResponseCode();
      if(responseCode != 201)
        throw new Exception("Unable to store data");
    }

    /*
     * Todo: Storing using spark connector
     * https://github.com/apache/incubator-geode/tree/develop/geode-spark-connector
     *
     * RDD<Row> rdd = data.rdd();
     * rdd.saveToGemfire(region);
     *
     */

  }

  /**
   * Purpose of this function is to read the geode region and return dataframe of format as below
   *
   * | time | thing  | signal1 | signal2 | signal3 |
   * -----------------------------------------------
   * | 1234 | thingA | 2.3     | 3.4     | label1  |
   * -----------------------------------------------
   * | 1235 | thingB | 3.667   | 4       | label2  |
   * -----------------------------------------------
   * | 1236 | thingA | 4.56    | 7.8     | label3  |
   * -----------------------------------------------
   *
   * NOTE : `time` column is of type Long
   *        `thing` column is of type String
   *        `signal1` column is of type Double
   *        `signal2` column is of type Double
   *        `signal3` column is of type String
   *
   * @param sparkContext
   * @param region
   * @param startTime [Optional]
   * @param endTime [Optional]
   * @return Dataframe
   * @throws Exception
   */

  public DataFrame readDataFrame(JavaSparkContext sparkContext, String region, Long startTime, Long endTime) throws Exception {

    /*
     * Todo : Use geode-spark-connector to read data from a region
     * https://github.com/apache/incubator-geode/tree/develop/geode-spark-connector
     *
     * Also, see if `time` filter can be pushed down to spark region read API.
     */

//    RDD rdd = sparkContext.gemfireRegion(region);
//    return rdd.toDF();
    return new SQLContext(sparkContext).emptyDataFrame();
  }
}
