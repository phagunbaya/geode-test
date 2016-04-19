package com.phagunbaya;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by phagunbaya on 09/04/16.
 */
public class TestTimeseriesRegion {
  GeodeService geodeService = new GeodeService();
  JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf()
      .setMaster("local[*,4]")
      .setAppName("TestSpark")
  );
  SQLContext sqlContext = new SQLContext(sparkContext);
  DataFrame raw = null;


  @Before
  public void setUp() throws Exception {
    // creating raw dataframe from csv file
    raw = sqlContext.read()
        .format("com.databricks.spark.csv")
        .option("header", "true")
        .option("parserLib", "univocity")
        .option("ignoreLeadingWhiteSpace", "true")
        .option("ignoreTrailingWhiteSpace", "true")
        .load(this.getClass().getResource("input_example.csv").getPath());
  }

  /*
   * Purpose is to query timeseries data with time range filter
   *
   * The query handler should be able to work over large dataset with minimum latency and processing time.
   */

  @Test
  public void timeRangeQuery() throws Exception {
    //storing data frame
    geodeService.storeDataframe(sparkContext, "timeseries", raw, new ArrayList<String>());

    /*
     * read data frame and perform time range query
     */

    //Approach 1, if we cannot push down time range filter to geode

    Long from = null;
    Long to = null;
    DataFrame df1 = geodeService.readDataFrame(sparkContext, "timeseries", null, null);
    DataFrame result = null;
    if(from == null && to == null)
      result = df1.sqlContext().sql("select * from /timeseries");
    else if(from == null && to != null)
      result = df1.sqlContext().sql("select * from /timeseries where time < "+to);
    else if(from != null && to == null)
      result = df1.sqlContext().sql("select * from /timeseries where time > "+from);
    else
      result = df1.sqlContext().sql("select * from /timeseries where time > "+from+" and time < "+to);

    //Approach 2: if we can push down time range filter to geode

    DataFrame df2 = geodeService.readDataFrame(sparkContext, "timeseries", from, to);

    Assert.assertEquals(result, df2);
    Assert.assertEquals(result.count(), df2.count());

  }

  /*
   * Purpose is to query timeseries data with time range and thing field
   */
  @Test
  public void timeRangeWithCustomQuery() throws Exception {
    //storing data frame
    geodeService.storeDataframe(sparkContext, "timeseries", raw, new ArrayList<String>());

    /*
     * read data frame and perform time range as well as filter on some column value
     */

    //Approach 1: if we cannot push down filter to geode

    Long from = null;
    Long to = null;
    DataFrame df1 = geodeService.readDataFrame(sparkContext, "timeseries", null, null);
    DataFrame result1 = null;
    if(from == null && to == null)
      result1 = df1.sqlContext().sql("select * from /timeseries where thing='thing2'");
    else if(from == null && to != null)
      result1 = df1.sqlContext().sql("select * from /timeseries where time < "+to+" and thing='thing2'");
    else if(from != null && to == null)
      result1 = df1.sqlContext().sql("select * from /timeseries where time > "+from+" and thing='thing2'");
    else
      result1 = df1.sqlContext().sql("select * from /timeseries where time > "+from+" and time < "+to+" and thing='thing2'");

    //Approach 2: if we can push down time range filter to geode

    DataFrame df2 = geodeService.readDataFrame(sparkContext, "timeseries", from, to);
    DataFrame result2 = df2.sqlContext().sql("select * from /timeseries where thing='thing2'");

    Assert.assertEquals(result1, result2);
    Assert.assertEquals(result1.count(), result2.count());
  }

}
