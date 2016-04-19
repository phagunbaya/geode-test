package com.phagunbaya;

import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

/**
 * Created by phagunbaya on 09/04/16.
 */
public class TestStreamEventBuffer {

  /*
   * In this test case we are streaming input file data and storing it as a raw in event buffer.
   */

  GeodeService geodeService = new GeodeService();

  @Before
  public void setUp() throws Exception {

  }

  /*
   * In this test case we are storing raw data in event buffer
   *
   */

  @Test
  public void storeDatainEB() throws Exception {

    /*
     * Currently we store data from each request as raw file in hdfs path
     * Need to find a way to handle this case with geode
     *
     * Requirement is to to store the raw data as it arrives and later all data should be parsable. Correctly, we support csv and json.
     *
     * Approach: Store raw data for each ingestion request in separate in kv format.
     * Then we need to manage the key of each kv pair.
     */

    BufferedReader reader1 = new BufferedReader(new FileReader(this.getClass().getResource("part1.csv").getFile()));
    //Todo: store reader1 in geode

    // need a way to store data stream in kv store
    // geodeService.store("raw", "EB-01-01", reader1);


    BufferedReader reader2 = new BufferedReader(new FileReader(this.getClass().getResource("part2.csv").getFile()));
    //Todo: append reader2 in geode

    // need a way to store data stream in kv store
    // geodeService.store("raw", "EB-01-02", reader2);

  }
}
