package com.phagunbaya;

/**
 * Created by phagunbaya on 12/04/16.
 */

import com.fasterxml.jackson.databind.ObjectMapper;
import com.phagunbaya.entities.Episode;
import com.phagunbaya.entities.Person;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * This test case is to store timeseries objects.
 * Need to find out trade-offs between the two approaches where episodes should be stored as kv pairs or as a whole dataframe ?
 * Using kv storage, requires to store objects with id. It's overhead. Then who manages pointer to these ids ?
 *
 */

public class TestEpisodeRegion {
  GeodeService geodeService = new GeodeService();
  List<Episode> episodes = new ArrayList<Episode>();

  /*
   * In this test case we are storing episode data as kv pairs in a region.
   * A region correspond to episodes of similar type. In this case how do we manage creation of new regions in production ?
   *
   */

  @Before
  public void setUp() throws Exception {
    for(int i=0; i<100;i++) {
      Episode epi = new Episode();
      epi.setTime(System.currentTimeMillis() - 6000000*i);
      epi.setEndTime(System.currentTimeMillis() - 30000000*i);
      epi.setLabel("Label-"+i);
      epi.setLabel("Thing-"+i);
      episodes.add(epi);
    }

    Iterator<Episode> itr = episodes.iterator();
    while (itr.hasNext()){
      Episode ep = itr.next();
      ObjectMapper mapper = new ObjectMapper();

      //storing episodes
      String resp = geodeService.store("episode", null, mapper.writeValueAsString(ep));
      Assert.assertNotEquals(null, resp);
    }

  }

  /*
   * Query episode data with time range
   */
  @Test
  public void timeRangeQuery() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    String response = geodeService.query("select * from /episode where time>10000 and time<"+System.currentTimeMillis());
    Assert.assertNotEquals(null, response);
    List<Person> queriedEpisodes = mapper.readValue(response, mapper.getTypeFactory().constructCollectionType(List.class, Episode.class));
    Assert.assertEquals(10, queriedEpisodes.size());
  }

  /*
   * Query episode data with time range and thing field
   */
  @Test
  public void timeRangeWithCustomQuery_1() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    String response = geodeService.query("select * from /episode where time>10000 and time<"+System.currentTimeMillis()+" and thing='Thing-10'");
    Assert.assertNotEquals(null, response);
    List<Person> queriedEpisodes = mapper.readValue(response, mapper.getTypeFactory().constructCollectionType(List.class, Episode.class));
    Assert.assertEquals(1, queriedEpisodes.size());
  }

  /*
   * Query episode data with time range with label field
   */
  @Test
  public void timeRangeWithCustomQuery_2() throws Exception {
    ObjectMapper mapper = new ObjectMapper();
    String response = geodeService.query("select * from /episode where time>10000 and time<"+System.currentTimeMillis()+" and label='Label-10'");
    Assert.assertNotEquals(null, response);
    List<Person> queriedEpisodes = mapper.readValue(response, mapper.getTypeFactory().constructCollectionType(List.class, Episode.class));
    Assert.assertEquals(1, queriedEpisodes.size());
  }
}
