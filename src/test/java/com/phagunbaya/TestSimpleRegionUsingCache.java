package com.phagunbaya;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.client.ClientRegionShortcut;
import com.phagunbaya.entities.Person;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by phagunbaya on 09/04/16.
 */

/**
 * This test case is to store non-timeries objects in region cache as well as using geode's rest APIs
 * Need to find out trade-offs between the two approaches.
 */
public class TestSimpleRegionUsingCache {

  ClientCache cache = null;
  GeodeService geodeService = new GeodeService();

  @Before
  public void setUp() throws Exception {
    cache = new ClientCacheFactory()
        .addPoolLocator("localhost", 10334)
        .create();
  }


  /*
   * This test stores person objects in cache using ClientRegionFactory
   */

  @Test
  public void storeSimpleJson() throws Exception {
    Region<String, String> region = cache
        .<String, String>createClientRegionFactory(ClientRegionShortcut.CACHING_PROXY)
        .create("simple");

    List<String> persons = new ArrayList<String>();

    for(int i=1 ; i<=50 ; i++) {
      ObjectMapper mapper = new ObjectMapper();
      Person person = new Person();
      person.setId("Id-"+Math.random());
      person.setFname("Test");
      person.setLname("User-"+i);
      person.setRandom("~!@#$%^&*()_+`-=[]\\;',./<>?:\"{}|");
      person.setAge(10+i);
      person.setCreateTime(System.currentTimeMillis() - 2*60*60*1000);
      person.setUpdateTime(System.currentTimeMillis());
      person.setType("entities.Person");
      region.put(person.getId(), mapper.writeValueAsString(person));
      persons.add(person.getId());
    }

    Assert.assertEquals(50, region.size());
    Assert.assertEquals(true, region.containsKey(persons.get(0)));
    Assert.assertEquals(false, region.containsKey("123"));
    Assert.assertEquals(50, region.query("select * from /person").asList().size());

    /*
     * Todo : Cannot perform query with where clause needs serializable class
     * Example : select * from /person where age > 12
     *
     * This creates overhead to restart geode with configured classes jar.
     */
    //Assert.assertEquals(3, region.query("select * from /person where age>12").asList().size());
  }


  /*
   * This test stores person objects using geode rest APIs
   * Also, several queries to be performed in the stored objects
   */

  @Test
  public void storeUsingRestAPIs() throws Exception {

    List<String> persons = new ArrayList<String>();

    for(int i=1 ; i<=50 ; i++) {
      ObjectMapper mapper = new ObjectMapper();
      Person person = new Person();
      person.setId("Id-"+Math.random());
      person.setFname("Test");
      person.setLname("User-"+i);
      person.setRandom("~!@#$%^&*()_+`-=[]\\;',./<>?:\"{}|");
      person.setAge(20+i);
      person.setCreateTime(System.currentTimeMillis() - 2*60*60*1000);
      person.setUpdateTime(System.currentTimeMillis());
      person.setType("entities.Person");

      persons.add(person.getId());

      //storing person objects with Http POST API /v1/{region}
      String response = geodeService.store("person", person.getId(), mapper.writeValueAsString(person));
      Assert.assertNotEquals(null, response);
    }

    /*
     * querying with where clauses using API /v1/queries/adhoc
     * No serialization required and can perform filter queries as suppose to storeSimpleJson test case.
     *
     */

    ObjectMapper mapper = new ObjectMapper();
    String response = geodeService.query("select * from /person where age < 20");
    Assert.assertNotEquals(null, response);
    List<Person> queriedPersons = mapper.readValue(response, mapper.getTypeFactory().constructCollectionType(List.class, Person.class));
    Assert.assertEquals(10, queriedPersons.size());

    response = geodeService.query("select * from /person where id='"+persons.get(0)+"'");
    Assert.assertNotEquals(null, response);
    queriedPersons = mapper.readValue(response, mapper.getTypeFactory().constructCollectionType(List.class, Person.class));
    Assert.assertEquals(1, queriedPersons.size());

    /*
     * TODO: Error executing queries with special characters
     * Need a solution ??
     *
     */
//    response = geodeService.query("select * from /person where random='~!@#$%^&*()_+`-=[]\\;\',./<>?:\"{}|'");
//    Assert.assertNotEquals(null, response);
//    queriedPersons = mapper.readValue(response, mapper.getTypeFactory().constructCollectionType(List.class, Person.class));
//    Assert.assertEquals(1, queriedPersons.size());
  }

  @After
  public void after() throws Exception {
    cache.close();
  }
}
