package com.senseidb.federated.broker;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;

import junit.framework.TestCase;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import voldemort.client.StoreClient;
import zu.core.cluster.ZuCluster;

import com.browseengine.bobo.api.BrowseSelection;
import com.senseidb.federated.broker.proxy.BrokerProxy;
import com.senseidb.federated.broker.proxy.SenseiBrokerProxy;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiResult;
import com.twitter.common.zookeeper.ZooKeeperClient;
import com.twitter.common.zookeeper.testing.BaseZooKeeperTest;

public class FederatedBrokerIntegrationTest extends BaseZooKeeperTest{
  
  private ClassPathXmlApplicationContext brokerContext;
  private FederatedBroker federatedBroker;
  private StoreClient<String, String> storeClient;
  private BrokerProxy senseiProxy;
  
  private ZooKeeperClient zkClient;
  private ZuCluster clusterClient;
  
  @Before
  public void init() throws Exception {
   
   brokerContext = new ClassPathXmlApplicationContext("federatedBroker-context.xml");
   
   storeClient = (StoreClient<String,String>) brokerContext.getBean("storeClient");
   
   BrokerProxy voldermortProxy = (BrokerProxy) brokerContext.getBean("voldermortProxy");
   
   Configuration senseiConfiguration =(Configuration)brokerContext.getBean("senseiConfiguration");
   
   senseiProxy = SenseiBrokerProxy.valueOf(senseiConfiguration, new HashMap<String,String>(), clusterClient);
   
   federatedBroker = new FederatedBroker(Arrays.asList(senseiProxy,voldermortProxy));
   
   JSONArray arr = readCarDocs();
   storeClient.put("test", arr.toString());
   
   federatedBroker.start();
   
   zkClient = createZkClient();
   clusterClient = new ZuCluster(zkClient, "senseiClient");
   SingleNodeStarter.start("conf", 15000, clusterClient);
   
  }
  
  private JSONArray readCarDocs() throws IOException, URISyntaxException, JSONException {
    JSONArray arr = new JSONArray();
     LineIterator lineIterator = FileUtils.lineIterator(new File(FederatedBrokerIntegrationTest.class.getClassLoader().getResource("data/cars.json").toURI()));
     while(lineIterator.hasNext()) {
       String car = lineIterator.next();
       if (car != null && car.contains("{")) {
        JSONObject carDoc = new JSONObject(car);
        carDoc.put("id", carDoc.getLong("id") + 15000);
        arr.put(carDoc);
      }
      
     }
    return arr;
  }
  
  @Test
  public void test1SearchOnTwoClusters() throws Exception {
    SenseiRequest req = new SenseiRequest();
    BrowseSelection sel = new BrowseSelection("year");
    String selVal = "[2001 TO 2002]";
    sel.addValue(selVal);
    req .addSelection(sel);
    SenseiResult result = federatedBroker.browse(req);
    TestCase.assertEquals(30000, result.getTotalDocs());
    TestCase.assertEquals(5814, result.getNumHits());
    SenseiResult oneProxyResult = senseiProxy.doQuery(req).get(0);
    TestCase.assertEquals(15000, oneProxyResult.getTotalDocs());
    TestCase.assertEquals(2907, oneProxyResult.getNumHits());
  }
  
  
  @After
  public void shutdown() throws Exception {
	try{
	  federatedBroker.stop(); 
	}
	finally {
	  try{
		brokerContext.close();
	  }
	  finally {
		zkClient.close();
        clusterClient.shutdown();
	  }
	}
  }
}
