package com.senseidb.federated.broker;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.mortbay.jetty.Server;

import zu.core.cluster.ZuCluster;

import com.senseidb.conf.SenseiConfParams;
import com.senseidb.conf.SenseiServerBuilder;
import com.senseidb.federated.broker.proxy.SenseiBrokerProxy;
import com.senseidb.search.node.SenseiServer;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiResult;

public class SingleNodeStarter {
  private static boolean serverStarted = false;
  private static Server jettyServer;
  private static SenseiServer server;
  private static ZuCluster zuCluster;

  public static void start(String localPath, int expectedDocs, ZuCluster clusterClient) {
    start(new File(getUri(localPath)), expectedDocs, clusterClient);
  }

  public static void stop() {
	  try {
        jettyServer.stop();
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        try {
          server.shutdown();
        }
        finally{
          zuCluster.shutdown();
        }
      }
    
  }
  
  public static void start(File confDir, int expectedDocs, final ZuCluster clusterClient) {
    if (!serverStarted) {
      try {
    	  zuCluster = clusterClient;
        PropertiesConfiguration senseiConfiguration = new PropertiesConfiguration(new File(confDir, "sensei.properties"));
        final String indexDir = senseiConfiguration.getString(SenseiConfParams.SENSEI_INDEX_DIR);
        rmrf(new File(indexDir));
        SenseiServerBuilder senseiServerBuilder = new SenseiServerBuilder(confDir, null);
        senseiServerBuilder.setClusterClient(clusterClient);
        server = senseiServerBuilder.buildServer();
        jettyServer = senseiServerBuilder.buildHttpRestServer();
        server.start(true);
        jettyServer.start();
        Runtime.getRuntime().addShutdownHook(new Thread() {
          @Override
          public void run() {
            try {
            	rmrf(new File(indexDir));
            } catch (Exception e) {
              e.printStackTrace();
            } 
          }
        });
        SenseiBrokerProxy brokerProxy = SenseiBrokerProxy.valueOf(senseiConfiguration, new HashMap<String, String>(), clusterClient);
        while (true) {
          SenseiResult senseiResult = brokerProxy.doQuery(new SenseiRequest()).get(0);
          int totalDocs = senseiResult.getTotalDocs();
          System.out.println("TotalDocs = " + totalDocs);
          if (totalDocs >= expectedDocs) {
            break;
          }
          Thread.sleep(100);
        }
      } catch (Exception ex) {
        throw new RuntimeException(ex);
      }
    }
  }

  public static boolean rmrf(File f) {
    if (f == null || !f.exists()) {
      return true;
    }
    if (f.isDirectory()) {
      for (File sub : f.listFiles()) {
        if (!rmrf(sub))
          return false;
      }
    }
    return f.delete();
  }

  private static URI getUri(String localPath) {
    try {
      return FederatedBrokerIntegrationTest.class.getClassLoader().getResource(localPath).toURI();
    } catch (URISyntaxException ex) {
      throw new RuntimeException(ex);
    }
  }
}
