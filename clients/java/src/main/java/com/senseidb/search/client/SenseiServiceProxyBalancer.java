package com.senseidb.search.client;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.Validate;
import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.BytesPushThroughSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * The configuration file should contain information like this sensei.cluster.name=sensei-example
 * sensei.cluster.url=10.235.6.46:2181,10.235.6.47:2181,10.235.6.48:2181
 * zookeeper.session.timeout=3001 zookeeper.connection.timeout=3001 sensei.server.node.ids=0,1,2,3
 * server.0=10.235.6.49:8080 server.1=10.235.6.50:8080 server.2=10.235.6.51:8080
 * server.3=10.235.6.52:8080
 */

public class SenseiServiceProxyBalancer {
  private static Logger logger = LoggerFactory.getLogger(SenseiServiceProxyBalancer.class);
  private static ZkClient zkClient = null;
  private static boolean initialized = false;
  private static final ReentrantReadWriteLock senseiServiceProxyMapLock = new ReentrantReadWriteLock();
  private static Map<Integer, SenseiServerConf> senseiConfMap = new HashMap<Integer, SenseiServerConf>();
  private static Integer[] availableNodeIds = null;
  private static Map<String, SenseiServiceProxy> senseiServiceProxyMap = new ConcurrentHashMap<String, SenseiServiceProxy>();
  private static Random random = new Random(System.currentTimeMillis());

  public static SenseiServiceProxy getSenseiServiceProxy() {
    try {
      senseiServiceProxyMapLock.readLock().lock();
      if (senseiServiceProxyMap.isEmpty()) {
        return null;
      }
      Validate.notNull(availableNodeIds);
      Validate.notEmpty(availableNodeIds);

      Integer nodeId = availableNodeIds[random.nextInt(availableNodeIds.length)];
      SenseiServerConf serverConf = senseiConfMap.get(nodeId);
      Validate.notNull(serverConf);
      Validate.isTrue(senseiServiceProxyMap.containsKey(serverConf.toString()));
      return senseiServiceProxyMap.get(serverConf.toString());
    } finally {
      senseiServiceProxyMapLock.readLock().unlock();
    }
  }

  public static synchronized boolean init(final String filePath) throws Exception {
    if (initialized) {
      return true;
    }

    File confFile = new File(filePath);
    if (!confFile.exists()) {
      throw new ConfigurationException("configuration file: " + confFile.getAbsolutePath()
          + " does not exist.");
    }
    PropertiesConfiguration conf = new PropertiesConfiguration();
    conf.setDelimiterParsingDisabled(true);
    conf.load(confFile);

    String zkClusterUrl = conf.getString("sensei.cluster.url");
    Validate.notNull(zkClusterUrl);
    int zkSessionTimeout = conf.getInt("zookeeper.session.timeout", 30000);
    int zkConnectionTimeout = conf.getInt("zookeeper.connection.timeout", 30000);

    String nodeIds = conf.getString("sensei.server.node.ids");
    Validate.notNull(nodeIds);

    String[] idsArray = nodeIds.split("[,\\s]+");
    Validate.notEmpty(idsArray);

    for (int i = 0; i < idsArray.length; i++) {
      Integer nodeId = Integer.valueOf(idsArray[i]);
      String serverAddress = conf.getString("server." + nodeId);
      Validate.notNull(serverAddress);
      if (serverAddress == null) {
        throw new ConfigurationException("server." + i + " doesn't exist.");
      }
      SenseiServerConf serverConf = new SenseiServerConf();
      String[] parts = serverAddress.split(":");
      if (parts.length != 2) {
        throw new ConfigurationException("bad server address: " + serverAddress);
      }
      serverConf.host = parts[0];
      serverConf.port = Integer.parseInt(parts[1]);
      senseiConfMap.put(nodeId, serverConf);
    }

    logger.info("Trying to initialize an ZkClient instance, zookeeper servers : {}.", zkClusterUrl);
    zkClient = new ZkClient(zkClusterUrl, zkSessionTimeout, zkConnectionTimeout,
        new BytesPushThroughSerializer());
    Validate.notNull(zkClient);

    String senseiClusterName = conf.getString("sensei.cluster.name");

    String availableServerPath = "/" + senseiClusterName + "/available";
    zkClient.subscribeChildChanges(availableServerPath, new IZkChildListener() {
      @Override
      public void handleChildChange(String parentPath, List<String> currentChildren)
          throws Exception {
        logger.info("ZNODE {}'s children were changed, current children are {}.", parentPath,
          currentChildren);
        resetSenseiServiceMap(currentChildren);
      }
    });

    List<String> availableServers = zkClient.getChildren(availableServerPath);
    resetSenseiServiceMap(availableServers);
    initialized = true;
    return true;
  }

  private static void resetSenseiServiceMap(List<String> availableServers) throws Exception {
    if (availableServers.isEmpty()) {
      senseiServiceProxyMapLock.writeLock().lock();
      senseiServiceProxyMap.clear();
      availableNodeIds = null;
      senseiServiceProxyMapLock.writeLock().unlock();
    } else {
      Map<String, SenseiServiceProxy> tempServerMap = new ConcurrentHashMap<String, SenseiServiceProxy>();
      Integer[] tempNodeIds = new Integer[availableServers.size()];
      boolean newServerAdded = false;
      for (int i = 0; i < availableServers.size(); i++) {
        Integer nodeId = Integer.valueOf(availableServers.get(i));
        tempNodeIds[i] = nodeId;
        SenseiServerConf serverConf = senseiConfMap.get(nodeId);
        Validate.notNull(serverConf);
        if (senseiServiceProxyMap.containsKey(serverConf.toString())) {
          tempServerMap
              .put(serverConf.toString(), senseiServiceProxyMap.get(serverConf.toString()));
        } else {
          SenseiServiceProxy senseiServiceProxy = new SenseiServiceProxy(serverConf.getHost(),
              serverConf.getPort());
          tempServerMap.put(serverConf.toString(), senseiServiceProxy);
          newServerAdded = true;
        }
      }

      // if new server is added, we need sleep several seconds to make sure new server is ready
      if (newServerAdded) {
        Thread.sleep(5000);
      }

      senseiServiceProxyMapLock.writeLock().lock();
      senseiServiceProxyMap = tempServerMap;
      availableNodeIds = tempNodeIds;
      senseiServiceProxyMapLock.writeLock().unlock();
      tempServerMap = null;
      tempNodeIds = null;
    }
  }

  static class SenseiServerConf {
    private String host;
    private int port;

    String getHost() {
      return host;
    }

    int getPort() {
      return port;
    }

    @Override
    public String toString() {
      return host + ":" + port;
    }
  }

}
