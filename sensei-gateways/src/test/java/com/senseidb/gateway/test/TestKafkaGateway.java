package com.senseidb.gateway.test;

import com.senseidb.gateway.SenseiGateway;
import com.senseidb.plugin.SenseiPluginRegistry;
import com.twitter.common.application.ShutdownRegistry.ShutdownRegistryImpl;
import com.twitter.common.zookeeper.testing.ZooKeeperTestServer;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.io.FileUtils;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import proj.zoie.impl.indexing.StreamDataProvider;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

public class TestKafkaGateway {
  @SuppressWarnings("rawtypes")
  static SenseiGateway kafkaGateway;

  @SuppressWarnings("rawtypes")
  static SenseiGateway simpleKafkaGateway;

  static SenseiPluginRegistry pluginRegistry = null;
  static SenseiPluginRegistry pluginRegistry2 = null;

  static KafkaServerStartable kafkaServer = null;

  static File kafkaLogFile = null;

  private static ZooKeeperTestServer zkTestServer;
  private static int port;

  @BeforeClass
  public static void init() throws Exception {
    final ShutdownRegistryImpl shutdownRegistry = new ShutdownRegistryImpl();

    try {
      zkTestServer = new ZooKeeperTestServer(0, shutdownRegistry,
          ZooKeeperTestServer.DEFAULT_SESSION_TIMEOUT);
      port = zkTestServer.startNetwork();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    Properties kafkaProps = new Properties();
    kafkaProps.setProperty("num.partitions", "1");
    kafkaProps.setProperty("port", "9092");
    kafkaProps.setProperty("broker.id", "0");
    kafkaProps.setProperty("log.dir", "/tmp/sensei-gateway-test-kafka-logs");
    // override to the local running zk server
    kafkaProps.setProperty("zookeeper.connect", "localhost:" + port);

    kafkaLogFile = new File(kafkaProps.getProperty("log.dir"));
    FileUtils.deleteDirectory(kafkaLogFile);

    KafkaConfig kafkaConfig = new KafkaConfig(kafkaProps);
    kafkaServer = new KafkaServerStartable(kafkaConfig);

    kafkaServer.startup();

    Configuration config = new PropertiesConfiguration();
    config
        .addProperty("sensei.gateway.class", "com.senseidb.gateway.kafka.KafkaDataProviderBuilder");
    config.addProperty("sensei.gateway.kafka.group.id", "1");
    config.addProperty("sensei.gateway.kafka.zookeeper.connect", "localhost:" + port);
    config.addProperty("sensei.gateway.kafka.auto.offset.reset", "smallest");
    config.addProperty("sensei.gateway.kafka.topic", "test");
    config.addProperty("sensei.gateway.provider.batchSize", "1");
    pluginRegistry = SenseiPluginRegistry.build(config);
    pluginRegistry.start();

    kafkaGateway = pluginRegistry.getBeanByFullPrefix("sensei.gateway", SenseiGateway.class);
    kafkaGateway.start();

    config = new PropertiesConfiguration();
    config.addProperty("sensei.gateway.class", "com.senseidb.gateway.kafka.SimpleKafkaGateway");
    config.addProperty("sensei.gateway.kafka.host", "localhost");
    config.addProperty("sensei.gateway.kafka.port", "9092");
    config.addProperty("sensei.gateway.kafka.topic", "test");
    config.addProperty("sensei.gateway.kafka.timeout", "3000");
    config.addProperty("sensei.gateway.provider.batchSize", "1");
    pluginRegistry2 = SenseiPluginRegistry.build(config);
    pluginRegistry2.start();

    simpleKafkaGateway = pluginRegistry2.getBeanByFullPrefix("sensei.gateway", SenseiGateway.class);
    simpleKafkaGateway.start();

    Properties props = new Properties();
    props.put("metadata.broker.list", "localhost:9092");
    props.put("serializer.class", "kafka.serializer.StringEncoder");

    ProducerConfig producerConfig = new ProducerConfig(props);
    Producer<String, String> kafkaProducer = new Producer<String, String>(producerConfig);

    for (JSONObject jsonObj : BaseGatewayTestUtil.dataList) {
      KeyedMessage<String, String> data = new KeyedMessage<String, String>("test",
          jsonObj.toString());
      kafkaProducer.send(data);
    }
  }

  @AfterClass
  public static void shutdown() {
    kafkaGateway.stop();
    pluginRegistry.stop();

    simpleKafkaGateway.stop();
    pluginRegistry2.stop();

    try {
      if (kafkaServer != null) {
        kafkaServer.shutdown();
        kafkaServer.awaitShutdown();
      }
      zkTestServer.shutdownNetwork();
    } finally {
      try {
        FileUtils.deleteDirectory(kafkaLogFile);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  @Test
  public void testSimpleKafka() throws Exception {
    final StreamDataProvider<JSONObject> dataProvider = simpleKafkaGateway.buildDataProvider(null,
        String.valueOf("0"), null, null);
    BaseGatewayTestUtil.doTest(dataProvider);
  }

  @Test
  public void testKafka() throws Exception {
    final StreamDataProvider<JSONObject> dataProvider = kafkaGateway.buildDataProvider(null,
        String.valueOf("0"), null, null);
    BaseGatewayTestUtil.doTest(dataProvider);
  }
}
