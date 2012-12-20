package com.senseidb.gateway.kafka;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.Message;
import kafka.message.MessageAndMetadata;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.impl.indexing.StreamDataProvider;

import com.senseidb.indexing.DataSourceFilter;

public class KafkaStreamDataProvider extends StreamDataProvider<JSONObject>{

  private final String _topic;
  private final String _consumerGroupId;
  private ConsumerConnector _consumerConnector;
  private ConsumerIterator<Message> _consumerIterator;

  
  private static Logger logger = Logger.getLogger(KafkaStreamDataProvider.class);
    private final String _zookeeperUrl;
    private final int _kafkaSoTimeout;
    private volatile boolean _started = false;
    private final DataSourceFilter<DataPacket> _dataConverter;
  
  public KafkaStreamDataProvider(Comparator<String> versionComparator,String zookeeperUrl,int soTimeout,int batchSize,
                                 String consumerGroupId,String topic,long startingOffset,DataSourceFilter<DataPacket> dataConverter){
    super(versionComparator);
    _consumerGroupId = consumerGroupId;
    _topic = topic;
    super.setBatchSize(batchSize);
    _zookeeperUrl = zookeeperUrl;
    _kafkaSoTimeout = soTimeout;
    _consumerConnector = null;
    _consumerIterator = null;

    _dataConverter = dataConverter;
    if (_dataConverter == null){
      throw new IllegalArgumentException("kafka data converter is null");
    }
  }
  
  @Override
  public void setStartingOffset(String version){
  }
  
  @Override
  public DataEvent<JSONObject> next() {
    if (!_started) return null;

    try
    {
      if (!_consumerIterator.hasNext())
        return null;
    }
    catch (Exception e)
    {
      // Most likely timeout exception - ok to ignore
      return null;
    }

    MessageAndMetadata<Message> msgAndmeta = _consumerIterator.next();
    if (logger.isDebugEnabled()){
      logger.debug("got new message: " + msgAndmeta.message());
    }
    long version = System.currentTimeMillis();
    
    JSONObject data;
    try {
      int size = msgAndmeta.message().payloadSize();
      ByteBuffer byteBuffer = msgAndmeta.message().payload();
      byte[] bytes = new byte[size];
      byteBuffer.get(bytes,0,size);
      data = _dataConverter.filter(new DataPacket(bytes,0,size));
      
      if (logger.isDebugEnabled()){
        logger.debug("message converted: "+data);
      }
      return new DataEvent<JSONObject>(data, String.valueOf(version));
    } catch (Exception e) {
      logger.error(e.getMessage(),e);
      return null;
    }
  }

  @Override
  public void reset() {
  }

  @Override
  public void start() {
    Properties props = new Properties();
    props.put("zk.connect", _zookeeperUrl);
    //props.put("consumer.timeout.ms", _kafkaSoTimeout);
    props.put("groupid", _consumerGroupId);

    ConsumerConfig consumerConfig = new ConsumerConfig(props);
    _consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);

    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(_topic, 1);
    Map<String, List<KafkaStream<Message>>> topicMessageStreams =
        _consumerConnector.createMessageStreams(topicCountMap);
    List<KafkaStream<Message>> streams = topicMessageStreams.get(_topic);
    KafkaStream<Message> KafkaStream = streams.iterator().next();
    _consumerIterator = KafkaStream.iterator();

    super.start();
    _started = true;
  }

  @Override
  public void stop() {
    _started = false;

    try
    {
      if (_consumerConnector!=null){
        _consumerConnector.shutdown();
      }
    }
    finally
    {
      super.stop();
    }
  }  
}
