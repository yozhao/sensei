package com.senseidb.gateway.kafka;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

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

public class MiKafkaStreamDataProvider extends StreamDataProvider<JSONObject> {

  private final String _topic;
  private final String _consumerGroupId;
  private ConsumerConnector _consumerConnector;
  private ConsumerIterator<Message> _consumerIterator;

  private static Logger logger = Logger.getLogger(KafkaStreamDataProvider.class);
  private final String _zookeeperUrl;
  private final String _oldSinceKey;
  private final boolean _rewind;
  private final int _kafkaTimeout;
  private volatile boolean _started = false;
  private final DataSourceFilter<DataPacket> _dataConverter;
  private volatile Map<String, Long> _partitionOffsetMap = new TreeMap<String, Long>();

  private static long _startTime = 0;
  private static int count = 0;

  public MiKafkaStreamDataProvider(Comparator<String> versionComparator, String zookeeperUrl,
      int timeout, String consumerGroupId, String topic, String oldSinceKey,
      DataSourceFilter<DataPacket> dataConverter, boolean rewind) {
    super(versionComparator);
    _consumerGroupId = consumerGroupId;
    _topic = topic;
    _zookeeperUrl = zookeeperUrl;
    _oldSinceKey = oldSinceKey;
    _rewind = rewind;
    _kafkaTimeout = timeout;
    _consumerConnector = null;
    _consumerIterator = null;

    _dataConverter = dataConverter;
    if (_dataConverter == null) {
      throw new IllegalArgumentException("kafka data converter is null");
    }
  }

  @Override
  public void setStartingOffset(String version) {
  }

  @Override
  public DataEvent<JSONObject> next() {
    if (!_started) return null;

    try {
      if (!_consumerIterator.hasNext()) return null;
    } catch (Exception e) {
      // Most likely timeout exception - ok to ignore
      return null;
    }

    MessageAndMetadata<Message> messageAndMetadata = _consumerIterator.next();
    if (logger.isDebugEnabled()) {
      logger.debug("got new message: " + messageAndMetadata.message());
    }

    JSONObject data;
    try {
      int size = messageAndMetadata.message().payloadSize();
      ByteBuffer byteBuffer = messageAndMetadata.message().payload();
      byte[] bytes = new byte[size];
      byteBuffer.get(bytes, 0, size);
      data = _dataConverter.filter(new DataPacket(bytes, 0, size));

      if (logger.isDebugEnabled()) {
        logger.debug("message converted: " + data);
      }
      _partitionOffsetMap.put(messageAndMetadata.partition(), messageAndMetadata.offset());

      String version = String.valueOf(System.currentTimeMillis());
      for (Map.Entry<String, Long> entry : _partitionOffsetMap.entrySet()) {
        version += ";" + entry.getKey() + ":" + entry.getValue().toString();
      }
      ++count;
      if (count % 50000 == 0) {
        System.out.println("Count = " + count + " QPS: " + count * 1000.0
            / (System.currentTimeMillis() - _startTime));
      }

      return new DataEvent<JSONObject>(data, version);
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
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
    props.put("consumer.timeout.ms", String.valueOf(_kafkaTimeout));
    props.put("groupid", _consumerGroupId);
    props.put("autocommit.enable", "true");

    ConsumerConfig consumerConfig = new ConsumerConfig(props);
    _consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);

    if (_rewind && _oldSinceKey != null) {
      // rewind to last checkpoint
      String parts[] = _oldSinceKey.split(";");
      // the first one is time stamp
      if (parts.length > 1) {
        boolean failed = false;

        for (int i = 1; i < parts.length; ++i) {
          String pair[] = parts[i].split(":");
          if (pair.length != 2) {
            failed = true;
            break;
          }
          if (!pair[1].matches("[0-9]+")) {
            failed = true;
            break;
          }
          _partitionOffsetMap.put(pair[0], Long.valueOf(pair[1]));
        }

        if (failed) {
          _partitionOffsetMap.clear();
        } else {
          _consumerConnector.commitOffsets(_topic, _partitionOffsetMap);
        }
      }
    }

    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(_topic, 1);
    Map<String, List<KafkaStream<Message>>> topicMessageStreams = _consumerConnector
        .createMessageStreams(topicCountMap);
    List<KafkaStream<Message>> streams = topicMessageStreams.get(_topic);
    KafkaStream<Message> KafkaStream = streams.iterator().next();
    _consumerIterator = KafkaStream.iterator();

    super.start();
    _started = true;
    _startTime = System.currentTimeMillis();
  }

  @Override
  public void stop() {
    _started = false;

    try {
      if (_consumerConnector != null) {
        _consumerConnector.shutdown();
      }
    } finally {
      super.stop();
    }
  }
}
