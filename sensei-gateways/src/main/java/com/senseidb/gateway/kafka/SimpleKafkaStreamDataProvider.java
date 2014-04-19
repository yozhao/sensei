package com.senseidb.gateway.kafka;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.OffsetRequest;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.impl.indexing.StreamDataProvider;

import com.senseidb.indexing.DataSourceFilter;

public class SimpleKafkaStreamDataProvider extends StreamDataProvider<JSONObject> {
  private final String _topic;
  private long _offset;
  private long _startingOffset;
  private SimpleConsumer _kafkaConsumer = null;

  private Iterator<MessageAndOffset> _msgIter;
  private ThreadLocal<byte[]> _bytesFactory;

  private static Logger logger = Logger.getLogger(KafkaStreamDataProvider.class);

  public static final int DEFAULT_MAX_MSG_SIZE = 5 * 1024 * 1024;
  private final String _kafkaHost;
  private final int _kafkaPort;
  private final int _kafkaTimeout;
  private volatile boolean _started = false;
  private final DataSourceFilter<DataPacket> _dataConverter;
  private final String _kafkaClientId = "0";
  private final int _kafkaPartitionId = 0;

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public SimpleKafkaStreamDataProvider(Comparator<String> versionComparator,
      Map<String, String> config, long startingOffset, DataSourceFilter<DataPacket> dataFilter) {
    super(versionComparator);

    _kafkaHost = config.get("kafka.host");
    _kafkaPort = Integer.parseInt(config.get("kafka.port"));
    _topic = config.get("kafka.topic");
    String timeoutStr = config.get("kafka.timeout");
    _kafkaTimeout = timeoutStr != null ? Integer.parseInt(timeoutStr) : 10000;
    int batchSize = config.get("provider.batchSize") != null ? Integer.parseInt(config
        .get("provider.batchSize")) : 500;
    super.setBatchSize(batchSize);

    _startingOffset = startingOffset;
    _offset = startingOffset;

    if (dataFilter == null) {
      String type = config.get("kafka.msg.type");
      if (type == null) {
        type = "json";
      }
      if ("json".equals(type)) {
        dataFilter = new DefaultJsonDataSourceFilter();
      } else if ("avro".equals(type)) {
        try {
          String msgClsString = config.get("kafka.msg.avro.class");
          String dataMapperClassString = config.get("kafka.msg.avro.datamapper");
          Class<?> cls = Class.forName(msgClsString);
          Class<?> dataMapperClass = Class.forName(dataMapperClassString);
          DataSourceFilter dataMapper = (DataSourceFilter) dataMapperClass.newInstance();
          dataFilter = new AvroDataSourceFilter(cls, dataMapper);
        } catch (Exception e) {
          throw new IllegalArgumentException("Unable to construct avro data filter", e);
        }
      } else {
        throw new IllegalArgumentException("Invalid msg type: " + type);
      }
    }
    _dataConverter = dataFilter;

    _bytesFactory = new ThreadLocal<byte[]>() {
      @Override
      protected byte[] initialValue() {
        return new byte[DEFAULT_MAX_MSG_SIZE];
      }
    };
  }

  @Override
  public void setStartingOffset(String version) {
    _offset = Long.parseLong(version);
  }

  private FetchRequest buildReq() {
    if (_offset <= 0) {
      long time = OffsetRequest.EarliestTime();
      if (_offset == -1) {
        time = OffsetRequest.LatestTime();
      }

      TopicAndPartition topicAndPartition = new TopicAndPartition(_topic, _kafkaPartitionId);
      Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
      requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(time, 1));
      kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo,
          kafka.api.OffsetRequest.CurrentVersion(), _kafkaClientId);
      kafka.javaapi.OffsetResponse response = _kafkaConsumer.getOffsetsBefore(request);

      if (response.hasError()) {
        logger.error("Error fetching data Offset Data the Broker. Reason: "
            + response.errorCode(_topic, _kafkaPartitionId));
        return null;
      }
      long[] offsets = response.offsets(_topic, _kafkaPartitionId);
      _offset = offsets[0];
    }
    FetchRequest req = new FetchRequestBuilder().clientId(_kafkaClientId)
        .addFetch(_topic, _kafkaPartitionId, _offset, 10000).build();
    return req;
  }

  @Override
  public DataEvent<JSONObject> next() {
    if (!_started) {
      return null;
    }
    if (_msgIter == null || !_msgIter.hasNext()) {
      if (logger.isDebugEnabled()) {
        logger.debug("fetching new batch from offset: " + _offset);
      }
      FetchRequest req = buildReq();
      if (req == null) {
        return null;
      }
      FetchResponse response = _kafkaConsumer.fetch(req);
      ByteBufferMessageSet msgSet = response.messageSet(_topic, _kafkaPartitionId);
      _msgIter = msgSet.iterator();
    }

    if (_msgIter == null || !_msgIter.hasNext()) {
      if (logger.isDebugEnabled()) {
        logger.debug("no more data, msgIter: " + _msgIter);
      }
      return null;
    }

    MessageAndOffset msg = _msgIter.next();
    if (logger.isDebugEnabled()) {
      logger.debug("got new message: " + msg);
    }
    long version = _offset;
    _offset = msg.nextOffset();

    JSONObject data;
    try {
      int size = msg.message().payloadSize();
      ByteBuffer byteBuffer = msg.message().payload();
      byte[] bytes = _bytesFactory.get();
      byteBuffer.get(bytes, 0, size);

      data = _dataConverter.filter(new DataPacket(bytes, 0, size));

      if (logger.isDebugEnabled()) {
        logger.debug("message converted: " + data);
      }
      return new DataEvent<JSONObject>(data, String.valueOf(version));
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      return null;
    }
  }

  @Override
  public void reset() {
    _offset = _startingOffset;
  }

  @Override
  public void start() {
    _kafkaConsumer = new SimpleConsumer(_kafkaHost, _kafkaPort, _kafkaTimeout,
        DEFAULT_MAX_MSG_SIZE, _kafkaClientId);
    super.start();
    _started = true;
  }

  @Override
  public void stop() {
    _started = false;
    try {
      if (_kafkaConsumer != null) {
        _kafkaConsumer.close();
      }
    } finally {
      super.stop();
    }
  }
}
