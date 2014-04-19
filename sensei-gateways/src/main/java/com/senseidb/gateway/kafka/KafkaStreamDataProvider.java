package com.senseidb.gateway.kafka;

import java.text.DecimalFormat;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import proj.zoie.api.DataConsumer.DataEvent;
import proj.zoie.impl.indexing.StreamDataProvider;
import proj.zoie.impl.indexing.ZoieConfig;

import com.senseidb.indexing.DataSourceFilter;

public class KafkaStreamDataProvider extends StreamDataProvider<JSONObject> {
  private static Logger logger = Logger.getLogger(KafkaStreamDataProvider.class);

  private final Set<String> _topics;
  private Properties _kafkaConfig;
  protected ConsumerConnector _consumerConnector;
  private Iterator<byte[]> _consumerIterator;
  private final ThreadLocal<DecimalFormat> formatter = new ThreadLocal<DecimalFormat>() {
    @Override
    protected DecimalFormat initialValue() {
      return new DecimalFormat("00000000000000000000");
    }
  };

  private ExecutorService _executorService;

  private volatile boolean _started = false;
  private DataSourceFilter<DataPacket> _dataConverter;

  public KafkaStreamDataProvider() {
    super(ZoieConfig.DEFAULT_VERSION_COMPARATOR);
    _topics = new HashSet<String>();
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public KafkaStreamDataProvider(Comparator<String> versionComparator, Map<String, String> config,
      DataSourceFilter<DataPacket> dataFilter) {
    super(versionComparator);

    int batchSize = config.get("provider.batchSize") != null ? Integer.parseInt(config
        .get("provider.batchSize")) : 500;
    super.setBatchSize(batchSize);

    String topic = config.get("kafka.topic");
    _topics = new HashSet<String>();
    for (String raw : topic.split("[, ;]+")) {
      String t = raw.trim();
      if (t.length() != 0) {
        _topics.add(t);
      }
    }

    Properties props = new Properties();
    for (String key : config.keySet()) {
      if (key.equalsIgnoreCase("kafka.topic")) {
        continue;
      }
      if (key.startsWith("kafka.")) {
        props.put(key.substring("kafka.".length()), config.get(key));
      }
    }
    _kafkaConfig = props;

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
  }

  public void commit() {
    _consumerConnector.commitOffsets();
  }

  @Override
  public void setStartingOffset(String version) {
  }

  @Override
  public DataEvent<JSONObject> next() {
    if (!_started) {
      return null;
    }

    try {
      if (!_consumerIterator.hasNext()) {
        return null;
      }
    } catch (Exception e) {
      // Most likely timeout exception - ok to ignore
      return null;
    }

    byte[] msg = _consumerIterator.next();
    long version = getNextVersion();

    JSONObject data;
    try {
      data = _dataConverter.filter(new DataPacket(msg, 0, msg.length));
      if (logger.isDebugEnabled()) {
        logger.debug("message converted: " + data);
      }
      return new DataEvent<JSONObject>(data, getStringVersionRepresentation(version));
    } catch (Exception e) {
      logger.error(e.getMessage(), e);
      return null;
    }
  }

  public long getNextVersion() {
    return System.currentTimeMillis();
  }

  public String getStringVersionRepresentation(long version) {
    return formatter.get().format(version);
  }

  @Override
  public void reset() {
  }

  @Override
  public void start() {
    logger.info("Kafka properties: " + _kafkaConfig);
    ConsumerConfig consumerConfig = new ConsumerConfig(_kafkaConfig);
    _consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);

    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    for (String topic : _topics) {
      topicCountMap.put(topic, 1);
    }
    Map<String, List<KafkaStream<byte[], byte[]>>> topicMessageStreams = _consumerConnector
        .createMessageStreams(topicCountMap);

    final ArrayBlockingQueue<byte[]> queue = new ArrayBlockingQueue<byte[]>(8, true);

    int streamCount = 0;
    for (List<KafkaStream<byte[], byte[]>> streams : topicMessageStreams.values()) {
      streamCount += streams.size();
    }
    _executorService = Executors.newFixedThreadPool(streamCount);

    for (List<KafkaStream<byte[], byte[]>> streams : topicMessageStreams.values()) {
      for (KafkaStream<byte[], byte[]> stream : streams) {
        final KafkaStream<byte[], byte[]> messageStream = stream;
        _executorService.execute(new Runnable() {
          @Override
          public void run() {
            logger.info("Kafka consumer thread started: " + Thread.currentThread().getId());
            try {
              ConsumerIterator<byte[], byte[]> it = messageStream.iterator();
              while (it.hasNext()) {
                queue.put(it.next().message());
              }
            } catch (Exception e) {
              // normally it should the stop interupt exception.
              logger.error(e.getMessage(), e);
            }
            logger.info("Kafka consumer thread ended: " + Thread.currentThread().getId());
          }
        });
      }
    }

    _consumerIterator = new Iterator<byte[]>() {
      private byte[] message = null;

      @Override
      public boolean hasNext() {
        if (message != null) {
          return true;
        }

        try {
          message = queue.poll(1, TimeUnit.SECONDS);
        } catch (InterruptedException ie) {
          return false;
        }

        if (message != null) {
          return true;
        } else {
          return false;
        }
      }

      @Override
      public byte[] next() {
        if (hasNext()) {
          byte[] res = message;
          message = null;
          return res;
        } else {
          throw new NoSuchElementException();
        }
      }

      @Override
      public void remove() {
        throw new UnsupportedOperationException("not supported");
      }
    };

    super.start();
    _started = true;
  }

  @Override
  public void stop() {
    _started = false;

    try {
      if (_executorService != null) {
        _executorService.shutdown();
      }
    } finally {
      try {
        if (_consumerConnector != null) {
          _consumerConnector.shutdown();
        }
      } finally {
        super.stop();
      }
    }
  }
}
