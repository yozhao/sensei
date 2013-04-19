package com.xiaomi.sms;

import java.io.FileInputStream;
import java.util.Properties;

import at.orz.hash.CityHash;
import kafka.javaapi.producer.ProducerData;
import kafka.producer.ProducerConfig;
import org.json.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SmsKafkaClient {
  private static Logger logger = LoggerFactory.getLogger(SmsKafkaClient.class);
  private static boolean initialized = false;
  private static kafka.javaapi.producer.Producer<Integer, String> producer;
  private static final Properties props = new Properties();
  private static int retryNumber = 0;
  private static int retryIntervalInMillis = 500;
  private static String kafkaTopic = null;

  public static class MessageDeleteRequest {
    public long id;

    public long getId() {
      return this.id;
    }

    public String type;

    public String getType() {
      return this.type;
    }

    public int userId;

    public int getUserId() {
      return this.userId;
    }
  }

  public static boolean AddOneMessage(ShortMessage message) throws Exception {
    if (message.userId < 0 || message.msgId == null || message.msgTime < 0
        || message.contents == null) {
      throw new Exception("Bad message input");
    }
    byte[] msgIdByte = message.msgId.getBytes();
    message.id = CityHash.cityHash64(msgIdByte, 0, msgIdByte.length);
    JSONObject json = new JSONObject(message);

    boolean done = true;
    for (int i = 0; i < retryNumber + 1; ++i) {
      try {
        producer.send(new ProducerData<Integer, String>(kafkaTopic, message.userId, json.toString()));
      } catch (Exception ex) {
        if (ex instanceof kafka.common.NoBrokersForPartitionException) {
          logger.error("No broker exception, seems all brokers are down.");
        } else if (ex instanceof java.io.IOException) {
          logger.error("Probably one broker is down, exception: " + ex.toString());
        } else {
          logger.error("Exception: " + ex.toString());
        }
        done = false;
      }

      if (done) {
        break;
      }
      if (i < retryNumber) {
        Thread.sleep(retryIntervalInMillis);
      }
    }
    return done;
  }

  public static boolean DeleteOneMessage(int userId, String msgId) throws Exception {
    MessageDeleteRequest request = new MessageDeleteRequest();
    request.userId = userId;
    byte[] msgIdByte = msgId.getBytes();
    request.id = CityHash.cityHash64(msgIdByte, 0, msgIdByte.length);
    request.type = "delete";
    JSONObject json = new JSONObject(request);

    boolean done = true;
    for (int i = 0; i < retryNumber + 1; ++i) {
      try {
        producer.send(new ProducerData<Integer, String>(kafkaTopic, request.userId, json.toString()));
      } catch (Exception ex) {
        if (ex instanceof kafka.common.NoBrokersForPartitionException) {
          logger.error("No broker exception, seems all brokers are down.");
        } else if (ex instanceof java.io.IOException) {
          logger.error("Probably one broker is down, exception: " + ex.toString());
        } else {
          logger.error("Exception: " + ex.toString());
        }
        done = false;
      }
      if (done) {
        break;
      }
      if (i < retryNumber) {
        Thread.sleep(retryIntervalInMillis);
      }
    }
    return done;
  }

  public static boolean init(final String filePath) throws Exception {
    if (initialized) {
      return true;
    }
    props.load(new FileInputStream(filePath));
    producer = new kafka.javaapi.producer.Producer<Integer, String>(new ProducerConfig(props));
    retryNumber = Integer.valueOf(props.getProperty("client.retry.number"));
    retryIntervalInMillis = Integer.valueOf(props.getProperty("client.retry.interval"));
    kafkaTopic = props.getProperty("client.kafka.topic");
    if (kafkaTopic == null) {
      logger.error("client.kafka.topic parameter doesn't exist.");
      return false;
    }
    initialized = true;
    return true;
  }
}
