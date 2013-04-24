package com.xiaomi.sms.tools.backfill;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import org.apache.log4j.Logger;

import com.xiaomi.sms.ShortMessage;
import com.xiaomi.sms.SmsKafkaClient;

public class HBaseMapper extends TableMapper<Text, Text> {

  private final static Logger logger = Logger.getLogger(HBaseMapper.class);
  private volatile boolean _setup = false;
  private long timestampLowerBound = 0;
  private long timestampHigherBound = Long.MAX_VALUE;
  public static final long MAX_TIMESTAMP = 9999999999999l;
  private int processed = 0;

  public void map(ImmutableBytesWritable key, Result value, Context context) throws IOException,
      InterruptedException {

    if (_setup == false) {
      throw new IllegalStateException("Mapper's setup method wasn't sucessful.");
    }

    if (++processed % 10000 != 0) {
      return;
    }
    
    context.progress();

    long timestamp = -1;
    for (int i = 0; i < value.raw().length; ++i) {
      logger.info("i cf:column: " + Bytes.toString(value.raw()[i].getFamily()) + " : "
          + Bytes.toString(value.raw()[i].getQualifier()));
      timestamp = Math.max(timestamp, value.raw()[i].getTimestamp());
    }

    if (timestamp < timestampLowerBound || timestamp > timestampHigherBound) {
      return;
    }

    byte[] status = value.getValue(Bytes.toBytes("S"), Bytes.toBytes("D"));
    if (status == null) {
      throw new IOException("Missing status column: 'S:D'.");
    }

    byte[] rowKey = ((ImmutableBytesWritable) key).get();
    String msgId = Bytes.toString(rowKey);
    String[] parts = msgId.split("-");
    if (parts.length != 4) {
      throw new IOException("Bad hbase key format.");
    }
    StringBuilder userIdStr = new StringBuilder(parts[0]);
    userIdStr.reverse();
    int userId = Integer.parseInt(userIdStr.toString());

    if (processed % 1000 == 0) {
      Thread.sleep(1000);
    }
    // the message is deleted
    if (!Arrays.equals(status, Bytes.toBytes("U"))) {
      SmsKafkaClient.DeleteOneMessage(userId, msgId);
      return;
    }

    byte[] content = value.getValue(Bytes.toBytes("C"), null);
    if (content == null) {
      throw new IOException("Missing content column: 'C'.");
    }

    ShortMessage message = new ShortMessage();
    message.msgId = msgId;

    // time stamp is inverted and stored in HBase
    long msgTime = Long.parseLong(parts[2]);
    if (msgTime > MAX_TIMESTAMP) {
      throw new IOException(value + "exceeds max time stamp");
    }
    message.msgTime = MAX_TIMESTAMP - msgTime;
    message.contents = new String(content, HConstants.UTF8_ENCODING);
    SmsKafkaClient.AddOneMessage(message);
  }

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    Configuration conf = context.getConfiguration();
    timestampLowerBound = conf.getLong("hbase.timestamp.lower.bound", 0);
    logger.info("hbase.timestamp.lower.bound: " + timestampLowerBound);
    timestampHigherBound = conf.getLong("hbase.timestamp.higher.bound", Long.MAX_VALUE);
    logger.info("hbase.timestamp.higher.bound: " + timestampHigherBound);
    String kafkaClientConf = conf.get("sms.kafka.client.conf");
    if (kafkaClientConf == null) {
      return;
    }
    URI[] localFiles = context.getCacheFiles();
    if (localFiles != null) {
      String confBaseName = new Path(kafkaClientConf).getName();
      for (int i = 0; i < localFiles.length; i++) {
        if (new Path(localFiles[i].toString()).getName().equals(confBaseName)) {
          kafkaClientConf = localFiles[i].toString();
          break;
        }
      }
    }

    // Try 10 times
    boolean succeeded = false;
    for (int i = 0; i < 10 && !succeeded; i++) {
      try {
        if (SmsKafkaClient.init(kafkaClientConf)) {
          succeeded = true;
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    _setup = succeeded;
  }
}
