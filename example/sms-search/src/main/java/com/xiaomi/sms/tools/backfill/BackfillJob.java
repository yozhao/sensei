package com.xiaomi.sms.tools.backfill;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import com.senseidb.indexing.hadoop.util.PropertiesLoader;

public class BackfillJob {

  public static void main() throws IOException {
    Configuration conf = PropertiesLoader.loadProperties("conf/backfill.job");
    conf = HBaseConfiguration.create(conf);
    @SuppressWarnings("deprecation")
    Job job = new Job(conf, "SmsBackfillJob");
    job.setJarByClass(BackfillJob.class);

    Scan scan = new Scan();
    scan.setCaching(500);
    scan.setCacheBlocks(false);
    scan.addFamily(Bytes.toBytes("C"));
    scan.addColumn(Bytes.toBytes("S"), Bytes.toBytes("D"));
    String tableName = conf.get("sensei.input.hbase.tablename");

    TableMapReduceUtil.initTableMapperJob(tableName, scan, HBaseMapper.class, Text.class,
      Text.class, job);
    try {
      job.waitForCompletion(true);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
