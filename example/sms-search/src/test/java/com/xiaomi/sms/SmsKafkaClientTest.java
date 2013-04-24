package com.xiaomi.sms;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Random;

public class SmsKafkaClientTest {
  private static final Random randomSeed = new Random(System.currentTimeMillis());

  private static long nextRandomSeed() {
    return randomSeed.nextLong();
  }

  private static final Random rand = new Random(nextRandomSeed());
  private static int testScale = 10000;
  private static ArrayList<String> wordList = new ArrayList<String>();
  
  public static void InitWordList() throws Exception {
    File wordListFile = new File("main2012.dic");
    BufferedReader reader = new BufferedReader(new FileReader(wordListFile));
    String tempString = null;
    while ((tempString = reader.readLine()) != null) {
      wordList.add(tempString);
    }
    System.out.println("Load " + wordList.size() + " words from word list file.");
  }

  public static void SendOneMessage(long id) throws Exception {
    ShortMessage message = new ShortMessage();
    message.userId = (int)id + 110000;
    message.msgId = String.valueOf(id);
    message.msgTime = System.currentTimeMillis();

    int contentLength = (int) (30.0 * rand.nextGaussian() + 30);
    if (contentLength < 0) {
      contentLength = 1;
    }
    message.contents = "";
    
    while (message.contents.length() < contentLength) {
      int idx = rand.nextInt(wordList.size());
      message.contents += wordList.get(idx);
    }
    SmsKafkaClient.AddOneMessage(message);
  }
  
  public static void DeleteOneMessage(long id) throws Exception {
    SmsKafkaClient.DeleteOneMessage((int)id + 110000, String.valueOf(id));
  }
  

  public static void main(String[] args) throws Exception {
    InitWordList();

    if (args.length != 2) {
      throw new Exception("Input args number is wrong");
    }

    testScale = Integer.parseInt(args[0]);

    int reportPeriod = testScale / 10 > 10 ? testScale / 10 : 10;

    if (!SmsKafkaClient.init(args[1])) {
      throw new Exception("Kafka client init failed!");
    }
    
    long startTime = System.currentTimeMillis();
    for (long i = 1; i <= testScale; ++i) {
      SendOneMessage(i);
      if (i % reportPeriod == 0) {
        long curTime = System.currentTimeMillis();
        System.out.println("Send " + i + " message in " + (curTime - startTime) / 1000.0
            + " seconds, QPS is " + i * 1000.0 / (curTime - startTime));
        Thread.sleep(1000);
      }
    }
    
    System.out.println("Deleting ...");
    
    for (long i = 1; i <= testScale; ++i) {
      DeleteOneMessage(i);
      if (i % reportPeriod == 0) {
        long curTime = System.currentTimeMillis();
        System.out.println("Delete " + i + " message in " + (curTime - startTime) / 1000.0
            + " seconds, QPS is " + i * 1000.0 / (curTime - startTime));
        Thread.sleep(1000);
      }
    }
    System.out.println("Deleted!");
  }
}
