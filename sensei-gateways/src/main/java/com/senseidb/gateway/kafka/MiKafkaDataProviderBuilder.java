package com.senseidb.gateway.kafka;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Set;
import java.util.regex.Pattern;

import org.json.JSONObject;

import proj.zoie.impl.indexing.StreamDataProvider;

import com.senseidb.gateway.SenseiGateway;
import com.senseidb.indexing.DataSourceFilter;
import com.senseidb.indexing.ShardingStrategy;

public class MiKafkaDataProviderBuilder extends SenseiGateway<DataPacket>{

	private final Comparator<String> _versionComparator = new MiVersionComparator(); 

	@Override
  public StreamDataProvider<JSONObject> buildDataProvider(DataSourceFilter<DataPacket> dataFilter,
      String oldSinceKey,
      ShardingStrategy shardingStrategy,
      Set<Integer> partitions) throws Exception
  {
	  String zookeeperUrl = config.get("kafka.zookeeperUrl");
	  String consumerGroupId = config.get("kafka.consumerGroupId");
    String topic = config.get("kafka.topic");
    String timeoutStr = config.get("kafka.timeout");
    String rewindStr = config.get("kafka.rewind");
    int timeout = timeoutStr != null ? Integer.parseInt(timeoutStr) : 10000;
    boolean rewind = rewindStr != null ? Boolean.parseBoolean(rewindStr) : false;

    if (dataFilter==null){
      String type = config.get("kafka.msg.type");
      if (type == null){
        type = "json";
      }
    
      if ("json".equals(type)){
        dataFilter = new DefaultJsonDataSourceFilter();
      }
      else if ("avro".equals(type)){
        String msgClsString = config.get("kafka.msg.avro.class");
        String dataMapperClassString = config.get("kafka.msg.avro.datamapper");
        Class cls = Class.forName(msgClsString);
        Class dataMapperClass = Class.forName(dataMapperClassString);
        DataSourceFilter dataMapper = (DataSourceFilter)dataMapperClass.newInstance();
        dataFilter = new AvroDataSourceFilter(cls, dataMapper);
      }
      else{
        throw new IllegalArgumentException("invalid msg type: "+type);
      }
    }
    
		MiKafkaStreamDataProvider provider = new MiKafkaStreamDataProvider(_versionComparator,zookeeperUrl,timeout,consumerGroupId,topic,oldSinceKey,dataFilter, rewind);
		return provider;
	}

  @Override
  public Comparator<String> getVersionComparator() {
    return _versionComparator;
  }
  
  public static class MiVersionComparator implements Comparator<String>, Serializable {
    private static final long serialVersionUID = 1L;
    private static final Pattern _numPattern = Pattern.compile("[0-9]+");

    public int compare(String version1, String version2) {
      if (version1 == version2) return 0;
      if (version1 == null) return -1;
      if (version2 == null) return 1;

      String timestamp1 = version1.split(";")[0];
      String timestamp2 = version2.split(";")[0];

      if (_numPattern.matcher(timestamp1).matches() && _numPattern.matcher(timestamp2).matches()) {
        try {
          return Long.valueOf(timestamp1).compareTo(Long.valueOf(timestamp2));
        } catch (Throwable t) {
        }
      }
      return timestamp1.compareTo(timestamp2);
    }

    public boolean equals(String s1, String s2) {
      return (compare(s1, s2) == 0);
    }
  }
}
