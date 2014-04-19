package com.senseidb.gateway.kafka;

import java.util.Comparator;
import java.util.Set;

import org.json.JSONObject;

import proj.zoie.impl.indexing.StreamDataProvider;
import proj.zoie.impl.indexing.ZoieConfig;

import com.senseidb.gateway.SenseiGateway;
import com.senseidb.gateway.kafka.persistent.PersistentCacheManager;
import com.senseidb.gateway.kafka.persistent.PersistentKafkaStreamDataProvider;
import com.senseidb.indexing.DataSourceFilter;
import com.senseidb.indexing.ShardingStrategy;

public class KafkaDataProviderBuilder extends SenseiGateway<DataPacket> {

  private final Comparator<String> _versionComparator = ZoieConfig.DEFAULT_VERSION_COMPARATOR;

  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  public StreamDataProvider<JSONObject> buildDataProvider(DataSourceFilter<DataPacket> dataFilter,
      String oldSinceKey, ShardingStrategy shardingStrategy, Set<Integer> partitions)
      throws Exception {

    long offset = oldSinceKey == null ? 0L : Long.parseLong(oldSinceKey);

    if (dataFilter == null) {
      String type = config.get("kafka.msg.type");
      if (type == null) {
        type = "json";
      }

      if ("json".equals(type)) {
        dataFilter = new DefaultJsonDataSourceFilter();
      } else if ("avro".equals(type)) {
        String msgClsString = config.get("kafka.msg.avro.class");
        String dataMapperClassString = config.get("kafka.msg.avro.datamapper");
        Class cls = Class.forName(msgClsString);
        Class dataMapperClass = Class.forName(dataMapperClassString);
        DataSourceFilter dataMapper = (DataSourceFilter) dataMapperClass.newInstance();
        dataFilter = new AvroDataSourceFilter(cls, dataMapper);
      } else {
        throw new IllegalArgumentException("Invalid msg type: " + type);
      }
    }
    String persistentManagerName = config.get("kafka.persistentManager");
    if (persistentManagerName == null) {
      KafkaStreamDataProvider provider = new KafkaStreamDataProvider(_versionComparator, config,
          dataFilter);
      return provider;
    } else {
      PersistentKafkaStreamDataProvider wrappedKafkaStreamProvider = new PersistentKafkaStreamDataProvider(
          _versionComparator, config, offset, dataFilter, pluginRegistry.getBeanByName(
            persistentManagerName, PersistentCacheManager.class));
      return wrappedKafkaStreamProvider;
    }
  }

  @Override
  public Comparator<String> getVersionComparator() {
    return _versionComparator;
  }
}
