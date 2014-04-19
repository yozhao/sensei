package com.senseidb.gateway.kafka;

import java.util.Comparator;
import java.util.Set;

import org.json.JSONObject;

import proj.zoie.impl.indexing.StreamDataProvider;

import com.senseidb.gateway.SenseiGateway;
import com.senseidb.indexing.DataSourceFilter;
import com.senseidb.indexing.ShardingStrategy;

public class SimpleKafkaGateway extends SenseiGateway<DataPacket> {
  @Override
  public StreamDataProvider<JSONObject> buildDataProvider(DataSourceFilter<DataPacket> dataFilter,
      String oldSinceKey, ShardingStrategy shardingStrategy, Set<Integer> partitions)
      throws Exception {
    long offset = oldSinceKey == null ? 0L : Long.parseLong(oldSinceKey);
    SimpleKafkaStreamDataProvider provider = new SimpleKafkaStreamDataProvider(
        KafkaDataProviderBuilder.DEFAULT_VERSION_COMPARATOR, config, offset, dataFilter);
    return provider;
  }

  @Override
  public Comparator<String> getVersionComparator() {
    return KafkaDataProviderBuilder.DEFAULT_VERSION_COMPARATOR;
  }
}
