package com.senseidb.plugin;

import java.util.Map;

import org.wltea.analyzer.lucene.IKAnalyzer;

public class IKAnalyzerSenseiPluginFactory implements SenseiPluginFactory<IKAnalyzer> {
  @Override
  public IKAnalyzer getBean(Map<String, String> initProperties, String fullPrefix,
      SenseiPluginRegistry pluginRegistry) {
    return new IKAnalyzer();
  }
}
