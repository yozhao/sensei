package com.senseidb.indexing;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.log4j.Logger;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.spatial.SpatialStrategy;
import org.apache.lucene.spatial.prefix.RecursivePrefixTreeStrategy;
import org.apache.lucene.spatial.prefix.tree.GeohashPrefixTree;
import org.apache.lucene.spatial.prefix.tree.SpatialPrefixTree;
import org.json.JSONException;
import org.json.JSONObject;

import proj.zoie.api.indexing.AbstractZoieIndexable;
import proj.zoie.api.indexing.AbstractZoieIndexableInterpreter;
import proj.zoie.api.indexing.ZoieIndexable;

import com.senseidb.conf.SenseiSchema;
import com.senseidb.conf.SenseiSchema.FieldDefinition;
import com.senseidb.search.plugin.PluggableSearchEngineManager;
import com.spatial4j.core.context.SpatialContext;
import com.spatial4j.core.shape.Shape;

public class DefaultJsonSchemaInterpreter extends AbstractZoieIndexableInterpreter<JSONObject> {

  private static final Logger logger = Logger.getLogger(DefaultJsonSchemaInterpreter.class);

  private final SenseiSchema schema;
  private final Set<Entry<String, FieldDefinition>> entries;
  private final String uidField;
  private final boolean compressSrcData;

  private final Map<String, JsonValExtractor> dateExtractorMap;

  private JsonFilter jsonFilter = null;

  private CustomIndexingPipeline customIndexingPipeline = null;

  private final Set<String> nonLuceneFields = new HashSet<String>();

  private SpatialStrategy spatialStrategy = null;
  private String longitudeFieldName = null;
  private String latitudeFieldName = null;

  public DefaultJsonSchemaInterpreter(SenseiSchema schema) throws ConfigurationException {
    this(schema, null);
  }

  public DefaultJsonSchemaInterpreter(SenseiSchema schema,
      PluggableSearchEngineManager pluggableSearchEngineManager) throws ConfigurationException {
    this.schema = schema;
    if (pluggableSearchEngineManager != null) {
      nonLuceneFields.addAll(pluggableSearchEngineManager.getFieldNames());
    }
    entries = this.schema.getFieldDefMap().entrySet();
    uidField = this.schema.getUidField();
    compressSrcData = this.schema.isCompressSrcData();
    dateExtractorMap = new HashMap<String, JsonValExtractor>();
    for (Entry<String, FieldDefinition> entry : entries) {
      final FieldDefinition def = entry.getValue();
      if (Date.class.equals(def.type)) {
        dateExtractorMap.put(entry.getKey(), new JsonValExtractor() {

          @Override
          public Object extract(String val) {
            try {
              return ((SimpleDateFormat) (def.formatter)).parse(val);
            } catch (Exception e) {
              throw new RuntimeException(e.getMessage(), e);
            }
          }

        });
      }
    }
    if (schema.getSpatialDefinition() != null) {
      SpatialPrefixTree grid = new GeohashPrefixTree(SpatialContext.GEO,
          schema.getSpatialDefinition().spatialPrefixTreeMaxLevels);
      spatialStrategy = new RecursivePrefixTreeStrategy(grid,
          schema.getSpatialDefinition().fieldName);
      longitudeFieldName = schema.getSpatialDefinition().longitude;
      latitudeFieldName = schema.getSpatialDefinition().latitude;
      logger
          .info(String
              .format(
                "Spatial parameters fieldName: %s, longitudeFieldName: %s, latitudeFieldName: %s spatialPrefixTreeMaxLevels: %d",
                schema.getSpatialDefinition().fieldName, longitudeFieldName, latitudeFieldName,
                schema.getSpatialDefinition().spatialPrefixTreeMaxLevels));
    }
  }

  private static interface JsonValExtractor {
    Object extract(String val);
  }

  private final static Map<Class<?>, JsonValExtractor> ExtractorMap = new HashMap<Class<?>, JsonValExtractor>();

  static {
    ExtractorMap.put(int.class, new JsonValExtractor() {

      @Override
      public Object extract(String val) {
        if (val == null || val.length() == 0) {
          return 0;
        } else {
          int num = Integer.parseInt(val);
          return num;
        }
      }

    });
    ExtractorMap.put(double.class, new JsonValExtractor() {

      @Override
      public Object extract(String val) {

        if (val == null || val.length() == 0) {
          return 0.0;
        } else {
          double num = Double.parseDouble(val);
          return num;
        }
      }

    });
    ExtractorMap.put(long.class, new JsonValExtractor() {

      @Override
      public Object extract(String val) {
        if (val == null || val.length() == 0) {
          return 0.0;
        } else {
          long num = Long.parseLong(val);
          return num;
        }
      }

    });
    ExtractorMap.put(String.class, new JsonValExtractor() {

      @Override
      public Object extract(String val) {
        return val;
      }

    });

  }

  public static byte[] compress(byte[] src) throws Exception {
    byte[] data = null;
    if (src != null) {
      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      GZIPOutputStream gzipStream = new GZIPOutputStream(bout);

      gzipStream.write(src);
      gzipStream.flush();
      gzipStream.close();
      bout.flush();

      data = bout.toByteArray();
    }

    return data;
  }

  public static byte[] decompress(byte[] src) throws Exception {
    byte[] data = null;
    if (src != null) {
      ByteArrayOutputStream bout = new ByteArrayOutputStream();
      byte[] buf = new byte[1024]; // 1k buffer
      ByteArrayInputStream bin = new ByteArrayInputStream(src);
      GZIPInputStream gzipStream = new GZIPInputStream(bin);

      int len;
      while ((len = gzipStream.read(buf)) > 0) {
        bout.write(buf, 0, len);
      }
      bout.flush();

      data = bout.toByteArray();
    }

    return data;
  }

  public void setCustomIndexingPipeline(CustomIndexingPipeline customIndexingPipeline) {
    this.customIndexingPipeline = customIndexingPipeline;
  }

  public CustomIndexingPipeline getCustomIndexingPipeline() {
    return customIndexingPipeline;
  }

  public void setJsonFilter(JsonFilter jsonFilter) {
    this.jsonFilter = jsonFilter;
  }

  public static List<String> tokenize(String val, String delim) {
    List<String> result = new ArrayList<String>();

    if (val == null || val.length() == 0) return result;

    if (delim == null || delim.length() == 0) result.add(val);
    else if (delim.length() == 1) {
      char de = delim.charAt(0);
      StringBuilder sb = new StringBuilder();
      boolean escape = false;
      for (char c : val.toCharArray()) {
        if (escape) {
          if (c == '\\' || c == de) sb.append(c);
          else sb.append('\\').append(c);

          escape = false;
        } else {
          if (c == '\\') {
            escape = true;
            continue;
          } else if (c == de) {
            if (sb.length() > 0) {
              result.add(sb.toString());
              sb.setLength(0);
            }
          } else sb.append(c);
        }
      }
      if (escape) sb.append('\\');
      if (sb.length() > 0) result.add(sb.toString());
    } else {
      StringTokenizer strtok = new StringTokenizer(val, delim);
      while (strtok.hasMoreTokens()) {
        result.add(strtok.nextToken());
      }
    }

    return result;
  }

  @Override
  public ZoieIndexable convertAndInterpret(JSONObject obj) {
    final JSONObject src = obj;
    final JSONObject filtered;
    if (jsonFilter != null) {
      try {
        filtered = jsonFilter.filter(src);
      } catch (Exception e) {
        throw new RuntimeException(e.getMessage(), e);
      }
    } else {
      filtered = src;
    }

    return new AbstractZoieIndexable() {

      @Override
      public IndexingReq[] buildIndexingReqs() {

        org.apache.lucene.document.Document luceneDoc = new org.apache.lucene.document.Document();
        for (Entry<String, FieldDefinition> entry : entries) {
          String name = entry.getKey();
          try {
            final FieldDefinition fldDef = entry.getValue();
            if (nonLuceneFields.contains(entry.getKey())) {
              continue;
            }
            if (fldDef.isMeta) {
              JsonValExtractor extractor = ExtractorMap.get(fldDef.type);
              if (extractor == null) {
                if (Date.class.equals(fldDef.type)) {
                  extractor = dateExtractorMap.get(name);
                } else {
                  extractor = ExtractorMap.get(String.class);
                }
              }

              if (filtered.has(fldDef.fromField)) {
                List<Object> vals = new LinkedList<Object>();
                if (filtered.isNull(fldDef.fromField)) continue;
                if (fldDef.isMulti) {
                  String val = filtered.optString(fldDef.fromField);
                  for (String token : tokenize(val, fldDef.delim)) {
                    Object obj = extractor.extract(token);
                    if (obj != null) {
                      vals.add(obj);
                    }
                  }
                } else {
                  String val = filtered.optString(fldDef.fromField);
                  if (val == null) continue;
                  Object obj = extractor.extract(filtered.optString(fldDef.fromField));
                  if (obj != null) {
                    vals.add(obj);
                  }
                }

                for (Object val : vals) {
                  if (val == null) continue;
                  String strVal = null;
                  if (fldDef.formatter != null) {
                    strVal = fldDef.formatter.format(val);
                  } else {
                    strVal = String.valueOf(val);
                  }
                  Field metaField = new StringField(name, strVal, fldDef.store);
                  luceneDoc.add(metaField);
                }
              }
            } else {
              Field textField = new Field(name, filtered.optString(fldDef.fromField),
                  fldDef.fieldType);
              luceneDoc.add(textField);
            }
          } catch (Exception e) {
            logger.error("Problem extracting data for field: " + name, e);
            throw new RuntimeException(e);
          }
        }

        if (spatialStrategy != null) {
          try {
            if (filtered.has(longitudeFieldName) && filtered.has(latitudeFieldName)) {
              double longitude = filtered.getDouble(longitudeFieldName);
              double latitude = filtered.getDouble(latitudeFieldName);
              Shape shape = SpatialContext.GEO.makePoint(longitude, latitude);
              for (IndexableField field : spatialStrategy.createIndexableFields(shape)) {
                luceneDoc.add(field);
              }
            }
          } catch (JSONException e) {
            logger.error("Get longitude or latitude failed", e);
          }
        }

        if (customIndexingPipeline != null) {
          customIndexingPipeline.applyCustomization(luceneDoc, schema, filtered);
        }
        return new IndexingReq[] { new IndexingReq(luceneDoc) };
      }

      @Override
      public long getUID() {
        try {
          return Long.parseLong(filtered.getString(uidField));
        } catch (JSONException e) {
          throw new IllegalStateException(e.getMessage(), e);
        }
      }

      @Override
      public boolean isDeleted() {
        String type = filtered.optString(SenseiSchema.EVENT_TYPE_FIELD, null);
        return SenseiSchema.EVENT_TYPE_DELETE.equalsIgnoreCase(type);
      }

      @Override
      public boolean isSkip() {
        String type = filtered.optString(SenseiSchema.EVENT_TYPE_FIELD, null);
        return SenseiSchema.EVENT_TYPE_SKIP.equalsIgnoreCase(type);
      }

      @Override
      public byte[] getStoreValue() {
        byte[] data = null;
        if (src != null) {
          Object type = src.remove(SenseiSchema.EVENT_TYPE_FIELD);
          try {
            String srcData = src.optString(schema.getSrcDataField(), null);
            if (srcData == null) {
              srcData = src.toString();
            }
            if (compressSrcData) {
              data = compress(srcData.getBytes("UTF-8"));
            } else {
              data = srcData.getBytes("UTF-8");
            }
          } catch (Exception e) {
            logger.error(e.getMessage(), e);
          }

          if (type != null) {
            try {
              src.put(SenseiSchema.EVENT_TYPE_FIELD, type);
            } catch (Exception e) {
              logger.error("Should never happen", e);
            }
          }
        }

        return data;
      }

      @Override
      public boolean isStorable() {
        return true;
      }

    };
  }

}
