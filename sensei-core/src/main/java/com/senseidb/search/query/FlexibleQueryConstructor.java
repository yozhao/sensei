package com.senseidb.search.query;

import com.google.common.base.Strings;

import com.senseidb.extention.flexiblequery.CustomMatchingStrategy;
import com.senseidb.extention.flexiblequery.CustomMatchingStrategyBuilder;
import com.senseidb.extention.flexiblequery.FlexibleQuery;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.search.Query;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class FlexibleQueryConstructor extends QueryConstructor {
  public static final String QUERY_TYPE = "flexible";
  private static final String FIELD_PARAM = "field";
  private static final String MODEL_PARAM = "model";
  private static final String NAME_PARAM = "name";
  private static final String PARAMS_PARAM = "params";
  private static final String EXPLAIN_PARAM = "explain";
  private static final String OVERWRITE_PARAM = "overwrite";
  private Analyzer _analyzer;

  // "flexible" : {
  // "fields" [ {"field": "a", "query":"test1 test2 test3", "boost":1}, {"field": "b", "boost":0.7]
  // "query" : "test1 test2 test3",
  // "model" : {
  //     "name" : "model1",
  //     "function" : "for (int i = 0; i < getFieldLength(); ++i) {}",
  // },

  public FlexibleQueryConstructor(Analyzer analyzer) {
    _analyzer = analyzer;
  }

  @Override
  protected Query doConstructQuery(JSONObject jsonQuery) throws JSONException, IOException {
    try {
      JSONArray fields = jsonQuery.optJSONArray(FIELDS_PARAM);
      String text = jsonQuery.optString(QUERY_PARAM);
      String boostStr = jsonQuery.optString(BOOST_PARAM);
      JSONObject modelObj = jsonQuery.optJSONObject(MODEL_PARAM);
      String name = modelObj.optString(NAME_PARAM);
      String function = modelObj.optString(FUNCTION_NAME);
      String explain = modelObj.optString(EXPLAIN_PARAM);

      List<CustomMatchingStrategyBuilder.FieldInfo> paramsFields = new ArrayList<CustomMatchingStrategyBuilder.FieldInfo>();
      JSONObject params = modelObj.optJSONObject(PARAMS_PARAM);
      if (params != null) {
        Iterator<String> iter = params.keys();
        while (iter.hasNext()) {
          String key = iter.next();
          paramsFields.add(new CustomMatchingStrategyBuilder.FieldInfo(key, params.getString(key)));
        }
      }

      List<FlexibleQuery.FlexibleField> fieldsList = new ArrayList<FlexibleQuery.FlexibleField>();
      if (fields != null && fields.length() > 0) {
        for (int i = 0; i < fields.length(); ++i) {
          FlexibleQuery.FlexibleField flexibleField = new FlexibleQuery.FlexibleField();
          JSONObject fieldObj = fields.getJSONObject(i);
          flexibleField.field = fieldObj.optString(FIELD_PARAM);
          if (fieldObj.has(BOOST_PARAM)) {
            flexibleField.boost = (float) fieldObj.getDouble(BOOST_PARAM);
          }
          if (fieldObj.has(QUERY_PARAM)) {
            flexibleField.content = fieldObj.optString(QUERY_PARAM);
          }
          fieldsList.add(flexibleField);
        }
      }
      CustomMatchingStrategy strategy = null;
      try {
        strategy = CustomMatchingStrategyBuilder.buildStragegy(name, function, explain, paramsFields);
      } catch (Exception e) {
        throw new IOException(e);
      }
      Query query = new FlexibleQuery(fieldsList, text, _analyzer, strategy);
      if (!Strings.isNullOrEmpty(boostStr)) {
        query.setBoost(Float.parseFloat(boostStr));
      }
      return query;
    } catch (JSONException e) {
      throw e;
    } catch (IOException e) {
      throw e;
    }
  }
}
