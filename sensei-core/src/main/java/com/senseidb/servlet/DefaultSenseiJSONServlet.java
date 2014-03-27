package com.senseidb.servlet;

import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_ERRORS;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_ERROR_CODE;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_ERROR_MESSAGE;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_ERROR_TYPE;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_FACETS;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_FACET_INFO_COUNT;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_FACET_INFO_SELECTED;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_FACET_INFO_VALUE;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_HITS;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_HITS_EXPL_DESC;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_HITS_EXPL_DETAILS;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_HITS_EXPL_VALUE;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_HIT_DOCID;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_HIT_EXPLANATION;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_HIT_GROUPFIELD;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_HIT_GROUPHITS;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_HIT_GROUPHITSCOUNT;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_HIT_GROUPVALUE;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_HIT_SCORE;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_HIT_SRC_DATA;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_HIT_STORED_FIELDS;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_HIT_STORED_FIELDS_NAME;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_HIT_STORED_FIELDS_VALUE;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_HIT_TERMVECTORS;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_HIT_UID;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_NUMGROUPS;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_NUMHITS;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_PARSEDQUERY;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_SELECT_LIST;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_TID;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_TIME;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_RESULT_TOTALDOCS;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SYSINFO_CLUSTERINFO;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SYSINFO_CLUSTERINFO_ADMINLINK;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SYSINFO_CLUSTERINFO_ID;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SYSINFO_CLUSTERINFO_NODELINK;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SYSINFO_CLUSTERINFO_PARTITIONS;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SYSINFO_FACETS;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SYSINFO_FACETS_NAME;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SYSINFO_FACETS_PROPS;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SYSINFO_FACETS_RUNTIME;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SYSINFO_LASTMODIFIED;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SYSINFO_NUMDOCS;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SYSINFO_SCHEMA;
import static com.senseidb.servlet.SenseiSearchServletParams.PARAM_SYSINFO_VERSION;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import proj.zoie.api.indexing.AbstractZoieIndexable;

import com.browseengine.bobo.api.BrowseFacet;
import com.browseengine.bobo.api.BrowseHit.BoboTerm;
import com.browseengine.bobo.api.BrowseHit.SerializableExplanation;
import com.browseengine.bobo.api.BrowseHit.SerializableField;
import com.browseengine.bobo.api.BrowseSelection;
import com.browseengine.bobo.api.FacetAccessible;
import com.browseengine.bobo.api.FacetSpec;
import com.browseengine.bobo.api.FacetSpec.FacetSortSpec;
import com.senseidb.conf.SenseiFacetHandlerBuilder;
import com.senseidb.search.req.SenseiError;
import com.senseidb.search.req.SenseiHit;
import com.senseidb.search.req.SenseiRequest;
import com.senseidb.search.req.SenseiResult;
import com.senseidb.search.req.SenseiSystemInfo;
import com.senseidb.util.JSONUtil.FastJSONArray;
import com.senseidb.util.JSONUtil.FastJSONObject;

public class DefaultSenseiJSONServlet extends AbstractSenseiRestServlet {

  private static final String PARAM_RESULT_MAP_REDUCE = "mapReduceResult";

  /**
   *
   */
  private static final long serialVersionUID = 1L;

  private static Logger logger = Logger.getLogger(DefaultSenseiJSONServlet.class);

  public static JSONObject convertExpl(SerializableExplanation expl) throws JSONException {
    JSONObject jsonObject = null;
    if (expl != null) {
      jsonObject = new FastJSONObject();
      jsonObject.put(PARAM_RESULT_HITS_EXPL_VALUE, expl.getValue());
      String descr = expl.getDescription();
      jsonObject.put(PARAM_RESULT_HITS_EXPL_DESC, descr == null ? "" : descr);
      SerializableExplanation[] details = expl.getDetails();
      if (details != null) {
        JSONArray detailArray = new FastJSONArray();
        for (SerializableExplanation detail : details) {
          JSONObject subObj = convertExpl(detail);
          if (subObj != null) {
            detailArray.put(subObj);
          }
        }
        jsonObject.put(PARAM_RESULT_HITS_EXPL_DETAILS, detailArray);
      }
    }

    return jsonObject;
  }

  public static JSONObject convert(Map<String, FacetAccessible> facetValueMap, SenseiRequest req)
      throws JSONException {
    JSONObject resMap = new FastJSONObject();
    if (facetValueMap != null) {
      Set<Entry<String, FacetAccessible>> entrySet = facetValueMap.entrySet();

      for (Entry<String, FacetAccessible> entry : entrySet) {
        String fieldname = entry.getKey();

        BrowseSelection sel = req.getSelection(fieldname);
        HashSet<String> selectedVals = new HashSet<String>();
        if (sel != null) {
          String[] vals = sel.getValues();
          if (vals != null && vals.length > 0) {
            selectedVals.addAll(Arrays.asList(vals));
          }
        }

        FacetAccessible facetAccessible = entry.getValue();
        List<BrowseFacet> facetList = facetAccessible.getFacets();

        ArrayList<JSONObject> facets = new ArrayList<JSONObject>();

        for (BrowseFacet f : facetList) {
          String fval = f.getValue();
          if (fval != null && fval.length() > 0) {
            JSONObject fv = new FastJSONObject();
            fv.put(PARAM_RESULT_FACET_INFO_COUNT, f.getFacetValueHitCount());
            fv.put(PARAM_RESULT_FACET_INFO_VALUE, fval);
            fv.put(PARAM_RESULT_FACET_INFO_SELECTED, selectedVals.remove(fval));
            facets.add(fv);
          }
        }

        if (selectedVals.size() > 0) {
          // selected vals did not make it in top n
          for (String selectedVal : selectedVals) {
            if (selectedVal != null && selectedVal.length() > 0) {
              BrowseFacet selectedFacetVal = facetAccessible.getFacet(selectedVal);
              JSONObject fv = new FastJSONObject();
              fv.put(PARAM_RESULT_FACET_INFO_COUNT,
                selectedFacetVal == null ? 0 : selectedFacetVal.getFacetValueHitCount());
              String fval = selectedFacetVal == null ? selectedVal : selectedFacetVal.getValue();
              fv.put(PARAM_RESULT_FACET_INFO_VALUE, fval);
              fv.put(PARAM_RESULT_FACET_INFO_SELECTED, true);
              facets.add(fv);
            }
          }

          // we need to sort it
          FacetSpec fspec = req.getFacetSpec(fieldname);
          assert fspec != null;
          sortFacets(fieldname, facets, fspec);
        }

        resMap.put(fieldname, facets);
      }
    }
    return resMap;
  }

  private static void sortFacets(String fieldName, ArrayList<JSONObject> facets, FacetSpec fspec) {
    FacetSortSpec sortSpec = fspec.getOrderBy();
    if (FacetSortSpec.OrderHitsDesc.equals(sortSpec)) {
      Collections.sort(facets, new Comparator<JSONObject>() {
        @Override
        public int compare(JSONObject o1, JSONObject o2) {
          try {
            int c1 = o1.getInt(PARAM_RESULT_FACET_INFO_COUNT);
            int c2 = o2.getInt(PARAM_RESULT_FACET_INFO_COUNT);
            int val = c2 - c1;
            if (val == 0) {
              String s1 = o1.getString(PARAM_RESULT_FACET_INFO_VALUE);
              String s2 = o1.getString(PARAM_RESULT_FACET_INFO_VALUE);
              val = s1.compareTo(s2);
            }
            return val;
          } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return 0;
          }
        }
      });
    } else if (FacetSortSpec.OrderValueAsc.equals(sortSpec)) {
      Collections.sort(facets, new Comparator<JSONObject>() {
        @Override
        public int compare(JSONObject o1, JSONObject o2) {
          try {
            String s1 = o1.getString(PARAM_RESULT_FACET_INFO_VALUE);
            String s2 = o1.getString(PARAM_RESULT_FACET_INFO_VALUE);
            return s1.compareTo(s2);
          } catch (Exception e) {
            logger.error(e.getMessage(), e);
            return 0;
          }
        }
      });
    } else {
      throw new IllegalStateException(fieldName + " sorting is not supported");
    }
  }

  @Override
  protected String buildResultString(HttpServletRequest httpReq, SenseiRequest req, SenseiResult res)
      throws Exception {
    return supportJsonp(httpReq, buildJSONResultString(req, res));
  }

  private String supportJsonp(HttpServletRequest httpReq, String jsonString) {
    String callback = httpReq.getParameter("callback");
    if (callback != null) {
      return callback + "(" + jsonString + ");";
    } else {
      return jsonString;
    }
  }

  public static String buildJSONResultString(SenseiRequest req, SenseiResult res) throws Exception {
    JSONObject jsonObj = buildJSONResult(req, res);
    return jsonObj.toString();
  }

  public static JSONArray buildJSONHits(SenseiRequest req, SenseiHit[] hits) throws Exception {
    Set<String> selectSet = req.getSelectSet();

    JSONArray hitArray = new FastJSONArray();
    for (SenseiHit hit : hits) {
      Map<String, String[]> fieldMap = hit.getFieldValues();

      JSONObject hitObj = new FastJSONObject();
      if (selectSet == null || selectSet.contains(PARAM_RESULT_HIT_UID)) {
        hitObj.put(PARAM_RESULT_HIT_UID, hit.getUID());
      }
      if (selectSet == null || selectSet.contains(PARAM_RESULT_HIT_DOCID)) {
        hitObj.put(PARAM_RESULT_HIT_DOCID, hit.getDocid());
      }
      if (selectSet == null || selectSet.contains(PARAM_RESULT_HIT_SCORE)) {
        hitObj.put(PARAM_RESULT_HIT_SCORE, hit.getScore());
      }
      if (selectSet == null || selectSet.contains(PARAM_RESULT_HIT_GROUPFIELD)) {
        hitObj.put(PARAM_RESULT_HIT_GROUPFIELD, hit.getGroupField());
      }
      if (selectSet == null || selectSet.contains(PARAM_RESULT_HIT_GROUPVALUE)) {
        hitObj.put(PARAM_RESULT_HIT_GROUPVALUE, hit.getGroupValue());
      }
      if (selectSet == null || selectSet.contains(PARAM_RESULT_HIT_GROUPHITSCOUNT)) {
        hitObj.put(PARAM_RESULT_HIT_GROUPHITSCOUNT, hit.getGroupHitsCount());
      }
      if (hit.getGroupHits() != null && hit.getGroupHits().length > 0) hitObj.put(
        PARAM_RESULT_HIT_GROUPHITS, buildJSONHits(req, hit.getSenseiGroupHits()));

      // get fetchStored even if request does not have it because it could be set at the
      // federated broker level
      if (selectSet == null || selectSet.contains(PARAM_RESULT_HIT_SRC_DATA)
          || req.isFetchStoredFields() || hit.getSrcData() != null) {
        hitObj.put(PARAM_RESULT_HIT_SRC_DATA, hit.getSrcData());
      }
      if (fieldMap != null) {
        Set<Entry<String, String[]>> entries = fieldMap.entrySet();
        for (Entry<String, String[]> entry : entries) {
          String key = entry.getKey();
          if (key.equals(PARAM_RESULT_HIT_UID)) {
            // UID is already set.
            continue;
          }
          if (key.equals(SenseiFacetHandlerBuilder.SUM_GROUP_BY_FACET_NAME)) {
            // UID is already set.
            continue;
          }
          String[] vals = entry.getValue();

          JSONArray valArray = new FastJSONArray();
          if (vals != null) {
            for (String val : vals) {
              valArray.put(val);
            }
          }
          if (selectSet == null || selectSet.contains(key)) {
            hitObj.put(key, valArray);
          }
        }
      }

      List<SerializableField> fields = hit.getStoredFields();
      if (fields != null) {
        List<JSONObject> storedData = new ArrayList<JSONObject>();
        for (SerializableField field : fields) {
          if (req.getStoredFieldsToFetch() != null
              && !req.getStoredFieldsToFetch().contains(field.name())) {
            continue;
          }
          // DOCUMENT_STORE_FIELD is already set to _srcdata
          if (!field.name().equals(AbstractZoieIndexable.DOCUMENT_STORE_FIELD)) {
            JSONObject data = new FastJSONObject();
            data.put(PARAM_RESULT_HIT_STORED_FIELDS_NAME, field.name());
            data.put(PARAM_RESULT_HIT_STORED_FIELDS_VALUE, field.stringValue());
            storedData.add(data);
          }
        }
        if (selectSet == null || selectSet.contains(PARAM_RESULT_HIT_STORED_FIELDS)) {
          hitObj.put(PARAM_RESULT_HIT_STORED_FIELDS, new FastJSONArray(storedData));
        }
      }

      Map<String, List<BoboTerm>> tvMap = hit.getTermVectorMap();
      if (tvMap != null && tvMap.size() > 0) {
        JSONObject tvObj = new FastJSONObject();
        if (selectSet == null || selectSet.contains(PARAM_RESULT_HIT_TERMVECTORS)) {
          hitObj.put(PARAM_RESULT_HIT_TERMVECTORS, tvObj);
        }
        Set<Entry<String, List<BoboTerm>>> entries = tvMap.entrySet();
        for (Entry<String, List<BoboTerm>> entry : entries) {
          String field = entry.getKey();
          JSONArray tvArray = new FastJSONArray();
          tvObj.put(field, tvArray);
          List<BoboTerm> boboTerms = entry.getValue();
          for (int i = 0; i < boboTerms.size(); ++i) {
            JSONObject tv = new FastJSONObject();
            tv.put("term", boboTerms.get(i).term);
            tv.put("freq", boboTerms.get(i).freq);
            tv.put("positions", boboTerms.get(i).positions);
            tv.put("startOffsets", boboTerms.get(i).startOffsets);
            tv.put("endOffsets", boboTerms.get(i).endOffsets);
            tvArray.put(tv);
          }
        }
      }

      SerializableExplanation expl = hit.getExplanation();
      if (expl != null) {
        if (selectSet == null || selectSet.contains(PARAM_RESULT_HIT_EXPLANATION)) {
          hitObj.put(PARAM_RESULT_HIT_EXPLANATION, convertExpl(expl));
        }
      }

      hitArray.put(hitObj);
    }
    return hitArray;
  }

  @SuppressWarnings("unchecked")
  public static JSONObject buildJSONResult(SenseiRequest req, SenseiResult res) throws Exception {
    JSONObject jsonObj = new FastJSONObject();
    jsonObj.put(PARAM_RESULT_TID, res.getTid());
    jsonObj.put(PARAM_RESULT_TOTALDOCS, res.getTotalDocsLong());
    jsonObj.put(PARAM_RESULT_NUMHITS, res.getNumHitsLong());
    jsonObj.put(PARAM_RESULT_NUMGROUPS, res.getNumGroupsLong());
    jsonObj.put(PARAM_RESULT_PARSEDQUERY, res.getParsedQuery());
    addErrors(jsonObj, res);
    SenseiHit[] hits = res.getSenseiHits();
    JSONArray hitArray = buildJSONHits(req, hits);
    jsonObj.put(PARAM_RESULT_HITS, hitArray);

    List<String> selectList = req.getSelectList();
    if (selectList != null) {
      JSONArray jsonSelectList = new FastJSONArray();
      for (String col : selectList) {
        jsonSelectList.put(col);
      }
      jsonObj.put(PARAM_RESULT_SELECT_LIST, jsonSelectList);
    }

    jsonObj.put(PARAM_RESULT_TIME, res.getTime());
    jsonObj.put(PARAM_RESULT_FACETS, convert(res.getFacetMap(), req));
    if (req.getMapReduceFunction() != null && res.getMapReduceResult() != null) {
      JSONObject mapReduceResult = req.getMapReduceFunction().render(
        res.getMapReduceResult().getReduceResult());
      if (!(mapReduceResult instanceof FastJSONObject) && mapReduceResult != null) {
        mapReduceResult = new FastJSONObject(mapReduceResult.toString());
      }
      jsonObj.put(PARAM_RESULT_MAP_REDUCE, mapReduceResult);
    }

    return jsonObj;
  }

  private static void addErrors(JSONObject jsonResult, SenseiResult res) throws JSONException {
    JSONArray errorsJson = new FastJSONArray();
    for (SenseiError error : res.getErrors()) {
      errorsJson.put(new FastJSONObject().put(PARAM_RESULT_ERROR_MESSAGE, error.getMessage())
          .put(PARAM_RESULT_ERROR_TYPE, error.getErrorType().name())
          .put(PARAM_RESULT_ERROR_CODE, error.getErrorCode()));
    }
    jsonResult.put(PARAM_RESULT_ERRORS, errorsJson);
    if (res.getErrors().size() > 0) {
      jsonResult.put(PARAM_RESULT_ERROR_CODE, res.getErrors().get(0).getErrorCode());
    } else {
      jsonResult.put(PARAM_RESULT_ERROR_CODE, 0);
    }
  }

  @Override
  protected String buildResultString(HttpServletRequest httpReq, SenseiSystemInfo info)
      throws Exception {
    JSONObject jsonObj = new FastJSONObject();
    jsonObj.put(PARAM_SYSINFO_NUMDOCS, info.getNumDocs());
    jsonObj.put(PARAM_SYSINFO_LASTMODIFIED, info.getLastModified());
    jsonObj.put(PARAM_SYSINFO_VERSION, info.getVersion());

    if (info.getSchema() != null && info.getSchema().length() != 0) {
      jsonObj.put(PARAM_SYSINFO_SCHEMA, new FastJSONObject(info.getSchema()));
    }

    JSONArray jsonArray = new FastJSONArray();
    jsonObj.put(PARAM_SYSINFO_FACETS, jsonArray);
    Set<SenseiSystemInfo.SenseiFacetInfo> facets = info.getFacetInfos();
    if (facets != null) {
      for (SenseiSystemInfo.SenseiFacetInfo facet : facets) {
        JSONObject facetObj = new FastJSONObject();
        facetObj.put(PARAM_SYSINFO_FACETS_NAME, facet.getName());
        facetObj.put(PARAM_SYSINFO_FACETS_RUNTIME, facet.isRunTime());
        facetObj.put(PARAM_SYSINFO_FACETS_PROPS, facet.getProps());
        jsonArray.put(facetObj);
      }
    }

    jsonArray = new FastJSONArray();
    jsonObj.put(PARAM_SYSINFO_CLUSTERINFO, jsonArray);
    List<SenseiSystemInfo.SenseiNodeInfo> clusterInfo = info.getClusterInfo();
    if (clusterInfo != null) {
      for (SenseiSystemInfo.SenseiNodeInfo nodeInfo : clusterInfo) {
        JSONObject nodeObj = new FastJSONObject();
        nodeObj.put(PARAM_SYSINFO_CLUSTERINFO_ID, nodeInfo.getId());
        nodeObj.put(PARAM_SYSINFO_CLUSTERINFO_PARTITIONS,
          new FastJSONArray(Arrays.asList(nodeInfo.getPartitions())));
        nodeObj.put(PARAM_SYSINFO_CLUSTERINFO_NODELINK, nodeInfo.getNodeLink());
        nodeObj.put(PARAM_SYSINFO_CLUSTERINFO_ADMINLINK, nodeInfo.getAdminLink());
        jsonArray.put(nodeObj);
      }
    }

    return supportJsonp(httpReq, jsonObj.toString());
  }
}
