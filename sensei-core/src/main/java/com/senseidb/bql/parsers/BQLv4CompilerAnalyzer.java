package com.senseidb.bql.parsers;

import com.senseidb.search.req.BQLParserUtils;
import com.senseidb.util.JSONUtil.FastJSONArray;
import com.senseidb.util.JSONUtil.FastJSONObject;
import com.senseidb.util.Pair;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeProperty;
import org.antlr.v4.runtime.tree.TerminalNode;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

/**
 *
 * @author Sam Harwell
 */
public class BQLv4CompilerAnalyzer extends BQLv4BaseListener {

    private static final int DEFAULT_REQUEST_OFFSET = 0;
    private static final int DEFAULT_REQUEST_COUNT = 10;
    private static final int DEFAULT_REQUEST_MAX_PER_GROUP = 10;
    private static final int DEFAULT_FACET_MINHIT = 1;
    private static final int DEFAULT_FACET_MAXHIT = 10;
    private static final Map<String, String> _fastutilTypeMap;
    private static final Map<String, String> _internalVarMap;
    private static final Map<String, String> _internalStaticVarMap;
    private static final Set<String> _supportedClasses;
    private static Map<String, Set<String>> _compatibleFacetTypes;

    private long _now;
    private HashSet<String> _variables;

    private SimpleDateFormat[] _format1 = new SimpleDateFormat[2];
    private SimpleDateFormat[] _format2 = new SimpleDateFormat[2];

    private LinkedList<Map<String, String>> _symbolTable;
    private Map<String, String> _currentScope;
    private Set<String> _usedFacets; // Facets used by relevance model
    private Set<String> _usedInternalVars; // Internal variables used by relevance model

    private final Map<String, String[]> _facetInfoMap;

    private final ParseTreeProperty<Object> jsonProperty = new ParseTreeProperty<Object>();
    private final ParseTreeProperty<Boolean> fetchStoredProperty = new ParseTreeProperty<Boolean>();
    private final ParseTreeProperty<List<Pair<String, String>>> aggregationFunctionsProperty = new ParseTreeProperty<List<Pair<String, String>>>();
    private final ParseTreeProperty<String> functionProperty = new ParseTreeProperty<String>();
    private final ParseTreeProperty<String> columnProperty = new ParseTreeProperty<String>();
    private final ParseTreeProperty<String> textProperty = new ParseTreeProperty<String>();
    private final ParseTreeProperty<Boolean> isRelevanceProperty = new ParseTreeProperty<Boolean>();
    private final ParseTreeProperty<Integer> offsetProperty = new ParseTreeProperty<Integer>();
    private final ParseTreeProperty<Integer> countProperty = new ParseTreeProperty<Integer>();
    private final ParseTreeProperty<String> functionNameProperty = new ParseTreeProperty<String>();
    private final ParseTreeProperty<JSONObject> propertiesProperty = new ParseTreeProperty<JSONObject>();
    private final ParseTreeProperty<JSONObject> specProperty = new ParseTreeProperty<JSONObject>();
    private final ParseTreeProperty<Object> valProperty = new ParseTreeProperty<Object>();
    private final ParseTreeProperty<String> keyProperty = new ParseTreeProperty<String>();
    private final ParseTreeProperty<String> typeNameProperty = new ParseTreeProperty<String>();
    private final ParseTreeProperty<String> varNameProperty = new ParseTreeProperty<String>();
    private final ParseTreeProperty<String> typeArgsProperty = new ParseTreeProperty<String>();
    private final ParseTreeProperty<String> facetProperty = new ParseTreeProperty<String>();
    private final ParseTreeProperty<JSONObject> paramProperty = new ParseTreeProperty<JSONObject>();
    private final ParseTreeProperty<String> paramTypeProperty = new ParseTreeProperty<String>();
    private final ParseTreeProperty<String> functionBodyProperty = new ParseTreeProperty<String>();

    static {
        _fastutilTypeMap = new HashMap<String, String>();
        _fastutilTypeMap.put("IntOpenHashSet", "set_int");
        _fastutilTypeMap.put("FloatOpenHashSet", "set_float");
        _fastutilTypeMap.put("DoubleOpenHashSet", "set_double");
        _fastutilTypeMap.put("LongOpenHashSet", "set_long");
        _fastutilTypeMap.put("ObjectOpenHashSet", "set_string");

        _fastutilTypeMap.put("Int2IntOpenHashMap", "map_int_int");
        _fastutilTypeMap.put("Int2FloatOpenHashMap", "map_int_float");
        _fastutilTypeMap.put("Int2DoubleOpenHashMap", "map_int_double");
        _fastutilTypeMap.put("Int2LongOpenHashMap", "map_int_long");
        _fastutilTypeMap.put("Int2ObjectOpenHashMap", "map_int_string");

        _fastutilTypeMap.put("Object2IntOpenHashMap", "map_string_int");
        _fastutilTypeMap.put("Object2FloatOpenHashMap", "map_string_float");
        _fastutilTypeMap.put("Object2DoubleOpenHashMap", "map_string_double");
        _fastutilTypeMap.put("Object2LongOpenHashMap", "map_string_long");
        _fastutilTypeMap.put("Object2ObjectOpenHashMap", "map_string_string");

        _internalVarMap = new HashMap<String, String>();
        _internalVarMap.put("_NOW", "long");
        _internalVarMap.put("_INNER_SCORE", "float");
        _internalVarMap.put("_RANDOM", "java.util.Random");

        _internalStaticVarMap = new HashMap<String, String>();
        _internalStaticVarMap.put("_RANDOM", "java.util.Random");

        _supportedClasses = new HashSet<String>();
        _supportedClasses.add("Boolean");
        _supportedClasses.add("Byte");
        _supportedClasses.add("Character");
        _supportedClasses.add("Double");
        _supportedClasses.add("Integer");
        _supportedClasses.add("Long");
        _supportedClasses.add("Short");

        _supportedClasses.add("Math");
        _supportedClasses.add("String");
        _supportedClasses.add("System");

        _compatibleFacetTypes = new HashMap<String, Set<String>>();
        _compatibleFacetTypes.put("range", new HashSet<String>(Arrays.asList(new String[]
                                                               {
                                                                   "simple",
                                                                   "multi"
                                                               })));
    }

    public BQLv4CompilerAnalyzer(Map<String, String[]> facetInfoMap) {
        _facetInfoMap = facetInfoMap;
        _facetInfoMap.put("_uid", new String[]{"simple", "long"});
    }

    private String predType(JSONObject pred) {
        return (String)pred.keys().next();
    }

    private String predField(JSONObject pred) throws JSONException {
        String type = (String)pred.keys().next();
        JSONObject fieldSpec = pred.getJSONObject(type);
        return (String)fieldSpec.keys().next();
    }

    private boolean verifyFacetType(final String field, final String expectedType) {
        String[] facetInfo = _facetInfoMap.get(field);
        if (facetInfo != null) {
            Set<String> compatibleTypes = _compatibleFacetTypes.get(expectedType);
            return (expectedType.equals(facetInfo[0]) ||
                "custom".equals(facetInfo[0]) ||
                (compatibleTypes != null && compatibleTypes.contains(facetInfo[0])));
        } else {
            return true;
        }
    }

    private boolean verifyValueType(Object value, final String columnType) {
        if (value instanceof String &&
            !((String)value).isEmpty() &&
            ((String)value).matches("\\$[^$].*")) {
            // This "value" is a variable, return true always
            return true;
        }

        if (columnType.equals("long") || columnType.equals("aint") || columnType.equals("int") || columnType.equals("short")) {
            return !(value instanceof Float || value instanceof String || value instanceof Boolean);
        } else if (columnType.equals("float") || columnType.equals("int") || columnType.equals("double")) {
            return !(value instanceof String || value instanceof Boolean);
        } else if (columnType.equals("string") || columnType.equals("char")) {
            return (value instanceof String);
        } else if (columnType.equals("boolean")) {
            return (value instanceof Boolean);
        } else if (columnType.isEmpty()) {
            // For a custom facet, the data type is unknown (empty
            // string).  We accept all value types here.
            return true;
        } else {
            return false;
        }
    }

    private boolean verifyFieldDataType(final String field, Object value) {
        String[] facetInfo = _facetInfoMap.get(field);

        if (value instanceof String &&
            !((String)value).isEmpty() &&
            ((String)value).matches("\\$[^$].*")) {
            // This "value" is a variable, return true always
            return true;
        } else if (value instanceof JSONArray) {
            try {
                if (facetInfo != null) {
                    String columnType = facetInfo[1];
                    for (int i = 0; i < ((JSONArray)value).length(); ++i) {
                        if (!verifyValueType(((JSONArray)value).get(i), columnType)) {
                            return false;
                        }
                    }
                }
                return true;
            } catch (JSONException err) {
                throw new IllegalStateException("JSONException: " + err.getMessage());
            }
        } else {
            if (facetInfo != null) {
                return verifyValueType(value, facetInfo[1]);
            } else {
                // Field is not a facet
                return true;
            }
        }
    }

    private boolean verifyFieldDataType(final String field, Object[] values) {
        String[] facetInfo = _facetInfoMap.get(field);
        if (facetInfo != null) {
            String columnType = facetInfo[1];
            for (Object value : values) {
                if (!verifyValueType(value, columnType)) {
                    return false;
                }
            }
        }
        return true;
    }

    private void extractSelectionInfo(JSONObject where,
                                      JSONArray selections,
                                      JSONObject filter,
                                      JSONObject query) throws JSONException {

        JSONObject queryPred = where.optJSONObject("query");
        JSONArray andPreds = null;

        if (queryPred != null) {
            query.put("query", queryPred);
        } else if ((andPreds = where.optJSONArray("and")) != null) {
            JSONArray filter_list = new FastJSONArray();
            for (int i = 0; i < andPreds.length(); ++i) {
                JSONObject pred = andPreds.getJSONObject(i);
                queryPred = pred.optJSONObject("query");
                if (queryPred != null) {
                    if (!query.has("query")) {
                        query.put("query", queryPred);
                    } else {
                        filter_list.put(pred);
                    }
                } else if (pred.has("and") || pred.has("or") || pred.has("isNull")) {
                    filter_list.put(pred);
                } else {
                    String[] facetInfo = _facetInfoMap.get(predField(pred));
                    if (facetInfo != null) {
                        if ("range".equals(predType(pred)) && !"range".equals(facetInfo[0])) {
                            filter_list.put(pred);
                        } else {
                            selections.put(pred);
                        }
                    } else {
                        filter_list.put(pred);
                    }
                }
            }
            if (filter_list.length() > 1) {
                filter.put("filter", new FastJSONObject().put("and", filter_list));
            } else if (filter_list.length() == 1) {
                filter.put("filter", filter_list.get(0));
            }
        } else if (where.has("or") || where.has("isNull")) {
            filter.put("filter", where);
        } else {
            String[] facetInfo = _facetInfoMap.get(predField(where));
            if (facetInfo != null) {
                if ("range".equals(predType(where)) && !"range".equals(facetInfo[0])) {
                    filter.put("filter", where);
                } else {
                    selections.put(where);
                }
            } else {
                filter.put("filter", where);
            }
        }
    }

    private int compareValues(Object v1, Object v2) {
        if (v1 instanceof String) {
            return ((String)v1).compareTo((String)v2);
        } else if (v1 instanceof Integer) {
            return ((Integer)v1).compareTo((Integer)v2);
        } else if (v1 instanceof Long) {
            return ((Long)v1).compareTo((Long)v2);
        } else if (v1 instanceof Float) {
            return ((Float)v1).compareTo((Float)v2);
        }
        return 0;
    }

    private Object[] getMax(Object value1, boolean include1, Object value2, boolean include2) {
        Object value;
        Boolean include;
        if (value1 == null) {
            value = value2;
            include = include2;
        } else if (value2 == null) {
            value = value1;
            include = include1;
        } else {
            int comp = compareValues(value1, value2);
            if (comp > 0) {
                value = value1;
                include = include1;
            } else if (comp == 0) {
                value = value1;
                include = (include1 && include2);
            } else {
                value = value2;
                include = include2;
            }
        }
        return new Object[]{value, include};
    }

    private Object[] getMin(Object value1, boolean include1, Object value2, boolean include2) {
        Object value;
        Boolean include;
        if (value1 == null) {
            value = value2;
            include = include2;
        } else if (value2 == null) {
            value = value1;
            include = include1;
        } else {
            int comp = compareValues(value1, value2);
            if (comp > 0) {
                value = value2;
                include = include2;
            } else if (comp == 0) {
                value = value1;
                include = (include1 && include2);
            } else {
                value = value1;
                include = include1;
            }
        }
        return new Object[]{value, include};
    }

    private void accumulateRangePred(JSONObject fieldMap, JSONObject pred)
        throws JSONException {
        String field = predField(pred);
        if (!fieldMap.has(field)) {
            fieldMap.put(field, pred);
            return;
        }
        JSONObject oldRange = (JSONObject)fieldMap.get(field);
        JSONObject oldSpec = (JSONObject)((JSONObject)oldRange.get("range")).get(field);
        Object oldFrom = oldSpec.opt("from");
        Object oldTo = oldSpec.opt("to");
        Boolean oldIncludeLower = oldSpec.optBoolean("include_lower", false);
        Boolean oldIncludeUpper = oldSpec.optBoolean("include_upper", false);

        JSONObject curSpec = (JSONObject)((JSONObject)pred.get("range")).get(field);
        Object curFrom = curSpec.opt("from");
        Object curTo = curSpec.opt("to");
        Boolean curIncludeLower = curSpec.optBoolean("include_lower", false);
        Boolean curIncludeUpper = curSpec.optBoolean("include_upper", false);

        Object[] result = getMax(oldFrom, oldIncludeLower, curFrom, curIncludeLower);
        Object newFrom = result[0];
        Boolean newIncludeLower = (Boolean)result[1];
        result = getMin(oldTo, oldIncludeUpper, curTo, curIncludeUpper);
        Object newTo = result[0];
        Boolean newIncludeUpper = (Boolean)result[1];

        if (newFrom != null && newTo != null && !newFrom.toString().startsWith("$") && !newTo.toString().startsWith("$")) {
            if (compareValues(newFrom, newTo) > 0 ||
                (compareValues(newFrom, newTo) == 0) && (!newIncludeLower || !newIncludeUpper)) {
                // This error is in general detected late, so the token
                // can be a little off, but hopefully the col index info
                // is good enough.
                throw new IllegalStateException("Inconsistent ranges detected for column: " + field);
            }
        }

        JSONObject newSpec = new FastJSONObject();
        if (newFrom != null) {
            newSpec.put("from", newFrom);
            newSpec.put("include_lower", newIncludeLower);
        }
        if (newTo != null) {
            newSpec.put("to", newTo);
            newSpec.put("include_upper", newIncludeUpper);
        }

        fieldMap.put(field, new FastJSONObject().put("range",
                                                     new FastJSONObject().put(field, newSpec)));
    }

    private void processRelevanceModelParam(JSONObject json,
                                            Set<String> params,
                                            String typeName,
                                            final String varName)
        throws JSONException {
        if (_facetInfoMap.containsKey(varName)) {
            throw new IllegalStateException("Facet name \"" + varName + "\" cannot be used as a relevance model parameter.");
        }

        if (_internalVarMap.containsKey(varName)) {
            throw new IllegalStateException("Internal variable \"" + varName + "\" cannot be used as a relevance model parameter.");
        }

        if (params.contains(varName)) {
            throw new IllegalStateException("Parameter name \"" + varName + "\" has already been used.");
        }

        if ("String".equals(typeName)) {
            typeName = "string";
        }

        JSONArray funcParams = json.optJSONArray("function_params");
        if (funcParams == null) {
            funcParams = new FastJSONArray();
            json.put("function_params", funcParams);
        }

        funcParams.put(varName);
        params.add(varName);

        JSONObject variables = json.optJSONObject("variables");
        if (variables == null) {
            variables = new FastJSONObject();
            json.put("variables", variables);
        }

        JSONArray varsWithSameType = variables.optJSONArray(typeName);
        if (varsWithSameType == null) {
            varsWithSameType = new FastJSONArray();
            variables.put(typeName, varsWithSameType);
        }
        varsWithSameType.put(varName);
    }

    // Check whether a variable is defined.
    private boolean verifyVariable(final String variable) {
        Iterator<Map<String, String>> itr = _symbolTable.descendingIterator();
        while (itr.hasNext()) {
            Map<String, String> scope = itr.next();
            if (scope.containsKey(variable)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void exitStatement(BQLv4Parser.StatementContext ctx) {
        if (ctx.select_stmt() != null) {
            jsonProperty.put(ctx, jsonProperty.get(ctx.select_stmt()));
        }
    }

    @Override
    public void enterSelect_stmt(BQLv4Parser.Select_stmtContext ctx) {
        _now = System.currentTimeMillis();
        _variables = new HashSet<String>();
    }

    @Override
    public void exitSelect_stmt(BQLv4Parser.Select_stmtContext ctx) {
        if (ctx.order_by_clause().size() > 1) {
            throw new IllegalStateException("ORDER BY clause can only appear once.");
        }

        if (ctx.limit_clause().size() > 1) {
            throw new IllegalStateException("LIMIT clause can only appear once.");
        }

        if (ctx.group_by_clause().size() > 1) {
            throw new IllegalStateException("GROUP BY clause can only appear once.");
        }

        if (ctx.distinct_clause().size() > 1) {
            throw new IllegalStateException("DISTINCT clause can only appear once.");
        }

        if (ctx.execute_clause().size() > 1) {
            throw new IllegalStateException("EXECUTE clause can only appear once.");
        }

        if (ctx.browse_by_clause().size() > 1) {
            throw new IllegalStateException("BROWSE BY clause can only appear once.");
        }

        if (ctx.fetching_stored_clause().size() > 1) {
            throw new IllegalStateException("FETCHING STORED clause can only appear once.");
        }

        if (ctx.route_by_clause().size() > 1) {
            throw new IllegalStateException("ROUTE BY clause can only appear once.");
        }

        if (ctx.relevance_model_clause().size() > 1) {
            throw new IllegalStateException("USING RELEVANCE MODEL clause can only appear once.");
        }

        JSONObject jsonObj = new FastJSONObject();
        JSONArray selections = new FastJSONArray();
        JSONObject filter = new FastJSONObject();
        JSONObject query = new FastJSONObject();

        try {
            JSONObject metaData = new FastJSONObject();
            if (ctx.cols == null) {
                metaData.put("select_list", new FastJSONArray().put("*"));
            } else {
                metaData.put("select_list", jsonProperty.get(ctx.cols));
                if (fetchStoredProperty.get(ctx.cols)) {
                    jsonObj.put("fetchStored", true);
                }
            }

            if (_variables.size() > 0) {
                metaData.put("variables", new FastJSONArray(_variables));
            }

            jsonObj.put("meta", metaData);

            if (ctx.order_by != null) {
                if (isRelevanceProperty.get(ctx.order_by)) {
                    JSONArray sortArray = new FastJSONArray();
                    sortArray.put("relevance");
                    jsonObj.put("sort", sortArray);
                } else {
                    jsonObj.put("sort", jsonProperty.get(ctx.order_by));
                }
            }
            if (ctx.limit != null) {
                jsonObj.put("from", offsetProperty.get(ctx.limit));
                jsonObj.put("size", countProperty.get(ctx.limit));
            }
            if (ctx.group_by != null) {
                jsonObj.put("groupBy", jsonProperty.get(ctx.group_by));

            }
            List<Pair<String, String>> aggregateFunctions = null;
            if (ctx.cols != null) {
                aggregateFunctions = aggregationFunctionsProperty.get(ctx.cols);
            }

            if (ctx.distinct != null) {
                jsonObj.put("distinct", jsonProperty.get(ctx.distinct));
            }
            if (ctx.browse_by != null) {
                jsonObj.put("facets", jsonProperty.get(ctx.browse_by));
            }
            if (ctx.executeMapReduce != null) {
                if (ctx.group_by != null) {
                    BQLParserUtils.decorateWithMapReduce(jsonObj, aggregationFunctionsProperty.get(ctx.cols), (JSONObject)jsonProperty.get(ctx.group_by), functionNameProperty.get(ctx.executeMapReduce), propertiesProperty.get(ctx.executeMapReduce));
                } else {
                    BQLParserUtils.decorateWithMapReduce(jsonObj, aggregationFunctionsProperty.get(ctx.cols), null, functionNameProperty.get(ctx.executeMapReduce), propertiesProperty.get(ctx.executeMapReduce));
                }
            } else {
                if (ctx.group_by != null) {
                    BQLParserUtils.decorateWithMapReduce(jsonObj, aggregationFunctionsProperty.get(ctx.cols), (JSONObject)jsonProperty.get(ctx.group_by), null, null);
                } else {
                    BQLParserUtils.decorateWithMapReduce(jsonObj, aggregationFunctionsProperty.get(ctx.cols), null, null, null);
                }
            }
            if (ctx.fetch_stored != null) {
                if (!(Boolean)valProperty.get(ctx.fetch_stored) && (ctx.cols != null && fetchStoredProperty.get(ctx.cols))) {
                    throw new IllegalStateException("FETCHING STORED cannot be false when _srcdata is selected.");
                } else if ((Boolean)valProperty.get(ctx.fetch_stored)) {
                    // Default is false
                    jsonObj.put("fetchStored", valProperty.get(ctx.fetch_stored));
                }
            }
            if (ctx.route_param != null) {
                jsonObj.put("routeParam", valProperty.get(ctx.route_param));
            }

            if (ctx.w != null) {
                extractSelectionInfo((JSONObject)jsonProperty.get(ctx.w), selections, filter, query);
                JSONObject queryPred = query.optJSONObject("query");
                if (queryPred != null) {
                    jsonObj.put("query", queryPred);
                }
                if (selections.length() > 0) {
                    jsonObj.put("selections", selections);
                }
                JSONObject f = filter.optJSONObject("filter");
                if (f != null) {
                    jsonObj.put("filter", f);
                }
            }

            if (ctx.given != null) {
                jsonObj.put("facetInit", jsonProperty.get(ctx.given));
            }

            if (ctx.rel_model != null) {
                JSONObject queryPred = jsonObj.optJSONObject("query");
                JSONObject query_string = null;
                if (queryPred != null) {
                    query_string = (JSONObject)queryPred.get("query_string");
                }
                if (query_string != null) {
                    queryPred = new FastJSONObject().put("query_string",
                                                         query_string.put("relevance", jsonProperty.get(ctx.rel_model)));
                } else {
                    queryPred = new FastJSONObject().put("query_string",
                                                         new FastJSONObject().put("query", "")
                        .put("relevance", jsonProperty.get(ctx.rel_model)));
                }
                jsonObj.put("query", queryPred);
            }
        } catch (JSONException err) {
            throw new IllegalStateException("JSONException: " + err.getMessage());
        }

        jsonProperty.put(ctx, jsonObj);
    }

    @Override
    public void enterSelection_list(BQLv4Parser.Selection_listContext ctx) {
        fetchStoredProperty.put(ctx, false);
        jsonProperty.put(ctx, new FastJSONArray());
        aggregationFunctionsProperty.put(ctx, new ArrayList<Pair<String, String>>());
    }

    @Override
    public void exitSelection_list(BQLv4Parser.Selection_listContext ctx) {
        for (BQLv4Parser.Column_nameContext col : ctx.column_name()) {
            String colName = textProperty.get(col);
            if (colName != null) {
                jsonProperty.put(ctx, textProperty.get(col));
                if ("_srcdata".equals(colName) || colName.startsWith("_srcdata.")) {
                    fetchStoredProperty.put(ctx, true);
                }
            }
        }

        for (BQLv4Parser.Aggregation_functionContext agrFunction : ctx.aggregation_function()) {
            aggregationFunctionsProperty.get(ctx).add(new Pair<String, String>(functionProperty.get(agrFunction), columnProperty.get(agrFunction)));
        }
    }

    @Override
    public void exitAggregation_function(BQLv4Parser.Aggregation_functionContext ctx) {
        functionProperty.put(ctx, textProperty.get(ctx.id));
        if (ctx.columnVar != null) {
            columnProperty.put(ctx, textProperty.get(ctx.columnVar));
        } else {
            columnProperty.put(ctx, "");
        }
    }

    @Override
    public void exitColumn_name(BQLv4Parser.Column_nameContext ctx) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < ctx.getChildCount(); i++) {
            ParseTree child = ctx.getChild(i);
            if (child instanceof TerminalNode) {
                TerminalNode terminal = (TerminalNode)child;
                String orig = terminal.getSymbol().getText();
                if (terminal.getSymbol().getType() == BQLv4Lexer.STRING_LITERAL) {
                    builder.append(orig.substring(1, orig.length() - 1));
                } else {
                    builder.append(orig);
                }
            }
        }

        textProperty.put(ctx, builder.toString());
    }

    @Override
    public void exitFunction_name(BQLv4Parser.Function_nameContext ctx) {
        if (ctx.min != null) {
            textProperty.put(ctx, "min");
        } else {
            textProperty.put(ctx, textProperty.get(ctx.colName));
        }
    }

    @Override
    public void exitWhere(BQLv4Parser.WhereContext ctx) {
        jsonProperty.put(ctx, jsonProperty.get(ctx.search_expr()));
    }

    @Override
    public void exitOrder_by_clause(BQLv4Parser.Order_by_clauseContext ctx) {
        if (ctx.RELEVANCE() != null) {
            isRelevanceProperty.put(ctx, true);
        } else {
            isRelevanceProperty.put(ctx, false);
            jsonProperty.put(ctx, jsonProperty.get(ctx.sort_specs()));
        }
    }

    @Override
    public void exitSort_specs(BQLv4Parser.Sort_specsContext ctx) {
        JSONArray sortArray = new FastJSONArray();
        for (BQLv4Parser.Sort_specContext sort : ctx.sort_spec()) {
            sortArray.put(jsonProperty.get(sort));
        }

        jsonProperty.put(ctx, sortArray);
    }

    @Override
    public void exitSort_spec(BQLv4Parser.Sort_specContext ctx) {
        JSONObject json = new FastJSONObject();
        jsonProperty.put(ctx, json);
        try {
            if (ctx.ordering == null) {
                json.put(textProperty.get(ctx.column_name()), "asc");
            } else {
                json.put(textProperty.get(ctx.column_name()), ctx.ordering.getText().toLowerCase());
            }
        } catch (JSONException err) {
            throw new IllegalStateException("JSONException: " + err.getMessage());
        }
    }

    @Override
    public void exitLimit_clause(BQLv4Parser.Limit_clauseContext ctx) {
        if (ctx.n1 != null) {
            offsetProperty.put(ctx, Integer.parseInt(ctx.n1.getText()));
        } else {
            offsetProperty.put(ctx, DEFAULT_REQUEST_OFFSET);
        }

        countProperty.put(ctx, Integer.parseInt(ctx.n2.getText()));
    }

    @Override
    public void exitComma_column_name_list(BQLv4Parser.Comma_column_name_listContext ctx) {
        JSONArray json = new FastJSONArray();
        jsonProperty.put(ctx, json);
        for (BQLv4Parser.Column_nameContext col : ctx.column_name()) {
            String colName = textProperty.get(col);
            if (colName != null) {
                json.put(colName);
            }
        }
    }

    @Override
    public void exitOr_column_name_list(BQLv4Parser.Or_column_name_listContext ctx) {
        JSONArray json = new FastJSONArray();
        jsonProperty.put(ctx, json);
        for (BQLv4Parser.Column_nameContext col : ctx.column_name()) {
            String colName = textProperty.get(col);
            if (colName != null) {
                json.put(colName);
            }
        }
    }

    @Override
    public void exitGroup_by_clause(BQLv4Parser.Group_by_clauseContext ctx) {
        JSONObject json = new FastJSONObject();
        jsonProperty.put(ctx, json);
        try {
            JSONArray cols = (JSONArray)jsonProperty.get(ctx.comma_column_name_list());
            /*for (int i = 0; i < cols.length(); ++i) {
             String col = cols.getString(i);
             String[] facetInfo = _facetInfoMap.get(col);
             if (facetInfo != null && (facetInfo[0].equals("range") ||
             facetInfo[0].equals("multi") ||
             facetInfo[0].equals("path"))) {
             throw new FailedPredicateException(input,
             "group_by_clause",
             "Range/multi/path facet, \"" + col + "\", cannot be used in the GROUP BY clause.");
             }
             }*/
            json.put("columns", cols);
            if (ctx.top != null) {
                json.put("top", Integer.parseInt(ctx.top.getText()));
            } else {
                json.put("top", DEFAULT_REQUEST_MAX_PER_GROUP);
            }
        } catch (JSONException err) {
            throw new IllegalStateException("JSONException: " + err.getMessage());
        }
    }

    @Override
    public void exitDistinct_clause(BQLv4Parser.Distinct_clauseContext ctx) {
        JSONObject json = new FastJSONObject();
        jsonProperty.put(ctx, json);
        try {
            JSONArray cols = (JSONArray)jsonProperty.get(ctx.or_column_name_list());
            if (cols.length() > 1) {
                throw new IllegalStateException("DISTINCT only support a single column now.");
            }

            for (int i = 0; i < cols.length(); ++i) {
                String col = cols.getString(i);
                String[] facetInfo = _facetInfoMap.get(col);
                if (facetInfo != null && (facetInfo[0].equals("range") ||
                    facetInfo[0].equals("multi") ||
                    facetInfo[0].equals("path"))) {
                    throw new IllegalStateException("Range/multi/path facet, \"" + col + "\", cannot be used in the DISTINCT clause.");
                }
            }

            json.put("columns", cols);
        } catch (JSONException err) {
            throw new IllegalStateException("JSONException: " + err.getMessage());
        }
    }

    @Override
    public void exitBrowse_by_clause(BQLv4Parser.Browse_by_clauseContext ctx) {
        JSONObject json = new FastJSONObject();
        jsonProperty.put(ctx, json);
        for (BQLv4Parser.Facet_specContext f : ctx.facet_spec()) {
            try {
                json.put(columnProperty.get(f), specProperty.get(f));
            } catch (JSONException err) {
                throw new IllegalStateException("JSONException: " + err.getMessage());
            }
        }
    }

    @Override
    public void exitExecute_clause(BQLv4Parser.Execute_clauseContext ctx) {
        functionNameProperty.put(ctx, textProperty.get(ctx.funName));
        if (ctx.map != null) {
            propertiesProperty.put(ctx, (JSONObject)jsonProperty.get(ctx.map));
        } else {
            JSONObject properties = new FastJSONObject();
            propertiesProperty.put(ctx, properties);
            for (BQLv4Parser.Key_value_pairContext p : ctx.key_value_pair()) {
                try {
                    properties.put(keyProperty.get(p), valProperty.get(p));
                } catch (JSONException err) {
                    throw new IllegalStateException("JSONException: " + err.getMessage());
                }
            }
        }
    }

    @Override
    public void exitFacet_spec(BQLv4Parser.Facet_specContext ctx) {
        boolean expand = false;
        int minhit = DEFAULT_FACET_MINHIT;
        int max = DEFAULT_FACET_MAXHIT;
        String orderBy = "hits";

        if (ctx.TRUE() != null) {
            expand = true;
        }

        if (ctx.VALUE() != null) {
            orderBy = "val";
        }

        columnProperty.put(ctx, textProperty.get(ctx.column_name()));
        if (ctx.n1 != null) {
            minhit = Integer.parseInt(ctx.n1.getText());
        }
        if (ctx.n2 != null) {
            max = Integer.parseInt(ctx.n2.getText());
        }

        try {
            specProperty.put(ctx, new FastJSONObject().put("expand", expand)
                .put("minhit", minhit)
                .put("max", max)
                .put("order", orderBy));
        } catch (JSONException err) {
            throw new IllegalStateException("JSONException: " + err.getMessage());
        }
    }

    @Override
    public void exitFetching_stored_clause(BQLv4Parser.Fetching_stored_clauseContext ctx) {
        valProperty.put(ctx, ctx.FALSE().isEmpty());
    }

    @Override
    public void exitRoute_by_clause(BQLv4Parser.Route_by_clauseContext ctx) {
        String orig = ctx.STRING_LITERAL().getText();
        valProperty.put(ctx, orig.substring(1, orig.length() - 1));
    }

    @Override
    public void exitSearch_expr(BQLv4Parser.Search_exprContext ctx) {
        JSONArray array = new FastJSONArray();
        for (BQLv4Parser.Term_exprContext t : ctx.term_expr()) {
            array.put(jsonProperty.get(t));
        }

        try {
            if (array.length() == 1) {
                jsonProperty.put(ctx, array.get(0));
            } else {
                jsonProperty.put(ctx, new FastJSONObject().put("or", array));
            }
        } catch (JSONException err) {
            throw new IllegalStateException("JSONException: " + err.getMessage());
        }
    }

    @Override
    public void exitTerm_expr(BQLv4Parser.Term_exprContext ctx) {
        JSONArray array = new FastJSONArray();
        for (BQLv4Parser.Factor_exprContext f : ctx.factor_expr()) {
            array.put(jsonProperty.get(f));
        }

        try {
            JSONArray newArray = new FastJSONArray();
            JSONObject fieldMap = new FastJSONObject();
            for (int i = 0; i < array.length(); ++i) {
                JSONObject pred = (JSONObject)array.get(i);
                if (!"range".equals(predType(pred))) {
                    newArray.put(pred);
                } else {
                    accumulateRangePred(fieldMap, pred);
                }
            }

            Iterator<?> itr = fieldMap.keys();
            while (itr.hasNext()) {
                newArray.put(fieldMap.get((String)itr.next()));
            }

            if (newArray.length() == 1) {
                jsonProperty.put(ctx, newArray.get(0));
            } else {
                jsonProperty.put(ctx, new FastJSONObject().put("and", newArray));
            }
        } catch (JSONException err) {
            throw new IllegalStateException("JSONException: " + err.getMessage());
        }
    }

    @Override
    public void exitFactor_expr(BQLv4Parser.Factor_exprContext ctx) {
        if (ctx.predicate() != null) {
            jsonProperty.put(ctx, jsonProperty.get(ctx.predicate()));
        } else {
            jsonProperty.put(ctx, jsonProperty.get(ctx.search_expr()));
        }
    }

    @Override
    public void exitPredicate(BQLv4Parser.PredicateContext ctx) {
        if (ctx.getChildCount() != 1) {
            throw new UnsupportedOperationException("Not yet implemented");
        }

        jsonProperty.put(ctx, jsonProperty.get(ctx.getChild(0)));
    }

    @Override
    public void exitIn_predicate(BQLv4Parser.In_predicateContext ctx) {
        String col = textProperty.get(ctx.column_name());
        String[] facetInfo = _facetInfoMap.get(col);

        if (facetInfo != null && facetInfo[0].equals("range")) {
            throw new IllegalStateException("Range facet \"" + col + "\" cannot be used in IN predicates.");
        }
        if (!verifyFieldDataType(col, jsonProperty.get(ctx.value_list()))) {
            throw new IllegalStateException("Value list for IN predicate of facet \"" + col + "\" contains incompatible value(s).");
        }

        if (ctx.except != null && !verifyFieldDataType(col, jsonProperty.get(ctx.except_clause()))) {
            throw new IllegalStateException("EXCEPT value list for IN predicate of facet \"" + col + "\" contains incompatible value(s).");
        }

        try {
            JSONObject dict = new FastJSONObject();
            dict.put("operator", "or");
            if (ctx.not == null) {
                dict.put("values", jsonProperty.get(ctx.value_list()));
                if (ctx.except != null) {
                    dict.put("excludes", jsonProperty.get(ctx.except_clause()));
                } else {
                    dict.put("excludes", new FastJSONArray());
                }
            } else {
                dict.put("excludes", jsonProperty.get(ctx.value_list()));
                if (ctx.except != null) {
                    dict.put("values", jsonProperty.get(ctx.except_clause()));
                } else {
                    dict.put("values", new FastJSONArray());
                }
            }
            if (_facetInfoMap.get(col) == null) {
                dict.put("_noOptimize", true);
            }
            jsonProperty.put(ctx, new FastJSONObject().put("terms",
                                                           new FastJSONObject().put(col, dict)));
        } catch (JSONException err) {
            throw new IllegalStateException("JSONException: " + err.getMessage());
        }
    }

    @Override
    public void exitEmpty_predicate(BQLv4Parser.Empty_predicateContext ctx) {
        try {
            JSONObject exp = new FastJSONObject();
            if (ctx.NOT() != null) {
                JSONObject functionJSON = new FastJSONObject();
                JSONArray params = new FastJSONArray();
                params.put(jsonProperty.get(ctx.value_list()));
                functionJSON.put("function", "length");
                functionJSON.put("params", params);
                exp.put("lvalue", functionJSON);
                exp.put("operator", ">");
                exp.put("rvalue", 0);
                jsonProperty.put(ctx, new FastJSONObject().put("const_exp", exp));
            } else {
                JSONObject functionJSON = new FastJSONObject();
                JSONArray params = new FastJSONArray();
                params.put(jsonProperty.get(ctx.value_list()));
                functionJSON.put("function", "length");
                functionJSON.put("params", params);
                exp.put("lvalue", functionJSON);
                exp.put("operator", "==");
                exp.put("rvalue", 0);
                jsonProperty.put(ctx, new FastJSONObject().put("const_exp", exp));
            }
        } catch (JSONException err) {
            throw new IllegalStateException("JSONException: " + err.getMessage());
        }
    }

    @Override
    public void exitContains_all_predicate(BQLv4Parser.Contains_all_predicateContext ctx) {
        String col = textProperty.get(ctx.column_name());
        String[] facetInfo = _facetInfoMap.get(col);
        if (facetInfo != null && facetInfo[0].equals("range")) {
            throw new IllegalStateException("Range facet column \"" + col + "\" cannot be used in CONTAINS ALL predicates.");
        }

        if (!verifyFieldDataType(col, jsonProperty.get(ctx.value_list()))) {
            throw new IllegalStateException("Value list for CONTAINS ALL predicate of facet \"" + col + "\" contains incompatible value(s).");
        }

        if (ctx.except != null && !verifyFieldDataType(col, jsonProperty.get(ctx.except_clause()))) {
            throw new IllegalStateException("EXCEPT value list for CONTAINS ALL predicate of facet \"" + col + "\" contains incompatible value(s).");
        }

        try {
            JSONObject dict = new FastJSONObject();
            dict.put("operator", "and");
            dict.put("values", jsonProperty.get(ctx.value_list()));
            if (ctx.except != null) {
                dict.put("excludes", jsonProperty.get(ctx.except_clause()));
            } else {
                dict.put("excludes", new FastJSONArray());
            }

            if (_facetInfoMap.get(col) == null) {
                dict.put("_noOptimize", true);
            }

            jsonProperty.put(ctx, new FastJSONObject().put("terms",
                                                           new FastJSONObject().put(col, dict)));
        } catch (JSONException err) {
            throw new IllegalStateException("JSONException: " + err.getMessage());
        }
    }

    @Override
    public void exitEqual_predicate(BQLv4Parser.Equal_predicateContext ctx) {
        String col = textProperty.get(ctx.column_name());
        if (!verifyFieldDataType(col, valProperty.get(ctx.value()))) {
            throw new IllegalStateException("Incompatible data type was found in an EQUAL predicate for column \"" + col + "\".");
        }

        try {
            String[] facetInfo = _facetInfoMap.get(col);
            if (facetInfo != null && facetInfo[0].equals("range")) {
                jsonProperty.put(ctx, new FastJSONObject().put("range",
                                                               new FastJSONObject().put(col,
                                                                                        new FastJSONObject().put("from", valProperty.get(ctx.value()))
                    .put("to", valProperty.get(ctx.value()))
                    .put("include_lower", true)
                    .put("include_upper", true))));
            } else if (facetInfo != null && facetInfo[0].equals("path")) {
                JSONObject valObj = new FastJSONObject();
                valObj.put("value", valProperty.get(ctx.value()));
                if (ctx.props != null) {
                    JSONObject propsJson = (JSONObject)jsonProperty.get(ctx.props);
                    Iterator<?> itr = propsJson.keys();
                    while (itr.hasNext()) {
                        String key = (String)itr.next();
                        if (key.equals("strict") || key.equals("depth")) {
                            valObj.put(key, propsJson.get(key));
                        } else {
                            throw new IllegalStateException("Unsupported property was found in an EQUAL predicate for path facet column \"" + col + "\": " + key + ".");
                        }
                    }
                }

                jsonProperty.put(ctx, new FastJSONObject().put("path", new FastJSONObject().put(col, valObj)));
            } else {
                JSONObject valSpec = new FastJSONObject().put("value", valProperty.get(ctx.value()));
                if (_facetInfoMap.get(col) == null) {
                    valSpec.put("_noOptimize", true);
                }

                jsonProperty.put(ctx, new FastJSONObject().put("term",
                                                               new FastJSONObject().put(col, valSpec)));
            }
        } catch (JSONException err) {
            throw new IllegalStateException("JSONException: " + err.getMessage());
        }
    }

    @Override
    public void exitNot_equal_predicate(BQLv4Parser.Not_equal_predicateContext ctx) {
        String col = textProperty.get(ctx.column_name());
        if (!verifyFieldDataType(col, valProperty.get(ctx.value()))) {
            throw new IllegalStateException("Incompatible data type was found in a NOT EQUAL predicate for column \"" + col + "\".");
        }

        try {
            String[] facetInfo = _facetInfoMap.get(col);
            if (facetInfo != null && facetInfo[0].equals("range")) {
                JSONObject left = new FastJSONObject().put("range",
                                                           new FastJSONObject().put(col,
                                                                                    new FastJSONObject().put("to", valProperty.get(ctx.value()))
                    .put("include_upper", false)));
                JSONObject right = new FastJSONObject().put("range",
                                                            new FastJSONObject().put(col,
                                                                                     new FastJSONObject().put("from", valProperty.get(ctx.value()))
                    .put("include_lower", false)));
                jsonProperty.put(ctx, new FastJSONObject().put("or", new FastJSONArray().put(left).put(right)));
            } else if (facetInfo != null && facetInfo[0].equals("path")) {
                throw new IllegalStateException("NOT EQUAL predicate is not supported for path facets (column \"" + col + "\").");
            } else {
                JSONObject valObj = new FastJSONObject();
                valObj.put("operator", "or");
                valObj.put("values", new FastJSONArray());
                valObj.put("excludes", new FastJSONArray().put(valProperty.get(ctx.value())));
                if (_facetInfoMap.get(col) == null) {
                    valObj.put("_noOptimize", true);
                }

                jsonProperty.put(ctx, new FastJSONObject().put("terms",
                                                               new FastJSONObject().put(col, valObj)));
            }
        } catch (JSONException err) {
            throw new IllegalStateException("JSONException: " + err.getMessage());
        }
    }

    @Override
    public void exitQuery_predicate(BQLv4Parser.Query_predicateContext ctx) {
        try {
            String orig = ctx.STRING_LITERAL().getText();
            orig = orig.substring(1, orig.length() - 1);
            jsonProperty.put(ctx, new FastJSONObject().put("query",
                                                           new FastJSONObject().put("query_string",
                                                                                    new FastJSONObject().put("query", orig))));
        } catch (JSONException err) {
            throw new IllegalStateException("JSONException: " + err.getMessage());
        }
    }

    @Override
    public void exitBetween_predicate(BQLv4Parser.Between_predicateContext ctx) {
        String col = textProperty.get(ctx.column_name());
        if (!verifyFacetType(col, "range")) {
            throw new IllegalStateException("Non-rangable facet column \"" + col + "\" cannot be used in BETWEEN predicates.");
        }

        if (!verifyFieldDataType(col, new Object[]{valProperty.get(ctx.val1), valProperty.get(ctx.val2)})) {
            throw new IllegalStateException("Incompatible data type was found in a BETWEEN predicate for column \"" + col + "\".");
        }

        try {
            if (ctx.not == null) {
                jsonProperty.put(ctx, new FastJSONObject().put("range",
                                                               new FastJSONObject().put(col,
                                                                                        new FastJSONObject().put("from", valProperty.get(ctx.val1))
                    .put("to", valProperty.get(ctx.val2))
                    .put("include_lower", true)
                    .put("include_upper", true))));
            } else {
                JSONObject range1
                    = new FastJSONObject().put("range",
                                               new FastJSONObject().put(col,
                                                                        new FastJSONObject().put("to", valProperty.get(ctx.val1))
                        .put("include_upper", false)));
                JSONObject range2
                    = new FastJSONObject().put("range",
                                               new FastJSONObject().put(col,
                                                                        new FastJSONObject().put("from", valProperty.get(ctx.val2))
                        .put("include_lower", false)));

                jsonProperty.put(ctx, new FastJSONObject().put("or", new FastJSONArray().put(range1).put(range2)));
            }
        } catch (JSONException err) {
            throw new IllegalStateException("JSONException: " + err.getMessage());
        }
    }

    @Override
    public void exitRange_predicate(BQLv4Parser.Range_predicateContext ctx) {
        String col = textProperty.get(ctx.column_name());
        if (!verifyFacetType(col, "range")) {
            throw new IllegalStateException("Non-rangable facet column \"" + col + "\" cannot be used in RANGE predicates.");
        }

        if (!verifyFieldDataType(col, valProperty.get(ctx.val))) {
            throw new IllegalStateException("Incompatible data type was found in a RANGE predicate for column \"" + col + "\".");
        }

        try {
            if (ctx.op.getText().charAt(0) == '>') {
                jsonProperty.put(ctx, new FastJSONObject().put("range",
                                                               new FastJSONObject().put(col,
                                                                                        new FastJSONObject().put("from", valProperty.get(ctx.val))
                    .put("include_lower", ">=".equals(ctx.op.getText())))));
            } else {
                jsonProperty.put(ctx, new FastJSONObject().put("range",
                                                               new FastJSONObject().put(col,
                                                                                        new FastJSONObject().put("to", valProperty.get(ctx.val))
                    .put("include_upper", "<=".equals(ctx.op.getText())))));
            }
        } catch (JSONException err) {
            throw new IllegalStateException("JSONException: " + err.getMessage());
        }
    }

    @Override
    public void exitTime_predicate(BQLv4Parser.Time_predicateContext ctx) {
        if (ctx.LAST() != null) {
            String col = textProperty.get(ctx.column_name());
            if (!verifyFacetType(col, "range")) {
                throw new IllegalStateException("Non-rangable facet column \"" + col + "\" cannot be used in TIME predicates.");
            }

            try {
                if (ctx.NOT() == null) {
                    jsonProperty.put(ctx, new FastJSONObject().put("range",
                                                                   new FastJSONObject().put(col,
                                                                                            new FastJSONObject().put("from", valProperty.get(ctx.time_span()))
                        .put("include_lower", false))));
                } else {
                    jsonProperty.put(ctx, new FastJSONObject().put("range",
                                                                   new FastJSONObject().put(col,
                                                                                            new FastJSONObject().put("to", valProperty.get(ctx.time_span()))
                        .put("include_upper", true))));
                }
            } catch (JSONException err) {
                throw new IllegalStateException("JSONException: " + err.getMessage());
            }
        } else {
            String col = textProperty.get(ctx.column_name());
            if (!verifyFacetType(col, "range")) {
                throw new IllegalStateException("Non-rangable facet column \"" + col + "\" cannot be used in TIME predicates.");
            }

            try {
                if (ctx.since != null && ctx.NOT() == null ||
                    ctx.since == null && ctx.NOT() != null) {
                    jsonProperty.put(ctx, new FastJSONObject().put("range",
                                                                   new FastJSONObject().put(col,
                                                                                            new FastJSONObject().put("from", valProperty.get(ctx.time_expr()))
                        .put("include_lower", false))));
                } else {
                    jsonProperty.put(ctx, new FastJSONObject().put("range",
                                                                   new FastJSONObject().put(col,
                                                                                            new FastJSONObject().put("to", valProperty.get(ctx.time_expr()))
                        .put("include_upper", false))));
                }
            } catch (JSONException err) {
                throw new IllegalStateException("JSONException: " + err.getMessage());
            }
        }
    }

    @Override
    public void exitTime_span(BQLv4Parser.Time_spanContext ctx) {
        long val = 0;
        if (ctx.week != null) {
            val += (Long)valProperty.get(ctx.week);
        }

        if (ctx.day != null) {
            val += (Long)valProperty.get(ctx.day);
        }

        if (ctx.hour != null) {
            val += (Long)valProperty.get(ctx.hour);
        }

        if (ctx.minute != null) {
            val += (Long)valProperty.get(ctx.minute);
        }

        if (ctx.second != null) {
            val += (Long)valProperty.get(ctx.second);
        }

        if (ctx.msec != null) {
            val += (Long)valProperty.get(ctx.msec);
        }

        valProperty.put(ctx, _now - val);
    }

    @Override
    public void exitTime_week_part(BQLv4Parser.Time_week_partContext ctx) {
        long val = Integer.parseInt(ctx.INTEGER().getText()) * 7 * 24 * 60 * 60 * 1000L;
        valProperty.put(ctx, val);
    }

    @Override
    public void exitTime_day_part(BQLv4Parser.Time_day_partContext ctx) {
        long val = Integer.parseInt(ctx.INTEGER().getText()) * 24 * 60 * 60 * 1000L;
        valProperty.put(ctx, val);
    }

    @Override
    public void exitTime_hour_part(BQLv4Parser.Time_hour_partContext ctx) {
        long val = Integer.parseInt(ctx.INTEGER().getText()) * 60 * 60 * 1000L;
        valProperty.put(ctx, val);
    }

    @Override
    public void exitTime_minute_part(BQLv4Parser.Time_minute_partContext ctx) {
        long val = Integer.parseInt(ctx.INTEGER().getText()) * 60 * 1000L;
        valProperty.put(ctx, val);
    }

    @Override
    public void exitTime_second_part(BQLv4Parser.Time_second_partContext ctx) {
        long val = Integer.parseInt(ctx.INTEGER().getText()) * 1000L;
        valProperty.put(ctx, val);
    }

    @Override
    public void exitTime_millisecond_part(BQLv4Parser.Time_millisecond_partContext ctx) {
        long val = Integer.parseInt(ctx.INTEGER().getText());
        valProperty.put(ctx, val);
    }

    @Override
    public void exitTime_expr(BQLv4Parser.Time_exprContext ctx) {
        if (ctx.time_span() != null) {
            valProperty.put(ctx, valProperty.get(ctx.time_span()));
        } else if (ctx.date_time_string() != null) {
            valProperty.put(ctx, valProperty.get(ctx.date_time_string()));
        } else if (ctx.NOW() != null) {
            valProperty.put(ctx, _now);
        } else {
            throw new UnsupportedOperationException("Not yet implemented");
        }
    }

    @Override
    public void exitDate_time_string(BQLv4Parser.Date_time_stringContext ctx) {
        SimpleDateFormat format;
        String dateTimeStr = ctx.DATE().getText();
        char separator = dateTimeStr.charAt(4);
        if (ctx.TIME() != null) {
            dateTimeStr = dateTimeStr + " " + ctx.TIME().getText();
        }

        int formatIdx = (separator == '-' ? 0 : 1);

        if (ctx.TIME() == null) {
            if (_format1[formatIdx] != null) {
                format = _format1[formatIdx];
            } else {
                format = _format1[formatIdx] = new SimpleDateFormat("yyyy" + separator + "MM" + separator + "dd");
            }
        } else {
            if (_format2[formatIdx] != null) {
                format = _format2[formatIdx];
            } else {
                format = _format2[formatIdx] = new SimpleDateFormat("yyyy" + separator + "MM" + separator + "dd HH:mm:ss");
            }
        }

        try {
            valProperty.put(ctx, format.parse(dateTimeStr).getTime());
            if (!dateTimeStr.equals(format.format(valProperty.get(ctx)))) {
                throw new IllegalStateException("Date string contains invalid date/time: \"" + dateTimeStr + "\".");
            }
        } catch (ParseException err) {
            throw new IllegalStateException("ParseException happened for \"" + dateTimeStr + "\": " +
                err.getMessage() + ".");
        }
    }

    @Override
    public void exitMatch_predicate(BQLv4Parser.Match_predicateContext ctx) {
        try {
            JSONArray cols = (JSONArray)jsonProperty.get(ctx.selection_list());
            for (int i = 0; i < cols.length(); ++i) {
                String col = cols.getString(i);
                String[] facetInfo = _facetInfoMap.get(col);
                if (facetInfo != null && !facetInfo[1].equals("string")) {
                    throw new IllegalStateException("Non-string type column \"" + col + "\" cannot be used in MATCH AGAINST predicates.");
                }
            }

            String orig = ctx.STRING_LITERAL().getText();
            orig = orig.substring(1, orig.length() - 1);
            jsonProperty.put(ctx, new FastJSONObject().put("query",
                                                           new FastJSONObject().put("query_string",
                                                                                    new FastJSONObject().put("fields", cols)
                .put("query", orig))));
            if (ctx.NOT() != null) {
                jsonProperty.put(ctx, new FastJSONObject().put("bool",
                                                               new FastJSONObject().put("must_not", jsonProperty.get(ctx))));
            }
        } catch (JSONException err) {
            throw new IllegalStateException("JSONException: " + err.getMessage());
        }
    }

    @Override
    public void exitLike_predicate(BQLv4Parser.Like_predicateContext ctx) {
        String col = textProperty.get(ctx.column_name());
        String[] facetInfo = _facetInfoMap.get(col);
        if (facetInfo != null && !facetInfo[1].equals("string")) {
            throw new IllegalStateException("Non-string type column \"" + col + "\" cannot be used in LIKE predicates.");
        }

        String orig = ctx.STRING_LITERAL().getText();
        orig = orig.substring(1, orig.length() - 1);
        String likeString = orig.replace('%', '*').replace('_', '?');
        try {
            jsonProperty.put(ctx, new FastJSONObject().put("query",
                                             new FastJSONObject().put("wildcard",
                                                                      new FastJSONObject().put(col, likeString))));
            if (ctx.NOT() != null) {
                jsonProperty.put(ctx, new FastJSONObject().put("bool",
                                                 new FastJSONObject().put("must_not", jsonProperty.get(ctx))));
            }
        }
        catch (JSONException err) {
            throw new IllegalStateException("JSONException: " + err.getMessage());
        }
    }

    @Override
    public void exitNull_predicate(BQLv4Parser.Null_predicateContext ctx) {
        String col = textProperty.get(ctx.column_name());
        try {
            jsonProperty.put(ctx, new FastJSONObject().put("isNull", col));
            if (ctx.NOT() != null) {
                jsonProperty.put(ctx, new FastJSONObject().put("bool",
                                                               new FastJSONObject().put("must_not", jsonProperty.get(ctx))));
            }
        } catch (JSONException err) {
            throw new IllegalStateException("JSONException: " + err.getMessage());
        }
    }

    @Override
    public void exitNon_variable_value_list(BQLv4Parser.Non_variable_value_listContext ctx) {
        JSONArray json = new FastJSONArray();
        jsonProperty.put(ctx, json);
        for (BQLv4Parser.ValueContext v : ctx.value()) {
            json.put(valProperty.get(v));
        }
    }

    @Override
    public void exitPython_style_list(BQLv4Parser.Python_style_listContext ctx) {
        JSONArray json = new FastJSONArray();
        jsonProperty.put(ctx, json);
        for (BQLv4Parser.Python_style_valueContext v : ctx.python_style_value()) {
            // TODO: make sure handling here is correct when first python_style_value is missing
            json.put(valProperty.get(v));
        }
    }

    @Override
    public void exitPython_style_dict(BQLv4Parser.Python_style_dictContext ctx) {
        JSONObject json = new FastJSONObject();
        jsonProperty.put(ctx, json);
        for (BQLv4Parser.Key_value_pairContext p : ctx.key_value_pair()) {
            try {
                json.put(keyProperty.get(p), valProperty.get(p));
            } catch (JSONException err) {
                throw new IllegalStateException("JSONException: " + err.getMessage());
            }
        }
    }

    @Override
    public void exitPython_style_value(BQLv4Parser.Python_style_valueContext ctx) {
        if (ctx.value() != null) {
            valProperty.put(ctx, valProperty.get(ctx.value()));
        } else if (ctx.python_style_list() != null) {
            valProperty.put(ctx, jsonProperty.get(ctx.python_style_list()));
        } else if (ctx.python_style_dict() != null) {
            valProperty.put(ctx, jsonProperty.get(ctx.python_style_dict()));
        } else {
            throw new UnsupportedOperationException("Not yet implemented.");
        }
    }

    @Override
    public void exitValue_list(BQLv4Parser.Value_listContext ctx) {
        if (ctx.non_variable_value_list() != null) {
            jsonProperty.put(ctx, jsonProperty.get(ctx.non_variable_value_list()));
        } else if (ctx.VARIABLE() != null) {
            jsonProperty.put(ctx, ctx.VARIABLE().getText());
            _variables.add(ctx.VARIABLE().getText().substring(1));
        } else {
            throw new UnsupportedOperationException("Not yet implemented.");
        }
    }

    @Override
    public void exitValue(BQLv4Parser.ValueContext ctx) {
        if (ctx.numeric() != null) {
            valProperty.put(ctx, valProperty.get(ctx.numeric()));
        } else if (ctx.STRING_LITERAL() != null) {
            String orig = ctx.STRING_LITERAL().getText();
            orig = orig.substring(1, orig.length() - 1);
            valProperty.put(ctx, orig);
        } else if (ctx.TRUE() != null) {
            valProperty.put(ctx, true);
        } else if (ctx.FALSE() != null) {
            valProperty.put(ctx, true);
        } else if (ctx.VARIABLE() != null) {
            valProperty.put(ctx, ctx.VARIABLE().getText());
            _variables.add(ctx.VARIABLE().getText().substring(1));
        } else {
            throw new UnsupportedOperationException("Not yet implemented.");
        }
    }

    @Override
    public void exitNumeric(BQLv4Parser.NumericContext ctx) {
        if (ctx.time_expr() != null) {
            valProperty.put(ctx, valProperty.get(ctx.time_expr()));
        } else if (ctx.INTEGER() != null) {
            try {
                valProperty.put(ctx, Long.parseLong(ctx.INTEGER().getText()));
            } catch (NumberFormatException err) {
                throw new IllegalStateException("Hit NumberFormatException: " + err.getMessage());
            }
        } else if (ctx.REAL() != null) {
            try {
                valProperty.put(ctx, Float.parseFloat(ctx.REAL().getText()));
            } catch (NumberFormatException err) {
                throw new IllegalStateException("Hit NumberFormatException: " + err.getMessage());
            }
        } else {
            throw new UnsupportedOperationException("Not yet implemented.");
        }
    }

    @Override
    public void exitExcept_clause(BQLv4Parser.Except_clauseContext ctx) {
        jsonProperty.put(ctx, jsonProperty.get(ctx.value_list()));
    }

    @Override
    public void exitPredicate_props(BQLv4Parser.Predicate_propsContext ctx) {
        jsonProperty.put(ctx, jsonProperty.get(ctx.prop_list()));
    }

    @Override
    public void exitProp_list(BQLv4Parser.Prop_listContext ctx) {
        JSONObject json = new FastJSONObject();
        jsonProperty.put(ctx, json);
        for (BQLv4Parser.Key_value_pairContext p : ctx.key_value_pair()) {
            try {
                json.put(keyProperty.get(p), valProperty.get(p));
            } catch (JSONException err) {
                throw new IllegalStateException("JSONException: " + err.getMessage());
            }
        }
    }

    @Override
    public void exitKey_value_pair(BQLv4Parser.Key_value_pairContext ctx) {
        if (ctx.STRING_LITERAL() != null) {
            String orig = ctx.STRING_LITERAL().getText();
            keyProperty.put(ctx, orig.substring(1, orig.length() - 1));
        } else {
            keyProperty.put(ctx, ctx.IDENT().getText());
        }

        if (ctx.v != null) {
            valProperty.put(ctx, valProperty.get(ctx.v));
        } else if (ctx.vs != null) {
            valProperty.put(ctx, jsonProperty.get(ctx.vs));
        } else {
            valProperty.put(ctx, jsonProperty.get(ctx.vd));
        }
    }

    @Override
    public void exitGiven_clause(BQLv4Parser.Given_clauseContext ctx) {
        jsonProperty.put(ctx, jsonProperty.get(ctx.facet_param_list()));
    }

    @Override
    public void exitVariable_declarators(BQLv4Parser.Variable_declaratorsContext ctx) {
        JSONArray json = new FastJSONArray();
        jsonProperty.put(ctx, json);
        for (BQLv4Parser.Variable_declaratorContext var : ctx.variable_declarator()) {
            json.put(varNameProperty.get(var));
        }
    }

    @Override
    public void exitVariable_declarator(BQLv4Parser.Variable_declaratorContext ctx) {
        varNameProperty.put(ctx, varNameProperty.get(ctx.variable_declarator_id()));
    }

    @Override
    public void exitVariable_declarator_id(BQLv4Parser.Variable_declarator_idContext ctx) {
        varNameProperty.put(ctx, ctx.IDENT().getText());
    }

    @Override
    public void exitType(BQLv4Parser.TypeContext ctx) {
        if (ctx.class_or_interface_type() != null) {
            typeNameProperty.put(ctx, typeNameProperty.get(ctx.class_or_interface_type()));
        } else if (ctx.primitive_type() != null) {
            typeNameProperty.put(ctx, textProperty.get(ctx.primitive_type()));
        } else if (ctx.boxed_type() != null) {
            typeNameProperty.put(ctx, textProperty.get(ctx.boxed_type()));
        } else if (ctx.limited_type() != null) {
            typeNameProperty.put(ctx, textProperty.get(ctx.limited_type()));
        } else {
            throw new UnsupportedOperationException("Not implemented yet.");
        }
    }

    @Override
    public void exitClass_or_interface_type(BQLv4Parser.Class_or_interface_typeContext ctx) {
        typeNameProperty.put(ctx, _fastutilTypeMap.get(ctx.FAST_UTIL_DATA_TYPE().getText()));
    }

    @Override
    public void exitType_arguments(BQLv4Parser.Type_argumentsContext ctx) {
        StringBuilder builder = new StringBuilder();
        for (BQLv4Parser.Type_argumentContext ta : ctx.type_argument()) {
            if (builder.length() > 0) {
                builder.append('_');
            }

            builder.append(ta.getText());
        }

        typeArgsProperty.put(ctx, builder.toString());
    }

    @Override
    public void exitFormal_parameters(BQLv4Parser.Formal_parametersContext ctx) {
        jsonProperty.put(ctx, jsonProperty.get(ctx.formal_parameter_decls()));
    }

    @Override
    public void exitFormal_parameter_decls(BQLv4Parser.Formal_parameter_declsContext ctx) {
        JSONObject json = new FastJSONObject();
        jsonProperty.put(ctx, json);
        Set<String> params = new HashSet<String>();
        for (BQLv4Parser.Formal_parameter_declContext decl : ctx.formal_parameter_decl()) {
            try {
                processRelevanceModelParam(json, params, typeNameProperty.get(decl), varNameProperty.get(decl));
            }
            catch (JSONException err) {
                throw new IllegalStateException("JSONException: " + err.getMessage());
            }
        }
    }

    @Override
    public void exitFormal_parameter_decl(BQLv4Parser.Formal_parameter_declContext ctx) {
        typeNameProperty.put(ctx, typeNameProperty.get(ctx.type()));
        varNameProperty.put(ctx, varNameProperty.get(ctx.variable_declarator_id()));
    }

    @Override
    public void enterRelevance_model(BQLv4Parser.Relevance_modelContext ctx) {
        _usedFacets = new HashSet<String>();
        _usedInternalVars = new HashSet<String>();
        _symbolTable = new LinkedList<Map<String, String>>();
        _currentScope = new HashMap<String, String>();
        _symbolTable.offerLast(_currentScope);
    }

    @Override
    public void exitRelevance_model(BQLv4Parser.Relevance_modelContext ctx) {
        functionBodyProperty.put(ctx, textProperty.get(ctx.model_block()));
        JSONObject json = (JSONObject)jsonProperty.get(ctx.params);
        jsonProperty.put(ctx, json);

        // Append facets and internal variable to "function_params".
        try {
            JSONArray funcParams = json.getJSONArray("function_params");

            JSONObject facets = new FastJSONObject();
            json.put("facets", facets);

            for (String facet : _usedFacets) {
                funcParams.put(facet);
                String[] facetInfo = _facetInfoMap.get(facet);
                String typeName = (facetInfo[0].equals("multi") ? "m"
                    : (facetInfo[0].equals("weighted-multi") ? "wm" : "")) +
                     _facetInfoMap.get(facet)[1];
                JSONArray facetsWithSameType = facets.optJSONArray(typeName);
                if (facetsWithSameType == null) {
                    facetsWithSameType = new FastJSONArray();
                    facets.put(typeName, facetsWithSameType);
                }
                facetsWithSameType.put(facet);
            }

                // Internal variables, like _NOW, do not need to be
            // included in "variables".
            for (String varName : _usedInternalVars) {
                if (!_internalStaticVarMap.containsKey(varName)) {
                    funcParams.put(varName);
                }
            }
        } catch (JSONException err) {
            throw new IllegalStateException("JSONException: " + err.getMessage());
        }
    }

    @Override
    public void enterModel_block(BQLv4Parser.Model_blockContext ctx) {
        if (!(ctx.getParent() instanceof BQLv4Parser.Relevance_modelContext)) {
            throw new UnsupportedOperationException("Parent of model_block must be relevance_model");
        }

        BQLv4Parser.Relevance_modelContext parent = (BQLv4Parser.Relevance_modelContext)ctx.getParent();
        try {
            JSONObject varParams = ((JSONObject)jsonProperty.get(parent.params)).optJSONObject("variables");
            if (varParams != null) {
                Iterator<?> itr = varParams.keys();
                while (itr.hasNext()) {
                    String key = (String)itr.next();
                    JSONArray vars = varParams.getJSONArray(key);
                    for (int i = 0; i < vars.length(); ++i) {
                        _currentScope.put(vars.getString(i), key);
                    }
                }
            }
        } catch (JSONException err) {
            throw new IllegalStateException("JSONException: " + err.getMessage());
        }
    }

    @Override
    public void enterBlock(BQLv4Parser.BlockContext ctx) {
        _currentScope = new HashMap<String, String>();
        _symbolTable.offerLast(_currentScope);
    }

    @Override
    public void exitBlock(BQLv4Parser.BlockContext ctx) {
        _symbolTable.pollLast();
        _currentScope = _symbolTable.peekLast();
    }

    @Override
    public void exitLocal_variable_declaration(BQLv4Parser.Local_variable_declarationContext ctx) {
        try {
            JSONArray vars = (JSONArray)jsonProperty.get(ctx.variable_declarators());
            for (int i = 0; i < vars.length(); ++i) {
                String var = vars.getString(i);
                if (_facetInfoMap.containsKey(var)) {
                    throw new IllegalStateException("Facet name \"" + var + "\" cannot be used to declare a variable.");
                } else if (_internalVarMap.containsKey(var)) {
                    throw new IllegalStateException("Internal variable \"" + var + "\" cannot be re-used to declare another variable.");
                } else if (verifyVariable(var)) {
                    throw new IllegalStateException("Variable \"" + var + "\" is already defined.");
                } else {
                    _currentScope.put(var, typeNameProperty.get(ctx.type()));
                }
            }
        } catch (JSONException err) {
            throw new IllegalStateException("JSONException: " + err.getMessage());
        }
    }

    @Override
    public void enterJava_statement(BQLv4Parser.Java_statementContext ctx) {
        if (ctx.FOR() != null) {
            _currentScope = new HashMap<String, String>();
            _symbolTable.offerLast(_currentScope);
        }
    }

    @Override
    public void exitJava_statement(BQLv4Parser.Java_statementContext ctx) {
        if (ctx.FOR() != null) {
            _symbolTable.pollLast();
            _currentScope = _symbolTable.peekLast();
        }
    }

    @Override
    public void enterAssignment_operator(BQLv4Parser.Assignment_operatorContext ctx) {
        checkOperatorSpacing(ctx);
    }

    @Override
    public void enterRelational_op(BQLv4Parser.Relational_opContext ctx) {
        checkOperatorSpacing(ctx);
    }

    @Override
    public void enterShift_op(BQLv4Parser.Shift_opContext ctx) {
        checkOperatorSpacing(ctx);
    }

    private void checkOperatorSpacing(ParserRuleContext ctx) {
        if (ctx.getChildCount() == 1) {
            return;
        }

        TerminalNode previous = null;
        for (int i = 0; i < ctx.getChildCount(); i++) {
            if (!(ctx.getChild(i) instanceof TerminalNode)) {
                throw new UnsupportedOperationException("Unexpected child type.");
            }

            TerminalNode current = (TerminalNode)ctx.getChild(i);
            if (previous != null) {
                if (previous.getSymbol().getStopIndex() + 1 != current.getSymbol().getStartIndex()) {
                    throw new IllegalStateException("Operators cannot contain spaces.");
                }
            }

            previous = current;
        }
    }

    @Override
    public void exitPrimary(BQLv4Parser.PrimaryContext ctx) {
        if (ctx.java_ident() != null) {
            String var = ctx.java_ident().getText();
            if (_facetInfoMap.containsKey(var)) {
                _usedFacets.add(var);
            } else if (_internalVarMap.containsKey(var)) {
                _usedInternalVars.add(var);
            } else if (!_supportedClasses.contains(var) && !verifyVariable(var)) {
                throw new IllegalStateException("Variable or class \"" + var + "\" is not defined.");
            }
        }
    }

    @Override
    public void exitRelevance_model_clause(BQLv4Parser.Relevance_model_clauseContext ctx) {
        JSONObject json = new FastJSONObject();
        jsonProperty.put(ctx, json);
        try {
            if (ctx.model == null) {
                json.put("predefined_model", ctx.IDENT().getText());
                json.put("values", jsonProperty.get(ctx.prop_list()));
            } else {
                JSONObject modelInfo = (JSONObject)jsonProperty.get(ctx.model);
                JSONObject modelJson = new FastJSONObject();
                modelJson.put("function", functionBodyProperty.get(ctx.model));

                JSONArray funcParams = modelInfo.optJSONArray("function_params");
                if (funcParams != null) {
                    modelJson.put("function_params", funcParams);
                }

                JSONObject facets = modelInfo.optJSONObject("facets");
                if (facets != null) {
                    modelJson.put("facets", facets);
                }

                JSONObject variables = modelInfo.optJSONObject("variables");
                if (variables != null) {
                    modelJson.put("variables", variables);
                }

                json.put("model", modelJson);
                json.put("values", jsonProperty.get(ctx.prop_list()));
            }
        } catch (JSONException err) {
            throw new IllegalStateException("JSONException: " + err.getMessage());
        }
    }

    @Override
    public void exitFacet_param_list(BQLv4Parser.Facet_param_listContext ctx) {
        JSONObject json = new FastJSONObject();
        jsonProperty.put(ctx, json);
        for (BQLv4Parser.Facet_paramContext p : ctx.facet_param()) {
            try {
                if (!json.has(facetProperty.get(p))) {
                    json.put(facetProperty.get(p), paramProperty.get(p));
                } else {
                    JSONObject currentParam = (JSONObject)json.get(facetProperty.get(p));
                    String paramName = (String)paramProperty.get(p).keys().next();
                    currentParam.put(paramName, paramProperty.get(p).get(paramName));
                }
            } catch (JSONException err) {
                throw new IllegalStateException("JSONException: " + err.getMessage());
            }
        }
    }

    @Override
    public void exitFacet_param(BQLv4Parser.Facet_paramContext ctx) {
        facetProperty.put(ctx, textProperty.get(ctx.column_name())); // XXX Check error here?
        try {
            Object valArray;
            if (ctx.val != null) {
                String varName = textProperty.get(ctx.value());
                if (varName.matches("\\$[^$].*")) {
                        // Here "value" is a variable.  In this case, it
                    // is REQUIRED that the variable should be
                    // replaced by a list, NOT a scalar value.
                    valArray = varName;
                } else {
                    valArray = new FastJSONArray().put(valProperty.get(ctx.val));
                }
            } else {
                valArray = jsonProperty.get(ctx.valList);
            }

            String orig = ctx.STRING_LITERAL().getText();
            orig = orig.substring(1, orig.length() - 1);
            paramProperty.put(ctx, new FastJSONObject().put(orig,
                                                            new FastJSONObject().put("type", paramTypeProperty.get(ctx.facet_param_type()))
                .put("values", valArray)));
        } catch (JSONException err) {
            throw new IllegalStateException("JSONException: " + err.getMessage());
        }
    }

    @Override
    public void exitFacet_param_type(BQLv4Parser.Facet_param_typeContext ctx) {
        paramTypeProperty.put(ctx, ctx.t.getText());
    }
}
