package com.senseidb.bql.parsers;

import com.senseidb.search.req.BQLParserUtils;
import com.senseidb.util.JSONUtil.FastJSONArray;
import com.senseidb.util.JSONUtil.FastJSONObject;
import com.senseidb.util.Pair;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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

    private long _now;
    private HashSet<String> _variables;

    private final ParseTreeProperty<Object> jsonProperty = new ParseTreeProperty<Object>();
    private final ParseTreeProperty<Boolean> fetchStoredProperty = new ParseTreeProperty<Boolean>();
    private final ParseTreeProperty<List<Pair<String, String>>> aggregationFunctionsProperty = new ParseTreeProperty<List<Pair<String, String>>>();
    private final ParseTreeProperty<String> functionProperty = new ParseTreeProperty<String>();
    private final ParseTreeProperty<String> columnProperty = new ParseTreeProperty<String>();
    private final ParseTreeProperty<String> textProperty = new ParseTreeProperty<String>();
    private final ParseTreeProperty<Boolean> isRelevanceProperty = new ParseTreeProperty<Boolean>();

    @Override
    public void exitStatement(BQLv4Parser.StatementContext ctx) {
        if (ctx.select_stmt != null) {
            jsonProperty.put(ctx, jsonProperty.get(ctx.select_stmt));
        }
    }

    @Override
    public void enterSelect_stmt(BQLv4Parser.Select_stmtContext ctx) {
        _now = System.currentTimeMillis();
        _variables = new HashSet<String>();
    }

    @Override
    public void exitSelect_stmt(BQLv4Parser.Select_stmtContext ctx) {
        JSONObject jsonObj = new FastJSONObject();
        JSONArray selections = new FastJSONArray();
        JSONObject filter = new FastJSONObject();
        JSONObject query = new FastJSONObject();

        try {
            JSONObject metaData = new FastJSONObject();
            if (ctx.cols == null) {
                metaData.put("select_list", new FastJSONArray().put("*"));
            } else {
                metaData.put("select_list", ctx.cols.json);
                if (ctx.cols.fetchStored) {
                    jsonObj.put("fetchStored", true);
                }
            }

            if (_variables.size() > 0) {
                metaData.put("variables", new FastJSONArray(_variables));
            }

            jsonObj.put("meta", metaData);

            if (ctx.order_by != null) {
                if (ctx.order_by.isRelevance) {
                    JSONArray sortArray = new FastJSONArray();
                    sortArray.put("relevance");
                    jsonObj.put("sort", sortArray);
                } else {
                    jsonObj.put("sort", ctx.order_by.json);
                }
            }
            if (ctx.limit != null) {
                jsonObj.put("from", ctx.limit.offset);
                jsonObj.put("size", ctx.limit.count);
            }
            if (ctx.group_by != null) {
                jsonObj.put("groupBy", ctx.group_by.json);

            }
            List<Pair<String, String>> aggregateFunctions = null;
            if (ctx.cols != null) {
                aggregateFunctions = ctx.cols.aggregationFunctions;
            }

            if (ctx.distinct != null) {
                jsonObj.put("distinct", ctx.distinct.json);
            }
            if (ctx.browse_by != null) {
                jsonObj.put("facets", ctx.browse_by.json);
            }
            if (ctx.executeMapReduce != null) {
                if (ctx.group_by != null) {
                    BQLParserUtils.decorateWithMapReduce(jsonObj, ctx.cols.aggregationFunctions, ctx.group_by.json, ctx.executeMapReduce.functionName, ctx.executeMapReduce.properties);
                } else {
                    BQLParserUtils.decorateWithMapReduce(jsonObj, ctx.cols.aggregationFunctions, null, ctx.executeMapReduce.functionName, ctx.executeMapReduce.properties);
                }
            } else {
                if (ctx.group_by != null) {
                    BQLParserUtils.decorateWithMapReduce(jsonObj, ctx.cols.aggregationFunctions, ctx.group_by.json, null, null);
                } else {
                    BQLParserUtils.decorateWithMapReduce(jsonObj, ctx.cols.aggregationFunctions, null, null, null);
                }
            }
            if (ctx.fetch_stored != null) {
                if (!ctx.fetch_stored.val && (ctx.cols != null && ctx.cols.fetchStored)) {
                    throw new IllegalStateException("FETCHING STORED cannot be false when _srcdata is selected.");
                } else if (ctx.fetch_stored.val) {
                    // Default is false
                    jsonObj.put("fetchStored", ctx.fetch_stored.val);
                }
            }
            if (ctx.route_param != null) {
                jsonObj.put("routeParam", ctx.route_param.val);
            }

            if (ctx.w != null) {
                extractSelectionInfo((JSONObject)ctx.w.json, selections, filter, query);
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
                jsonObj.put("facetInit", ctx.given.json);
            }

            if (ctx.rel_model != null) {
                JSONObject queryPred = jsonObj.optJSONObject("query");
                JSONObject query_string = null;
                if (queryPred != null) {
                    query_string = (JSONObject)queryPred.get("query_string");
                }
                if (query_string != null) {
                    queryPred = new FastJSONObject().put("query_string",
                                                         query_string.put("relevance", ctx.rel_model.json));
                } else {
                    queryPred = new FastJSONObject().put("query_string",
                                                         new FastJSONObject().put("query", "")
                        .put("relevance", ctx.rel_model.json));
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
            String colName = col.text;
            if (colName != null) {
                jsonProperty.put(ctx, col.text);
                if ("_srcdata".equals(colName) || colName.startsWith("_srcdata.")) {
                    fetchStoredProperty.put(ctx, true);
                }
            }
        }

        for (BQLv4Parser.Aggregation_functionContext agrFunction : ctx.aggregation_function()) {
            aggregationFunctionsProperty.get(ctx).add(new Pair<String, String>(agrFunction.function, agrFunction.column));
        }
    }

    @Override
    public void exitAggregation_function(BQLv4Parser.Aggregation_functionContext ctx) {
        functionProperty.put(ctx, ctx.id.text);
        if (ctx.columnVar != null) {
            columnProperty.put(ctx, ctx.columnVar.text);
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
            textProperty.put(ctx, ctx.colName.text);
        }
    }

    @Override
    public void exitWhere(BQLv4Parser.WhereContext ctx) {
        jsonProperty.put(ctx, jsonProperty.get(ctx.search_expr));
    }

    @Override
    public void exitOrder_by_clause(BQLv4Parser.Order_by_clauseContext ctx) {
        if (ctx.RELEVANCE() != null) {
            isRelevanceProperty.put(ctx, true);
        } else {
            isRelevanceProperty.put(ctx, false);
            jsonProperty.put(ctx, ctx.sort_specs.json);
        }
    }

    @Override
    public void exitSort_specs(BQLv4Parser.Sort_specsContext ctx) {
        JSONArray sortArray = new FastJSONArray();
        for (BQLv4Parser.Sort_specContext sort : ctx.sort_spec()) {
            sortArray.put(sort.json);
        }

        jsonProperty.put(ctx, sortArray);
    }

}
