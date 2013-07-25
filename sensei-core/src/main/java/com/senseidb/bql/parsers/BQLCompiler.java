package com.senseidb.bql.parsers;

import java.util.HashMap;
import java.util.Map;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.TokenStream;
import org.json.JSONObject;

public class BQLCompiler extends AbstractCompiler
{
  // A map containing facet type and data type info for a facet
  private Map<String, String[]> _facetInfoMap = new HashMap<String, String[]>();
  private final ThreadLocal<BQLParser> _parser = new ThreadLocal<BQLParser>();

  public BQLCompiler(Map<String, String[]> facetInfoMap)
  {
    _facetInfoMap = facetInfoMap;
  }

  @Override
  public JSONObject compile(String bqlStmt) throws RecognitionException
  {
    // Lexer splits input into tokens
    ANTLRStringStream input = new ANTLRStringStream(bqlStmt);
    TokenStream tokens = new CommonTokenStream(new BQLLexer(input));

    // Parser generates abstract syntax tree
    BQLParser parser = new BQLParser(tokens, _facetInfoMap);
    _parser.set(parser);
    BQLParser.statement_return ret = parser.statement();

    JSONObject json = (JSONObject) ret.json;
    // XXX To be removed
    // printTree(ast);
    // System.out.println(">>> json = " + json.toString());
    return json;
  }

  @Override
  public String getErrorMessage(RecognitionException error)
  {
    BQLParser parser = _parser.get();
    if (parser != null)
    {
      return parser.getErrorMessage(error, parser.getTokenNames());
    }
    else
    {
      return null;
    }
  }

  public void setFacetInfoMap(Map<String, String[]> facetInfoMap)
  {
    _facetInfoMap = facetInfoMap;
  }
}
