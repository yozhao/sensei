package com.senseidb.bql.parsers;

import java.util.HashMap;
import java.util.Map;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;

import org.json.JSONObject;

public class BQLCompiler extends AbstractCompiler {
  // A map containing facet type and data type info for a facet
  private Map<String, String[]> _facetInfoMap = new HashMap<String, String[]>();
  private final ThreadLocal<BQLv4Parser> _parser = new ThreadLocal<BQLv4Parser>();

  public BQLCompiler(Map<String, String[]> facetInfoMap) {
    _facetInfoMap = facetInfoMap;
  }

  @Override
  public JSONObject compile(String bqlStmt) throws RecognitionException {
    // Lexer splits input into tokens
    ANTLRInputStream input = new ANTLRInputStream(bqlStmt);
    TokenStream tokens = new CommonTokenStream(new BQLv4Lexer(input));

    // Parser generates abstract syntax tree
    BQLv4Parser parser = new BQLv4Parser(tokens);
    _parser.set(parser);
    BQLv4Parser.StatementContext ret = parser.statement();

    BQLv4CompilerAnalyzer analyzer = new BQLv4CompilerAnalyzer(_facetInfoMap);
    ParseTreeWalker.DEFAULT.walk(analyzer, ret);
    JSONObject json = (JSONObject)analyzer.getJsonProperty(ret);

    // XXX To be removed
    // printTree(ast);
    // System.out.println(">>> json = " + json.toString());
    return json;
  }

  @Override
  public String getErrorMessage(RecognitionException error) {
    BQLv4Parser parser = _parser.get();
    if (parser != null) {
      // TODO: get v4 error message
      return "TODO";
    } else {
      return null;
    }
  }

  public void setFacetInfoMap(Map<String, String[]> facetInfoMap) {
    _facetInfoMap = facetInfoMap;
  }
}
