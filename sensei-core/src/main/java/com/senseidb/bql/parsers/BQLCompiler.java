package com.senseidb.bql.parsers;

import java.util.HashMap;
import java.util.Map;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.FailedPredicateException;
import org.antlr.v4.runtime.InputMismatchException;
import org.antlr.v4.runtime.NoViableAltException;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.json.JSONObject;

public class BQLCompiler extends AbstractCompiler {
  // A map containing facet type and data type info for a facet
  private Map<String, String[]> _facetInfoMap = new HashMap<String, String[]>();
  private final ThreadLocal<BQLParser> _parser = new ThreadLocal<BQLParser>();

  public BQLCompiler(Map<String, String[]> facetInfoMap) {
    _facetInfoMap = facetInfoMap;
  }

  @Override
  public JSONObject compile(String bqlStmt) {
    // Lexer splits input into tokens
    ANTLRInputStream input = new ANTLRInputStream(bqlStmt);
    TokenStream tokens = new CommonTokenStream(new BQLLexer(input));

    // Parser generates abstract syntax tree
    BQLParser parser = new BQLParser(tokens);
    _parser.set(parser);

    parser.removeErrorListeners();
    parser.addErrorListener(BQLErrorListener.INSTANCE);

    parser.setErrorHandler(new BailErrorStrategy());
    BQLParser.StatementContext ret = parser.statement();

    BQLCompilerAnalyzer analyzer = new BQLCompilerAnalyzer(parser, _facetInfoMap);
    ParseTreeWalker.DEFAULT.walk(analyzer, ret);
    JSONObject json = (JSONObject) analyzer.getJsonProperty(ret);

    return json;
  }

  @Override
  public String getErrorMessage(IllegalStateException ex) {
    String errMsg = null;
    if (ex instanceof ParseCancellationException) {
      BQLParser parser = _parser.get();
      Throwable err = ex.getCause();
      if (err instanceof NoViableAltException) {
        NoViableAltException nvae = (NoViableAltException) err;
        int line = nvae.getOffendingToken().getLine();
        TokenStream tokens = parser.getInputStream();
        String input;
        if (tokens != null) {
          if (nvae.getStartToken().getType() == Token.EOF) {
            input = "<EOF>";
          } else {
            input = tokens.getText(nvae.getStartToken(), nvae.getOffendingToken());
          }
        } else {
          input = "<unknown input>";
        }
        errMsg = "[line " + line + "] no viable alternative at input " + escapeWSAndQuote(input);
      } else if (err instanceof InputMismatchException) {
        InputMismatchException ime = (InputMismatchException) err;
        int line = ime.getOffendingToken().getLine();
        errMsg = "[line " + line + "] mismatched input "
            + getTokenErrorDisplay(ime.getOffendingToken()) + " expecting "
            + ime.getExpectedTokens().toString(parser.getTokenNames());
      } else if (err instanceof FailedPredicateException) {
        FailedPredicateException fpe = (FailedPredicateException) err;
        int line = fpe.getOffendingToken().getLine();
        String ruleName = parser.getRuleNames()[parser.getContext().getRuleIndex()];
        errMsg = "[line " + line + "] rule " + ruleName + " " + err.getMessage();
      }
    }
    if (errMsg == null) {
      errMsg = ex.getMessage();
    }
    if (errMsg == null) {
      errMsg = "Unknown parsing error.";
    }
    return errMsg;
  }

  private String getTokenErrorDisplay(Token t) {
    if (t == null) return "<no token>";
    String s = t.getText();
    if (s == null) {
      if (t.getType() == Token.EOF) {
        s = "<EOF>";
      } else {
        s = "<" + t.getType() + ">";
      }
    }
    return escapeWSAndQuote(s);
  }

  private String escapeWSAndQuote(String s) {
    s = s.replace("\n", "\\n");
    s = s.replace("\r", "\\r");
    s = s.replace("\t", "\\t");
    return "'" + s + "'";
  }

  public void setFacetInfoMap(Map<String, String[]> facetInfoMap) {
    _facetInfoMap = facetInfoMap;
  }
}
