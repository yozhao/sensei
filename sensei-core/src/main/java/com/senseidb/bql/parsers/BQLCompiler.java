package com.senseidb.bql.parsers;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.BailErrorStrategy;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.misc.IntegerStack;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.antlr.v4.runtime.tree.TerminalNode;

import org.json.JSONObject;

public class BQLCompiler extends AbstractCompiler {
  // A map containing facet type and data type info for a facet
  private Map<String, String[]> _facetInfoMap = new HashMap<String, String[]>();
  private final ThreadLocal<BQLParser> _parser = new ThreadLocal<BQLParser>();

  public BQLCompiler(Map<String, String[]> facetInfoMap) {
    _facetInfoMap = facetInfoMap;
  }

  @Override
  public JSONObject compile(String bqlStmt) throws RecognitionException, ParseCancellationException {
    // Lexer splits input into tokens
    ANTLRInputStream input = new ANTLRInputStream(bqlStmt);
    TokenStream tokens = new CommonTokenStream(new BQLLexer(input));

    // Parser generates abstract syntax tree
    BQLParser parser = new BQLParser(tokens);
    _parser.set(parser);
    parser.setErrorHandler(new BailErrorStrategy());
    BQLParser.StatementContext ret = parser.statement();

    BQLCompilerAnalyzer analyzer = new BQLCompilerAnalyzer(parser, _facetInfoMap);
    ParseTreeWalker.DEFAULT.walk(analyzer, ret);
    JSONObject json = (JSONObject)analyzer.getJsonProperty(ret);

    // XXX To be removed
    // printTree(ast);
    // System.out.println(">>> json = " + json.toString());
    return json;
  }

  @Override
  public String getErrorMessage(RecognitionException error) {
    BQLParser parser = _parser.get();
    if (parser != null) {
      // TODO: get v4 error message
      return "TODO";
    } else {
      return null;
    }
  }

    public String getErrorMessage(ParseCancellationException error) {
        if (error.getCause() != null) {
            String message = error.getCause().getMessage();
            if (error.getCause() instanceof SemanticException) {
                SemanticException semanticException = (SemanticException)error.getCause();
                if (semanticException.getNode() != null) {
                    TerminalNode startNode = getStartNode(semanticException.getNode());
                    if (startNode != null) {
                        String prefix = String.format("[line:%d, col:%d] ", startNode.getSymbol().getLine(), startNode.getSymbol().getCharPositionInLine());
                        message = prefix + message;
                    }
                }

                return message;
            } else {
                return error.getCause().getMessage();
            }
        }

        return error.getMessage();
    }

  public void setFacetInfoMap(Map<String, String[]> facetInfoMap) {
    _facetInfoMap = facetInfoMap;
  }

    private static TerminalNode getStartNode(ParseTree tree) {
        if (tree instanceof TerminalNode) {
            return (TerminalNode)tree;
        }

        Deque<ParseTree> workList = new ArrayDeque<ParseTree>();
        IntegerStack workIndexStack = new IntegerStack();
        workList.push(tree);
        workIndexStack.push(0);
        while (!workList.isEmpty()) {
            ParseTree currentTree = workList.peek();
            int currentIndex = workIndexStack.peek();
            if (currentIndex == currentTree.getChildCount()) {
                workList.pop();
                workIndexStack.pop();
                continue;
            }

            // move work list to next child
            workIndexStack.push(workIndexStack.pop() + 1);

            // process the current child
            ParseTree child = currentTree.getChild(currentIndex);
            if (child instanceof TerminalNode) {
                return (TerminalNode)child;
            }

            workList.push(child);
            workIndexStack.push(0);
        }

        return null;
    }
}
