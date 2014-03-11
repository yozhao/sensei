package com.senseidb.bql.parsers;

import org.antlr.v4.runtime.tree.ParseTree;
import org.json.JSONObject;

public abstract class AbstractCompiler {

  public AbstractCompiler() {
    super();
  }

  public abstract JSONObject compile(String expression);

  public abstract String getErrorMessage(IllegalStateException ex);

  protected void printTree(ParseTree ast) {
    print(ast, 0);
  }

  private void print(ParseTree tree, int level) {
    // Indent level
    for (int i = 0; i < level; i++) {
      System.out.print("--");
    }

    if (tree == null) {
      System.out.println(" null tree.");
      return;
    }

    // Print node description: type code followed by token text
    // TODO: what "type" should print?
    System.out.println(" " + "type?" + " " + tree.getText());

    // Print all children
    if (tree.getChildCount() != 0) {
      for (int i = 0; i < tree.getChildCount(); i++) {
        print(tree.getChild(i), level + 1);
      }
    }
  }

}
