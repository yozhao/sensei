package com.senseidb.extention.flexiblequery;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import com.senseidb.extention.compiler.JavaCompilerHelper;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

public class CustomMatchingStrategyBuilder {
  private static HashSet<String> importMap = Sets.newHashSet();
  private static Map<String, Class<?>> compiledClassMap = Maps.newConcurrentMap();

  static {
    importMap.add("com.senseidb.extention.flexiblequery.StrategyFactory");
    importMap.add("com.browseengine.bobo.facets.data.TermValueList");
    importMap.add("com.browseengine.bobo.util.BigSegmentedArray");
    importMap.add("com.senseidb.extention.flexiblequery.CustomMatchingStrategy");
    importMap.add("java.io.IOException");
    importMap.add("com.browseengine.bobo.facets.data.TermIntList");
    importMap.add("com.browseengine.bobo.facets.data.TermFloatList");
    importMap.add("com.browseengine.bobo.facets.data.TermStringList");
    importMap.add("com.browseengine.bobo.facets.data.FacetDataCache");
    importMap.add("org.apache.lucene.search.Explanation");
    importMap.add("org.apache.lucene.search.Query");
  }

  static public class FieldInfo {
    public String type;
    public String name;
    public FieldInfo(String type, String name) {
      this.type = type;
      this.name = name;
    }
  }

  private static final String CLASS_HEADER_FORMAT = "public class %s extends CustomMatchingStrategy";
  private static final String SCORE_METHOD_FORMAT =
      "  @Override\n" +
      "  public float score() throws IOException {\n" +
      "  %s\n" +
      "  }\n";
  private static final String INIT_METHOD =
      " @Override\n"
      + "public void init() {\n %s\n }\n";
  private static final String GET_FIELD_METHOD =
      " public %s %s() {\n"
      + "int idx = %sorderArray.get(doc());\n"
      + "return ((%s) %stermList).getPrimitiveValue(idx);\n"
      + "}\n";
  private static final String EXPLAIN_METHOD =
      " @Override\n"
      + "public Explanation explain(Query query, int doc) throws IOException {\n%s}\n";

  private static String getImportHeader() {
    StringWriter writer = new StringWriter();
    PrintWriter printer = new PrintWriter(writer);
    for (String pkg : importMap) {
      printer.printf("import %s;\n", pkg);
    }
    printer.flush();
    printer.close();
    return writer.toString();
  }
  public static CustomMatchingStrategy buildStragegy(String name, String fucntion, String explain, List<FieldInfo> fields) throws Exception {
    return buildStragegy(name, fucntion, explain, fields, false);
  }

  public static CustomMatchingStrategy buildStragegy(
      String name, String fucntion, String explain, List<FieldInfo> fields, boolean flush) throws Exception {
    String className = "CMS" + name + Math.abs(fucntion.hashCode());
    if (!Strings.isNullOrEmpty(explain))
      className += Math.abs(explain.hashCode());
    Class<?> clazz = compiledClassMap.get(className);
    if (!flush && clazz != null) {
      return (CustomMatchingStrategy) clazz.newInstance();
    } else {
      StringWriter classBody = new StringWriter();
      PrintWriter classPrinter = new PrintWriter(classBody);
      String classHeader = String.format(CLASS_HEADER_FORMAT, className);
      // header
      classPrinter.println(getImportHeader());
      classPrinter.println(classHeader + "{");

      // score method
      classPrinter.printf(SCORE_METHOD_FORMAT, fucntion);
      if (fields != null && !fields.isEmpty()) {
        StringBuffer initString = new StringBuffer();
        for (FieldInfo field : fields) {
          // add field;
          classPrinter.printf("private BigSegmentedArray %sorderArray;\n", field.name);
          classPrinter.printf("private TermValueList<?> %stermList;\n", field.name);

          // add init method body.
          initString.append(String.format(
              "%sorderArray = ((FacetDataCache<?>) (boboReader.getFacetData(\"%s\"))).orderArray;\n",
              field.name, field.name));
          initString.append(String.format("%stermList = ((FacetDataCache<?>) (boboReader.getFacetData(\"%s\"))).valArray;\n",
                                          field.name, field.name));

          // add get method.
          String type = "";
          if (field.type.equals("Integer")) {
            type = "TermIntList";
          } else if (field.type.equals("Float")) {
            type = "TermFloatList";
          } else {

          }
          classPrinter.printf(GET_FIELD_METHOD, field.type, field.name, field.name, type, field.name);
        }
        classPrinter.printf(INIT_METHOD, initString.toString());
      }

      if (!Strings.isNullOrEmpty(explain)) {
        classPrinter.printf(EXPLAIN_METHOD, explain);
      }

      classPrinter.println("}");

      System.out.println(classBody.toString());
      clazz = JavaCompilerHelper.createClass(className, classBody.toString());
      compiledClassMap.put(className, clazz);
      return (CustomMatchingStrategy) clazz.newInstance();
    }
  }
}
