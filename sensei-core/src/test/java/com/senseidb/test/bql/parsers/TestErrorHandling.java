package com.senseidb.test.bql.parsers;

import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.json.JSONObject;
import org.junit.Test;

import com.senseidb.bql.parsers.BQLCompiler;

public class TestErrorHandling extends TestCase {

  private final BQLCompiler _compiler;
  public TestErrorHandling() {
    super();
    Map<String, String[]> facetInfoMap = new HashMap<String, String[]>();
    facetInfoMap.put("tags", new String[] { "multi", "string" });
    facetInfoMap.put("category", new String[] { "simple", "string" });
    facetInfoMap.put("price", new String[] { "range", "float" });
    facetInfoMap.put("mileage", new String[] { "range", "int" });
    facetInfoMap.put("color", new String[] { "simple", "string" });
    facetInfoMap.put("year", new String[] { "range", "int" });
    facetInfoMap.put("makemodel", new String[] { "path", "string" });
    facetInfoMap.put("city", new String[] { "path", "string" });
    facetInfoMap.put("long_id", new String[] { "simple", "long" });
    facetInfoMap.put("time", new String[] { "custom", "" }); // Mimic a custom facet
    _compiler = new BQLCompiler(facetInfoMap);
  }

  @SuppressWarnings("unused")
  @Test
  public void testBasicError1() throws Exception {
    System.out.println("testBasicError1");
    System.out.println("==================================================");

    boolean caughtException = false;
    try {
      // Incomplete where clause
      JSONObject json = _compiler.compile("select category " + "from cars " + "where");
    } catch (IllegalStateException err) {
      caughtException = true;
      assertTrue(_compiler.getErrorMessage(err).startsWith(
        "[line 1] no viable alternative at input '<EOF>'"));
    } finally {
      assertTrue(caughtException);
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void testInconsistentRanges() throws Exception {
    System.out.println("testInconsistentRanges");
    System.out.println("==================================================");

    boolean caughtException = false;
    try {
      JSONObject json = _compiler.compile("SELECT category \n" + "FROM cars \n"
          + "WHERE color = 'red' \n" + "  AND year > 2000 AND year < 1995 \n"
          + "  OR price < 1750.00");
    } catch (IllegalStateException err) {
      caughtException = true;
      assertEquals("[line 4] Inconsistent ranges detected for column: year",
        _compiler.getErrorMessage(err));
    } finally {
      assertTrue(caughtException);
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void testInvalidInPred() throws Exception {
    System.out.println("testInvalidInPred");
    System.out.println("==================================================");

    boolean caughtException = false;
    try {
      JSONObject json = _compiler.compile("SELECT category \n" + "FROM cars \n"
          + "WHERE year in (1995, 2000) \n" + "  AND price < 1750.00");
    } catch (IllegalStateException err) {
      caughtException = true;
      assertEquals(
        "[line 3] Range facet \"year\" cannot be used in IN predicates.",
        _compiler.getErrorMessage(err));
    } finally {
      assertTrue(caughtException);
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void testInvalidInPredValues() throws Exception {
    System.out.println("testInvalidInPredValues");
    System.out.println("==================================================");

    boolean caughtException = false;
    try {
      JSONObject json = _compiler.compile("SELECT category \n" + "FROM cars \n"
          + "WHERE color in ('red', 2000, 'blue') \n" + "  AND price < 1750.00");
    } catch (IllegalStateException err) {
      caughtException = true;
      assertEquals(
        "[line 3] Value list for IN predicate of facet \"color\" contains incompatible value(s).",
        _compiler.getErrorMessage(err));
    } finally {
      assertTrue(caughtException);
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void testInvalidInPredExceptValues() throws Exception {
    System.out.println("testInvalidInPredExceptValues");
    System.out.println("==================================================");

    boolean caughtException = false;
    try {
      JSONObject json = _compiler.compile("SELECT category \n" + "FROM cars \n"
          + "WHERE color IN ('red', 'blue') EXCEPT ('black', 2000) \n" + "  AND price < 1750.00");
    } catch (IllegalStateException err) {
      caughtException = true;
      assertEquals(
        "[line 3] EXCEPT value list for IN predicate of facet \"color\" contains incompatible value(s).",
        _compiler.getErrorMessage(err));
    } finally {
      assertTrue(caughtException);
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void testInvalidContainsAllPred() throws Exception {
    System.out.println("testInvalidContainsAllPred");
    System.out.println("==================================================");

    boolean caughtException = false;
    try {
      JSONObject json = _compiler.compile("SELECT category \n" + "FROM cars \n"
          + "WHERE year contains all (1995, 2000) \n" + "  AND price < 1750.00");
    } catch (IllegalStateException err) {
      caughtException = true;
      assertEquals(
        "[line 3] Range facet column \"year\" cannot be used in CONTAINS ALL predicates.",
        _compiler.getErrorMessage(err));
    } finally {
      assertTrue(caughtException);
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void testInvalidContainsAllPredValues() throws Exception {
    System.out.println("testInvalidContainsAllPredValues");
    System.out.println("==================================================");

    boolean caughtException = false;
    try {
      JSONObject json = _compiler.compile("SELECT category \n" + "FROM cars \n"
          + "WHERE tags CONTAINS ALL ('cool', 175.50, 'hybrid') \n" + "  AND price < 1750.00");
    } catch (IllegalStateException err) {
      caughtException = true;
      assertEquals(
        "[line 3] Value list for CONTAINS ALL predicate of facet \"tags\" contains incompatible value(s).",
        _compiler.getErrorMessage(err));
    } finally {
      assertTrue(caughtException);
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void testInvalidContainsAllPredExceptValues() throws Exception {
    System.out.println("testInvalidContainsAllPredExceptValues");
    System.out.println("==================================================");

    boolean caughtException = false;
    try {
      JSONObject json = _compiler.compile("SELECT category \n" + "FROM cars \n"
          + "WHERE tags contains all ('cool', 'hybrid') EXCEPT ('moon-roof', 2000) \n"
          + "  AND price < 1750.00");
    } catch (IllegalStateException err) {
      caughtException = true;
      assertEquals(
        "[line 3] EXCEPT value list for CONTAINS ALL predicate of facet \"tags\" contains incompatible value(s).",
        _compiler.getErrorMessage(err));
    } finally {
      assertTrue(caughtException);
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void testBadDataInEqualPred() throws Exception {
    System.out.println("testBadDataInEqualPred");
    System.out.println("==================================================");

    boolean caughtException = false;
    try {
      JSONObject json = _compiler.compile("SELECT category \n" + "FROM cars \n"
          + "WHERE color = 1234 \n" + "  AND price < 1750.00");
    } catch (IllegalStateException err) {
      caughtException = true;
      assertEquals(
        "[line 3] Incompatible data type was found in an EQUAL predicate for column \"color\".",
        _compiler.getErrorMessage(err));
    } finally {
      assertTrue(caughtException);
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void testExpectingCOLON() throws Exception {
    System.out.println("testExpectingCOLON");
    System.out.println("==================================================");

    boolean caughtException = false;
    try {
      JSONObject json = _compiler.compile("SELECT category \n" + "FROM cars \n"
          + "WHERE city = 'u.s.a./new york' WITH('strict', true) \n" + "  AND price < 1750.00");
    } catch (IllegalStateException err) {
      caughtException = true;
      assertEquals("[line 3] mismatched input ',' expecting ':'", _compiler.getErrorMessage(err));
    } finally {
      assertTrue(caughtException);
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void testUnsupportedProp() throws Exception {
    System.out.println("testUnsupportedProp");
    System.out.println("==================================================");

    boolean caughtException = false;
    try {
      JSONObject json = _compiler.compile("SELECT category \n" + "FROM cars \n"
          + "WHERE city = 'u.s.a./new york' WITH('ddd':123, 'strict':true) \n"
          + "  AND price < 1750.00");
    } catch (IllegalStateException err) {
      caughtException = true;
      assertEquals(
        "[line 3] Unsupported property was found in an EQUAL predicate for path facet column \"city\": ddd.",
        _compiler.getErrorMessage(err));
    } finally {
      assertTrue(caughtException);
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void testBadDataInNotEqualPred() throws Exception {
    System.out.println("testBadDataInNotEqualPred");
    System.out.println("==================================================");

    boolean caughtException = false;
    try {
      JSONObject json = _compiler.compile("SELECT category \n" + "FROM cars \n"
          + "WHERE color <> 1234 \n" + "  AND price < 1750.00");
    } catch (IllegalStateException err) {
      caughtException = true;
      assertEquals(
        "[line 3] Incompatible data type was found in a NOT EQUAL predicate for column \"color\".",
        _compiler.getErrorMessage(err));
    } finally {
      assertTrue(caughtException);
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void testNotEqualOnPath() throws Exception {
    System.out.println("testNotEqualOnPath");
    System.out.println("==================================================");

    boolean caughtException = false;
    try {
      JSONObject json = _compiler.compile("SELECT category \n" + "FROM cars \n"
          + "WHERE city <> 'u.s.a./new york' \n" + "  AND price < 1750.00");
    } catch (IllegalStateException err) {
      caughtException = true;
      assertEquals(
        "[line 3] NOT EQUAL predicate is not supported for path facets (column \"city\").",
        _compiler.getErrorMessage(err));
    } finally {
      assertTrue(caughtException);
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void testBadBetweenPred() throws Exception {
    System.out.println("testBadBetweenPred");
    System.out.println("==================================================");

    boolean caughtException = false;
    try {
      JSONObject json = _compiler.compile("SELECT category \n" + "FROM cars \n"
          + "WHERE city BETWEEN 'blue' AND 'red' \n" + "  AND price < 1750.00");
    } catch (IllegalStateException err) {
      caughtException = true;
      assertEquals(
        "[line 3] Non-rangable facet column \"city\" cannot be used in BETWEEN predicates.",
        _compiler.getErrorMessage(err));
    } finally {
      assertTrue(caughtException);
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void testBadDataInBetweenPred() throws Exception {
    System.out.println("testBadDataInBetweenPred");
    System.out.println("==================================================");

    boolean caughtException = false;
    try {
      JSONObject json = _compiler.compile("SELECT category \n" + "FROM cars \n"
          + "WHERE year BETWEEN 'blue' AND 2000 \n" + "  AND price < 1750.00");
    } catch (IllegalStateException err) {
      caughtException = true;
      assertEquals(
        "[line 3] Incompatible data type was found in a BETWEEN predicate for column \"year\".",
        _compiler.getErrorMessage(err));
    } finally {
      assertTrue(caughtException);
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void testBadRangePred() throws Exception {
    System.out.println("testBadRangePred");
    System.out.println("==================================================");

    boolean caughtException = false;
    try {
      JSONObject json = _compiler.compile("SELECT category \n" + "FROM cars \n"
          + "WHERE city > 'red' \n" + "  AND price < 1750.00");
    } catch (IllegalStateException err) {
      caughtException = true;
      assertEquals(
        "[line 3] Non-rangable facet column \"city\" cannot be used in RANGE predicates.",
        _compiler.getErrorMessage(err));
    } finally {
      assertTrue(caughtException);
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void testBadDataInRangePred() throws Exception {
    System.out.println("testBadDataInRangePred");
    System.out.println("==================================================");

    boolean caughtException = false;
    try {
      JSONObject json = _compiler.compile("SELECT category \n" + "FROM cars \n"
          + "WHERE year > 'red' \n" + "  AND price < 1750.00");
    } catch (IllegalStateException err) {
      caughtException = true;
      assertEquals(
        "[line 3] Incompatible data type was found in a RANGE predicate for column \"year\".",
        _compiler.getErrorMessage(err));
    } finally {
      assertTrue(caughtException);
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void testBadDatetime1() throws Exception {
    System.out.println("testBadDatetime1");
    System.out.println("==================================================");

    boolean caughtException = false;
    try {
      JSONObject json = _compiler.compile("SELECT category \n" + "FROM cars \n"
          + "WHERE time > 2011-16-20 55:10:10 \n" + "  AND price < 1750.00");
    } catch (IllegalStateException err) {
      caughtException = true;
      assertEquals(
        "[line 3] Date string contains invalid date/time: \"2011-16-20 55:10:10\".",
        _compiler.getErrorMessage(err));
    } finally {
      assertTrue(caughtException);
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void testBadDatetime2() throws Exception {
    System.out.println("testBadDatetime2");
    System.out.println("==================================================");

    boolean caughtException = false;
    try {
      JSONObject json = _compiler.compile("SELECT category \n" + "FROM cars \n"
          + "WHERE time > 2011-10/20 \n" + "  AND price < 1750.00");
    } catch (IllegalStateException err) {
      caughtException = true;
      assertEquals(
        "[line 3] ParseException happened for \"2011-10/20\": Unparseable date: \"2011-10/20\".",
        _compiler.getErrorMessage(err));
    } finally {
      assertTrue(caughtException);
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void testBadMatchPred() throws Exception {
    System.out.println("testBadMatchPred");
    System.out.println("==================================================");

    boolean caughtException = false;
    try {
      JSONObject json = _compiler.compile("SELECT color \n" + "FROM cars \n"
          + "WHERE MATCH(color, year) AGAINST('text1 AND text2') \n" + "  AND price < 1750.00");
    } catch (IllegalStateException err) {
      caughtException = true;
      assertEquals(
        "[line 3] Non-string type column \"year\" cannot be used in MATCH AGAINST predicates.",
        _compiler.getErrorMessage(err));
    } finally {
      assertTrue(caughtException);
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void testEOF() throws Exception {
    System.out.println("testEOF");
    System.out.println("==================================================");

    boolean caughtException = false;
    try {
      JSONObject json = _compiler.compile("select color, year from where year > 1");
    } catch (IllegalStateException err) {
      caughtException = true;
      assertEquals("[line 1] mismatched input 'where' expecting {STRING_LITERAL, IDENT}",
        _compiler.getErrorMessage(err));
    } finally {
      assertTrue(caughtException);
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void testBadSelectList() throws Exception {
    System.out.println("testBadSelectList");
    System.out.println("==================================================");

    boolean caughtException = false;
    try {
      JSONObject json = _compiler.compile("select color, from aa where color = 'red'");
    } catch (IllegalStateException err) {
      caughtException = true;
      assertTrue(_compiler.getErrorMessage(err).startsWith(
        "[line 1] no viable alternative at input 'from'"));
    } finally {
      assertTrue(caughtException);
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void testOrderByOnce() throws Exception {
    System.out.println("testOrderByOnce");
    System.out.println("==================================================");

    boolean caughtException = false;
    try {
      JSONObject json = _compiler.compile("select category \n" + "from cars \n"
          + "order by color \n" + "order by year \n" + "limit 10");
    } catch (IllegalStateException err) {
      caughtException = true;
      assertEquals("[line 5] ORDER BY clause can only appear once.",
        _compiler.getErrorMessage(err));
    } finally {
      assertTrue(caughtException);
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void testLimitOnce() throws Exception {
    System.out.println("testLimitOnce");
    System.out.println("==================================================");

    boolean caughtException = false;
    try {
      JSONObject json = _compiler.compile("select category \n" + "from cars \n" + "limit 10, 20 \n"
          + "limit 10 \n" + "order by color \n");
    } catch (IllegalStateException err) {
      caughtException = true;
      assertEquals("[line 5] LIMIT clause can only appear once.",
        _compiler.getErrorMessage(err));
    } finally {
      assertTrue(caughtException);
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void testBadTimePredicate() throws Exception {
    System.out.println("testBadTimePredicate");
    System.out.println("==================================================");

    boolean caughtException = false;
    try {
      JSONObject json = _compiler.compile("select category \n" + "from cars \n"
          + "where city IN LAST 2 days");
    } catch (IllegalStateException err) {
      caughtException = true;
      assertEquals(
        "[line 3] Non-rangable facet column \"city\" cannot be used in TIME predicates.",
        _compiler.getErrorMessage(err));
    } finally {
      assertTrue(caughtException);
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void testOverflowInteger() throws Exception {
    System.out.println("testOverflowInteger");
    System.out.println("==================================================");

    boolean caughtException = false;
    try {
      JSONObject json = _compiler.compile("select category \n" + "from cars \n"
          + "where year = 12345678901234567890");
    } catch (IllegalStateException err) {
      caughtException = true;
      assertEquals(
        "[line 3] Hit NumberFormatException: For input string: \"12345678901234567890\"",
        _compiler.getErrorMessage(err));
    } finally {
      assertTrue(caughtException);
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void testRouteByOnce() throws Exception {
    System.out.println("testRouteByOnce");
    System.out.println("==================================================");

    boolean caughtException = false;
    try {
      JSONObject json = _compiler.compile("select category \n" + "from cars \n"
          + "route by '1234' \n" + "route by '9999'");
    } catch (IllegalStateException err) {
      caughtException = true;
      assertEquals("[line 4] ROUTE BY clause can only appear once.",
        _compiler.getErrorMessage(err));
    } finally {
      assertTrue(caughtException);
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void testSrcdataFetchStoredError1() throws Exception {
    System.out.println("testSrcdataFetchStoredError1");
    System.out.println("==================================================");

    boolean caughtException = false;
    try {
      JSONObject json = _compiler.compile("select _srcdata.category \n" + "from cars \n"
          + "fetching stored false");
    } catch (IllegalStateException err) {
      caughtException = true;
      assertEquals(
        "[line 3] FETCHING STORED cannot be false when _srcdata is selected.",
        _compiler.getErrorMessage(err));
    } finally {
      assertTrue(caughtException);
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void testSrcdataFetchStoredError2() throws Exception {
    System.out.println("testSrcdataFetchStoredError2");
    System.out.println("==================================================");

    boolean caughtException = false;
    try {
      JSONObject json = _compiler.compile("select _srcdata, color \n" + "from cars \n"
          + "fetching stored false");
    } catch (IllegalStateException err) {
      caughtException = true;
      assertEquals(
        "[line 3] FETCHING STORED cannot be false when _srcdata is selected.",
        _compiler.getErrorMessage(err));
    } finally {
      assertTrue(caughtException);
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void testUsingRelevanceOnce() throws Exception {
    System.out.println("testUsingRelevanceOnce");
    System.out.println("==================================================");

    boolean caughtException = false;
    try {
      JSONObject json = _compiler.compile("select category \n" + "from cars \n"
          + "using relevance model md1 (srcid:1234) \n"
          + "using relevance model md2 (param1:'abc')");
    } catch (IllegalStateException err) {
      caughtException = true;
      assertEquals(
        "[line 4] USING RELEVANCE MODEL clause can only appear once.",
        _compiler.getErrorMessage(err));
    } finally {
      assertTrue(caughtException);
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void testRelevanceVarRedefined() throws Exception {
    System.out.println("testRelevanceVarRedefined");
    System.out.println("==================================================");

    boolean caughtException = false;
    try {
      JSONObject json = _compiler.compile("SELECT * \n" + "FROM cars \n"
          + "USING RELEVANCE MODEL md1 (srcid:1234) \n" + "  DEFINED AS (int srcid) \n"
          + "  BEGIN \n" + "    int x, y; \n" + "    short x = 5; \n" + "    return 0.5; \n"
          + "  END");
    } catch (IllegalStateException err) {
      caughtException = true;
      assertEquals("[line 7] Variable \"x\" is already defined.",
        _compiler.getErrorMessage(err));
    } finally {
      assertTrue(caughtException);
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void testRelevanceUndefinedVar1() throws Exception {
    System.out.println("testRelevanceUndefinedVar1");
    System.out.println("==================================================");

    boolean caughtException = false;
    try {
      JSONObject json = _compiler.compile("SELECT * \n" + "FROM cars \n"
          + "USING RELEVANCE MODEL md1 (srcid:1234) \n" + "  DEFINED AS (int srcid) \n"
          + "  BEGIN \n" + "    if (x == 5) \n" + "      return 0.1; \n" + "    return 0.5; \n"
          + "  END");
    } catch (IllegalStateException err) {
      caughtException = true;
      assertEquals("[line 6] Variable or class \"x\" is not defined.",
        _compiler.getErrorMessage(err));
    } finally {
      assertTrue(caughtException);
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void testRelevanceUndefinedVar2() throws Exception {
    System.out.println("testRelevanceUndefinedVar2");
    System.out.println("==================================================");

    boolean caughtException = false;
    try {
      JSONObject json = _compiler.compile("SELECT * \n" + "FROM cars \n"
          + "USING RELEVANCE MODEL md1 (srcid:1234) \n" + "  DEFINED AS (int srcid) \n"
          + "  BEGIN \n" + "    int x = 5; \n" + "    if (price > 2000.0) \n"
          + "      return 0.1; \n" + "    else { \n" + "      x = 10; \n" + "      y = x + 123; \n"
          + "    } \n" + "    return 0.5; \n" + "  END");
    } catch (IllegalStateException err) {
      caughtException = true;
      assertEquals("[line 11] Variable or class \"y\" is not defined.",
        _compiler.getErrorMessage(err));
    } finally {
      assertTrue(caughtException);
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void testRelevanceUndefinedVar3() throws Exception {
    System.out.println("testRelevanceUndefinedVar3");
    System.out.println("==================================================");

    boolean caughtException = false;
    try {
      JSONObject json = _compiler.compile("SELECT * \n" + "FROM cars \n"
          + "USING RELEVANCE MODEL md1 (srcid:1234) \n" + "  DEFINED AS (int srcid) \n"
          + "  BEGIN \n" + "    int total = 0; \n" + "    for (int i = 0; i < 10; ++i) { \n"
          + "      total += i; \n" + "    } \n" + "    i = 100; \n" + "  END");
    } catch (IllegalStateException err) {
      caughtException = true;
      assertEquals("[line 10] Variable or class \"i\" is not defined.",
        _compiler.getErrorMessage(err));
    } finally {
      assertTrue(caughtException);
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void testRelevanceVarDeclError1() throws Exception {
    System.out.println("testRelevanceVarDeclError1");
    System.out.println("==================================================");

    boolean caughtException = false;
    try {
      JSONObject json = _compiler.compile("SELECT * \n" + "FROM cars \n"
          + "USING RELEVANCE MODEL md1 (srcid:1234) \n" + "  DEFINED AS (int srcid) \n"
          + "  BEGIN \n" + "    int year; \n" + "  END");
    } catch (IllegalStateException err) {
      caughtException = true;
      assertEquals(
        "[line 6] Facet name \"year\" cannot be used to declare a variable.",
        _compiler.getErrorMessage(err));
    } finally {
      assertTrue(caughtException);
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void testRelevanceVarDeclError2() throws Exception {
    System.out.println("testRelevanceVarDeclError2");
    System.out.println("==================================================");

    boolean caughtException = false;
    try {
      JSONObject json = _compiler.compile("SELECT * \n" + "FROM cars \n"
          + "USING RELEVANCE MODEL md1 (srcid:1234) \n" + "  DEFINED AS (int srcid) \n"
          + "  BEGIN \n" + "    String _NOW; \n" + "  END");
    } catch (IllegalStateException err) {
      caughtException = true;
      assertEquals(
        "[line 6] Internal variable \"_NOW\" cannot be re-used to declare another variable.",
        _compiler.getErrorMessage(err));
    } finally {
      assertTrue(caughtException);
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void testRelevanceVarDeclError3() throws Exception {
    System.out.println("testRelevanceVarDeclError3");
    System.out.println("==================================================");

    boolean caughtException = false;
    try {
      JSONObject json = _compiler.compile("SELECT * \n" + "FROM cars \n"
          + "USING RELEVANCE MODEL md1 (srcid:1234) \n" + "  DEFINED AS (int srcid) \n"
          + "  BEGIN \n" + "    int x = 100; \n" + "    for (int i = 1; i < 10; ++i) { \n"
          + "      int x; \n" + "    } \n" + "  END");
    } catch (IllegalStateException err) {
      caughtException = true;
      assertEquals("[line 8] Variable \"x\" is already defined.",
        _compiler.getErrorMessage(err));
    } finally {
      assertTrue(caughtException);
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void testRelevanceModelParamError1() throws Exception {
    System.out.println("testRelevanceModelParamError1");
    System.out.println("==================================================");

    boolean caughtException = false;
    try {
      JSONObject json = _compiler.compile("SELECT * \n" + "FROM cars \n"
          + "USING RELEVANCE MODEL md1 (srcid:1234) \n"
          + "  DEFINED AS (int srcid, float price) \n" + "  BEGIN \n" + "    return 0.5; \n"
          + "  END");
    } catch (IllegalStateException err) {
      caughtException = true;
      assertEquals(
        "[line 4] Facet name \"price\" cannot be used as a relevance model parameter.",
        _compiler.getErrorMessage(err));
    } finally {
      assertTrue(caughtException);
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void testRelevanceModelParamError2() throws Exception {
    System.out.println("testRelevanceModelParamError2");
    System.out.println("==================================================");

    boolean caughtException = false;
    try {
      JSONObject json = _compiler.compile("SELECT * \n" + "FROM cars \n"
          + "USING RELEVANCE MODEL md1 (srcid:1234) \n"
          + "  DEFINED AS (int srcid, String srcid) \n" + "  BEGIN \n" + "    return 0.5; \n"
          + "  END");
    } catch (IllegalStateException err) {
      caughtException = true;
      assertEquals("[line 4] Parameter name \"srcid\" has already been used.",
        _compiler.getErrorMessage(err));
    } finally {
      assertTrue(caughtException);
    }
  }

  @SuppressWarnings("unused")
  @Test
  public void testRelevanceModelParamError3() throws Exception {
    System.out.println("testRelevanceModelParamError3");
    System.out.println("==================================================");

    boolean caughtException = false;
    try {
      JSONObject json = _compiler.compile("SELECT * \n" + "FROM cars \n"
          + "USING RELEVANCE MODEL md1 (srcid:1234) \n" + "  DEFINED AS (int srcid, long _NOW) \n"
          + "  BEGIN \n" + "    return 0.5; \n" + "  END");
    } catch (IllegalStateException err) {
      caughtException = true;
      assertEquals(
        "[line 4] Internal variable \"_NOW\" cannot be used as a relevance model parameter.",
        _compiler.getErrorMessage(err));
    } finally {
      assertTrue(caughtException);
    }
  }

}
