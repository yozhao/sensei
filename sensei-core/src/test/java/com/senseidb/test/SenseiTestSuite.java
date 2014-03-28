package com.senseidb.test;

import junit.framework.Test;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

import org.junit.Ignore;

@Ignore
public class SenseiTestSuite extends TestSuite {
  public static Test suite() {
    TestSuite suite = new TestSuite();
    suite.addTestSuite(TestSensei.class);
    suite.addTestSuite(TestIndexingAPI.class);
    return suite;
  }

  /**
   * @param args
   */
  public static void main(String[] args) {
    TestRunner.run(suite());
  }
}
