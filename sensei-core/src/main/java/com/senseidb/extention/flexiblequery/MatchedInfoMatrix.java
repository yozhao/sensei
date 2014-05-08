package com.senseidb.extention.flexiblequery;

public class MatchedInfoMatrix {
  private TermMatchedInfo[][] values;
  private float[] fieldBoost;
  private int doc;

  public MatchedInfoMatrix(TermMatchedInfo[][] matchedInfos, float[] fieldBoost) {
    values = matchedInfos;
    this.fieldBoost = fieldBoost;
  }

  public void setDoc(int doc) {
    this.doc = doc;
  }

  public int doc() { return doc; }

  public TermMatchedInfo get(int x, int y) {
    return values[x][y];
  }

  public int getFieldLength() { return values.length; }

  public int getTermLength(int field) {
    return values[field].length;
  }

  public float getFieldBoost(int field) { return fieldBoost[field]; }

}
