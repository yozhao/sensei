package com.senseidb.extention.flexiblequery;

import com.browseengine.bobo.api.BoboSegmentReader;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;

import java.io.IOException;

// not thread safe.
abstract public class CustomMatchingStrategy {
  private MatchedInfoMatrix matchedMatrix;
  protected BoboSegmentReader boboReader = null;

  public void init() {}

  public void setMatchedMatrix(MatchedInfoMatrix matchedMatrix) {
    this.matchedMatrix = matchedMatrix;
  }

  public void setBoboReader(BoboSegmentReader reader) {
    this.boboReader = reader;
  }

  public TermMatchedInfo termMatchedInfo(int field, int term) {
    return matchedMatrix.get(field, term);
  }

  public int doc() { return matchedMatrix.doc(); }

  public int getFieldLength() { return matchedMatrix.getFieldLength(); }

  public float getFieldBoost(int field) { return matchedMatrix.getFieldBoost(field); }

  public int getTermLength(int field) { return matchedMatrix.getTermLength(field); }

  public boolean isMatched(int field, int term) {
    return termMatchedInfo(field, term).isMatched();
  }

  public float getScore(int field, int term) throws IOException {
    return getRawScore(field, term) * getFieldBoost(field);
  }

  public float getRawScore(int field, int term) throws IOException {
    return termMatchedInfo(field, term).score();
  }

  public Explanation explain(int field, int term, Query query) {
    return termMatchedInfo(field, term).explain(query);
  }

  public String field(int field, int term) {
    return termMatchedInfo(field, term).getTerm().field();
  }

  public String text(int field, int term) {
    return termMatchedInfo(field, term).getTerm().text();
  }

  public int position(int field, int term) {
    return termMatchedInfo(field, term).position();
  }

  public Explanation explain(Query query, int doc) throws IOException {
    Explanation expl = new Explanation();
    expl.setDescription("FlexibleWeight");
    for (int i = 0; i < getFieldLength(); ++i) {
      Explanation subExpl = new Explanation();
      int matched = 0;
      for (int j = 0; j < getTermLength(i); ++j) {
        if (isMatched(i, j)) {
          Explanation result = new Explanation();
          String desp = String.format("%.2f * %.1f -- (%s)", getRawScore(i, j), getFieldBoost(i),
                                      text(i, j));
          result.setDescription(desp);
          result.setValue(getScore(i, j));
          subExpl.addDetail(result);
          ++matched;
        }
      }
      subExpl.setDescription(String.format("Field:%s matched:%d", field(i, 0), matched));
      subExpl.setValue(1);
      expl.addDetail(subExpl);
    }
    expl.setValue(1);
    return expl;
  }

  abstract public float score() throws IOException;
}
