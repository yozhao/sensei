package com.senseidb.extention.flexiblequery;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.ComplexExplanation;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.similarities.Similarity;

import java.io.IOException;

public class TermMatchedInfo {
  // don't use matchedDocs.position which changes every nextDoc().
  private final Similarity similarity;
  private final Similarity.ExactSimScorer docScorer;
  private final Term term;

  private int position = -1;
  private int doc = -1;
  private int freq = -1;

  public TermMatchedInfo(Term term, Similarity similarity, Similarity.ExactSimScorer docScorer) {
    this.term = term;
    this.similarity = similarity;
    this.docScorer = docScorer;
  }

  public float score() throws IOException {
    if (isMatched())
      return docScorer.score(doc, freq);
    else
      return 0f;
  }

  public int position() {
    return position;
  }

  public void setPosition(int pos) {
    position = pos;
  }

  public void setDoc(int doc) {
    this.doc = doc;
  }

  public void setFreq(int f) {
    freq = f;
  }

  public int doc() {
    return doc;
  }

  public Term getTerm() {
    return term;
  }

  public void reset() {
    position = -1;
    doc = -1;
    freq = -1;
  }

  public boolean isMatched() {
    return position != -1;
  }

  public Explanation explain(Query query) {
    if (!isMatched())
      return null;
    ComplexExplanation result = new ComplexExplanation();
    result.setDescription("weight("+query+" in "+ doc +") [" + similarity.getClass().getSimpleName() + "], result of:");
    Explanation scoreExplanation = docScorer.explain(doc, new Explanation(freq, "termFreq=" + freq));
    result.addDetail(scoreExplanation);
    result.setValue(scoreExplanation.getValue());
    result.setMatch(true);
    return result;
  }

  public String toString() {
    if (position == -1) {
      return getTerm() + " not matched.";
    } else {
      return String.format("%s(%d) Position:%d",
                           getTerm(), getTerm().text().getBytes().length, position);
    }
  }
}
