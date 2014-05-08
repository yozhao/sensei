package com.senseidb.extention.flexiblequery;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.DocsAndPositionsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.Bits;

import java.io.IOException;

public class TermDocsEnum extends DocsEnum {
  protected DocsAndPositionsEnum postings;
  protected Term term;
  protected int doc;
  protected int position;
  protected int freq;
  protected int count;
  protected Similarity similarity;
  // lucene 4.6
  // protected Similarity.SimScorer docScorer;
  protected Similarity.ExactSimScorer docScorer;
  protected int fieldPos = -1;
  protected int termPos = -1;

  public TermDocsEnum(AtomicReaderContext context, Bits acceptDocs, FlexibleWeight.TermStats termStats, Similarity similarity) throws IOException {
    this.similarity = similarity;
    this.docScorer = similarity.exactSimScorer(termStats.stats, context);
    this.term = termStats.term;
    doc = -1;
    final TermState state = termStats.termContext.get(context.ord);
    if (state == null)
      return;

    final TermsEnum termsEnum = context.reader().terms(term.field()).iterator(null);
    termsEnum.seekExact(term.bytes(), state);
    final DocsAndPositionsEnum postings = termsEnum.docsAndPositions(acceptDocs, null, DocsAndPositionsEnum.FLAG_OFFSETS);
    if (postings != null)
      this.postings = postings;
  }

  // only for EmptyMatchedDocs
  TermDocsEnum() {
    term = null;
    postings = null;
  }

  public Similarity.ExactSimScorer getDocScorer() { return docScorer; }

  public Similarity getSimilarity() { return similarity; }

  @Override
  public Term term() {
    return term;
  }

  @Override
  public boolean next() throws IOException {
    if (count == freq) {
      if (postings == null) {
        doc = DocIdSetIterator.NO_MORE_DOCS;
        return false;
      }
      doc = postings.nextDoc();
      if (doc == DocIdSetIterator.NO_MORE_DOCS) {
        return false;
      }
      freq = postings.freq();
      count = 0;
    }
    position = postings.nextPosition();
    count++;
    // readPayload = false;
    return true;
  }

  @Override
  public boolean skipTo(int target) throws IOException {
    assert target > doc;
    doc = postings.advance(target);
    if (doc == DocIdSetIterator.NO_MORE_DOCS) {
      return false;
    }

    freq = postings.freq();
    count = 0;
    position = postings.nextPosition();
    count++;
    // readPayload = false;
    return true;
  }

  @Override
  public int doc() {
    return doc;
  }

  @Override
  public int freq() {
    return freq;
  }

  @Override
  public int position() {
    return position;
  }

  @Override
  public long cost() {
    return postings.cost();
  }

  @Override
  public String toString() {
    if (postings != null) {
      return "MatchedDocs(" + term.toString() +")@" +
             (doc == -1 ? "START" : (doc == Integer.MAX_VALUE) ? "END" : doc + "-" + position);
    } else {
      return term + "NotMatched";
    }
  }
}
