package com.senseidb.extention.flexiblequery;

import com.browseengine.bobo.api.BoboSegmentReader;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Scorer;

import java.io.IOException;

public class FlexibleScorer extends Scorer {

  private int numMatchedEnums;
  private int doc = -1;
  private TermDocsEnum[] matchedEnumsList;
  private MatchedInfoMatrix matchedInfoMatrix;
  private CustomMatchingStrategy strategy;

  /**
   * Constructs a Scorer
   *
   * @param context
   * @param weight The scorers <code>Weight</code>.
   */
  protected FlexibleScorer(AtomicReaderContext context, FlexibleWeight weight,
                           TermDocsEnum[][] matchedEnumsMatrix) throws IOException {
    super(weight);

    TermMatchedInfo[][] matchedInfos = new TermMatchedInfo[matchedEnumsMatrix.length][];
    for (int i = 0; i < matchedEnumsMatrix.length; ++i) {
      matchedInfos[i] = new TermMatchedInfo[matchedEnumsMatrix[i].length];
      for (int j = 0; j < matchedEnumsMatrix[i].length; ++j) {
        matchedInfos[i][j] = new TermMatchedInfo(
            matchedEnumsMatrix[i][j].term(),
            matchedEnumsMatrix[i][j].getSimilarity(),
            matchedEnumsMatrix[i][j].getDocScorer());
        matchedEnumsMatrix[i][j].fieldPos = i;
        matchedEnumsMatrix[i][j].termPos = j;
      }
      numMatchedEnums += matchedEnumsMatrix[i].length;
    }
    this.matchedInfoMatrix = new MatchedInfoMatrix(matchedInfos, weight.getQuery().getFieldBoosts());

    strategy = weight.getQuery().getStrategy();
    strategy.setMatchedMatrix(matchedInfoMatrix);
    strategy.setBoboReader((BoboSegmentReader) context.reader());
    strategy.init();

    matchedEnumsList = new TermDocsEnum[numMatchedEnums];
    int counter = 0;
    for (TermDocsEnum[] fieldEnums : matchedEnumsMatrix) {
      for (TermDocsEnum docsEnum: fieldEnums) {
        matchedEnumsList[counter] = docsEnum;
        docsEnum.next();
        ++counter;
      }
    }

    heapify();
  }

  /**
   * Organize matchedEnumsList into a min heap with scorers generating the earliest document on top.
   */
  protected final void heapify() {
    for (int i = (numMatchedEnums >> 1) - 1; i >= 0; i--) {
      heapAdjust(i);
    }
  }
  /**
   * The subtree of matchedEnumsList at root is a min heap except possibly for its root element.
   * Bubble the root down as required to make the subtree a heap.
   */
  protected final void heapAdjust(int root) {
    TermDocsEnum scorer = matchedEnumsList[root];
    int doc = scorer.doc();
    int i = root;
    while (i <= (numMatchedEnums >> 1) - 1) {
      int lchild = (i << 1) + 1;
      TermDocsEnum lscorer = matchedEnumsList[lchild];
      int ldoc = lscorer.doc();
      int rdoc = Integer.MAX_VALUE, rchild = (i << 1) + 2;
      TermDocsEnum rscorer = null;
      if (rchild < numMatchedEnums) {
        rscorer = matchedEnumsList[rchild];
        rdoc = rscorer.doc();
      }
      if (ldoc < doc) {
        if (rdoc < ldoc) {
          matchedEnumsList[i] = rscorer;
          matchedEnumsList[rchild] = scorer;
          i = rchild;
        } else {
          matchedEnumsList[i] = lscorer;
          matchedEnumsList[lchild] = scorer;
          i = lchild;
        }
      } else if (rdoc < doc) {
        matchedEnumsList[i] = rscorer;
        matchedEnumsList[rchild] = scorer;
        i = rchild;
      } else {
        return;
      }
    }
  }

  /**
   * Remove the root Scorer from subScorers and re-establish it as a heap
   */
  protected final void heapRemoveRoot() {
    if (numMatchedEnums == 1) {
      matchedEnumsList[0] = null;
      numMatchedEnums = 0;
    } else {
      matchedEnumsList[0] = matchedEnumsList[numMatchedEnums - 1];
      matchedEnumsList[numMatchedEnums - 1] = null;
      --numMatchedEnums;
      heapAdjust(0);
    }
  }

  @Override
  public float score() throws IOException {
    return strategy.score();
  }

  @Override
  public int freq() throws IOException {
    return 1;
  }

  @Override
  public int docID() {
    return doc;
  }

  private void clear() {
    for (int i = 0; i < matchedInfoMatrix.getFieldLength(); ++i) {
      for (int j = 0; j < matchedInfoMatrix.getTermLength(i); ++j) {
        matchedInfoMatrix.get(i, j).reset();
      }
    }
  }

  @Override
  public int nextDoc() throws IOException {
    clear();

    if (matchedEnumsList.length == 0 || matchedEnumsList[0] == null || doc == NO_MORE_DOCS) return NO_MORE_DOCS;

    doc = matchedEnumsList[0].doc();

    while(true) {
      if (matchedEnumsList[0].doc() == doc) {
        addToTermMatchedMatrix(matchedEnumsList[0]);
        if (matchedEnumsList[0].next()) {
          heapAdjust(0);
        } else {
          heapRemoveRoot();
          if (numMatchedEnums == 0) {
            return doc;
          }
        }
      } else {
        return doc;
      }
    }
  }

  private void addToTermMatchedMatrix(TermDocsEnum matchedEnum) {
    TermMatchedInfo termMatchedInfo = matchedInfoMatrix.get(matchedEnum.fieldPos, matchedEnum.termPos);
    matchedInfoMatrix.setDoc(matchedEnum.doc());
    if (!termMatchedInfo.isMatched()) {
      termMatchedInfo.setPosition(matchedEnum.position());
      termMatchedInfo.setFreq(matchedEnum.freq());
      termMatchedInfo.setDoc(doc);
    }
  }

  @Override
  public int advance(int target) throws IOException {
    int doc;
    while ((doc = nextDoc()) < target) {
    }
    return doc;
  }

  @Override
  public long cost() {
    return 0;
  }

  public CustomMatchingStrategy getStrategy() { return strategy; }
}
