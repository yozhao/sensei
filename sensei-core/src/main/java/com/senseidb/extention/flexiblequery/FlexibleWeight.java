package com.senseidb.extention.flexiblequery;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermContext;
import org.apache.lucene.search.ComplexExplanation;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.Bits;

import java.io.IOException;

public class FlexibleWeight extends Weight {
  private FlexibleQuery query;
  private Similarity similarity;

  static public class TermStats {
    public Similarity.SimWeight stats;
    public Term term;
    public TermContext termContext;
  }

  private TermStats[][] termStatsMatrix;

  public FlexibleWeight(FlexibleQuery query, IndexSearcher searcher) throws IOException {
    this.query = query;
    this.similarity = searcher.getSimilarity();
    final IndexReaderContext context = searcher.getTopReaderContext();

    Term[][] terms = query.getTerms();
    termStatsMatrix = new TermStats[terms.length][];
    for (int i = 0; i < terms.length; ++i) {
      termStatsMatrix[i] = new TermStats[terms[i].length];
      for (int j = 0; j < terms[i].length; ++j) {
        Term term = terms[i][j];
        TermContext state = TermContext.build(context, term, true);
        final TermStatistics termStats = searcher.termStatistics(term, state);
        Similarity.SimWeight stats = similarity.computeWeight(query.getBoost(),
                                                              searcher.collectionStatistics(term.field()),
                                                              termStats);
        TermStats termStatsInfo = new TermStats();
        termStatsInfo.stats = stats;
        termStatsInfo.term = term;
        termStatsInfo.termContext = state;
        termStatsMatrix[i][j] = termStatsInfo;
      }
    }

  }

  @Override
  public Explanation explain(AtomicReaderContext context, int doc) throws IOException {
    FlexibleScorer scorer = (FlexibleScorer) scorer(context, true, false, context.reader().getLiveDocs());
    if (scorer != null) {
      int newDoc = scorer.advance(doc);
      if (newDoc == doc) {
        CustomMatchingStrategy strategy = scorer.getStrategy();
        Explanation expl = strategy.explain(query, doc);
        return expl;
      }
    }
    return new ComplexExplanation(false, 0.0f, "no matching term");
  }

  @Override
  public FlexibleQuery getQuery() {
    return query;
  }

  @Override
  public float getValueForNormalization() throws IOException {
    float sum = 0f;
    for (int i = 0; i < termStatsMatrix.length; ++i) {
      for (TermStats termStats : termStatsMatrix[i]) {
        float s = termStats.stats.getValueForNormalization();
        sum += s;
      }
    }
    // for (TermDocsEnum[] fieldMatchedDocs : matchedEnumsMatrix) {
    //     for (TermDocsEnum matchedDocs : fieldMatchedDocs) {
    //         float s = matchedDocs.getStats().getValueForNormalization();
    //         sum += s;
    //     }
    // }
    sum *= query.getBoost() * query.getBoost();
    return sum;
  }

  @Override
  public void normalize(float norm, float topLevelBoost) {
    for (int i = 0; i < termStatsMatrix.length; ++i) {
      for (TermStats termStats : termStatsMatrix[i])
        termStats.stats.normalize(norm, topLevelBoost);
    }

    // for (TermDocsEnum[] fieldMatchedDocs : matchedEnumsMatrix)
    //     for (TermDocsEnum matchedDocs : fieldMatchedDocs)
    //         matchedDocs.getStats().normalize(norm, topLevelBoost);
  }

  @Override
  public Scorer scorer(AtomicReaderContext context, boolean scoreDocsInOrder, boolean topScorer, Bits acceptDocs) throws IOException {
    Term[][] terms = query.getTerms();
    TermDocsEnum[][] matchedEnumsMatrix = new TermDocsEnum[terms.length][];
    for (int i = 0; i < query.getTerms().length; ++i) {
      Term[] fieldTerms = terms[i];
      matchedEnumsMatrix[i] = new TermDocsEnum[fieldTerms.length];
      for (int j = 0; j < fieldTerms.length; ++j)
        matchedEnumsMatrix[i][j] = new TermDocsEnum(context, acceptDocs, termStatsMatrix[i][j], similarity);
    }

    return new FlexibleScorer(context, this, matchedEnumsMatrix);
  }
}
