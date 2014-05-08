package com.senseidb.extention.flexiblequery;

import org.apache.commons.lang.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.WhitespaceAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Version;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class FlexibleQuery extends Query {
  private Term[][] terms;
  private CustomMatchingStrategy strategy;
  private String content;
  private Analyzer analyzer;
  private float[] fieldBoosts;

  static public class FlexibleField {
    public String field;
    public String content;
    public float boost = 1f;
  }

  public static class DefaultStrategy extends CustomMatchingStrategy {
    @Override
    public float score() throws IOException {
      float sum = 0f;
      for (int i = 0; i < getFieldLength(); ++i) {
        int matched = 0;
        float subSum = 0;
        for (int j = 0; j < getTermLength(i); ++j) {
          if (isMatched(i, j)) {
            subSum += getScore(i, j);
            ++matched;
          }
        }
        subSum *= (float)matched / getTermLength(i);
        sum += subSum;
      }
      return sum;
    }

    @Override
    public Explanation explain(Query query, int doc) {
      Explanation expl = new Explanation();
      expl.setDescription("FlexibleWeight-" + this.getClass().getSimpleName());
      float sum = 0f;
      for (int i = 0; i < getFieldLength(); ++i) {
        Explanation subExpl = new Explanation();
        subExpl.setDescription("Field:" + field(i, 0));
        float subSum = 0;
        int matched = 0;
        for (int j = 0; j < getTermLength(i); ++j) {
          if (isMatched(i, j)) {
            Explanation result = explain(i, j, query);
            subExpl.addDetail(result);
            subSum += result.getValue();
            ++matched;
          }
        }
        subSum *= (float) matched / getTermLength(i);
        Explanation coordExpl = new Explanation();
        coordExpl.setDescription("coord(" + matched + "/" + getTermLength(i) + ")");
        coordExpl.setValue((float) matched / getTermLength(i));
        subExpl.addDetail(coordExpl);
        subExpl.setValue(subSum);
        expl.addDetail(subExpl);
        sum += subSum;
      }
      expl.setValue(sum);
      return expl;
    }
  }

  public FlexibleQuery(List<FlexibleField> fields, String content, Analyzer analyzer, CustomMatchingStrategy strategy) throws IOException {
    this.strategy = strategy;
    this.analyzer = analyzer;
    init(fields, content);
  }

  public FlexibleQuery(List<FlexibleField> fields, String content, Analyzer analyzer) throws IOException {
    this(fields, content, analyzer, new DefaultStrategy());
  }

  public FlexibleQuery(List<FlexibleField> fields, String content) throws IOException {
    this(fields, content, new WhitespaceAnalyzer(Version.LUCENE_43), new DefaultStrategy());
  }

  public Term[][] getTerms() { return terms; }

  public CustomMatchingStrategy getStrategy() { return strategy; }

  public float[] getFieldBoosts() { return fieldBoosts; }

  private void init(List<FlexibleField> fields, String content) throws IOException {
    List<String> defaultWords = parseToTokens(content);
    terms = parseToTerms(fields, defaultWords);
    fieldBoosts = new float[fields.size()];
    for (int i = 0; i < fields.size(); ++i) {
      fieldBoosts[i] = fields.get(i).boost;
    }
    this.content = content;
  }

  private Term[][] parseToTerms(List<FlexibleField> fields, List<String> defaultWords) throws IOException {
    Term[][] terms = new Term[fields.size()][];
    // List<Term> terms = new ArrayList<Term>();
    for (int i = 0; i < fields.size(); ++i) {
      FlexibleField field = fields.get(i);
      if (StringUtils.isNotEmpty(field.content)) {
        List<String> fieldsWords = parseToTokens(field.content);
        terms[i] = new Term[fieldsWords.size()];
        for (int j = 0; j < fieldsWords.size(); ++j) {
          terms[i][j] = new Term(field.field, fieldsWords.get(j));
        }
      } else {
        terms[i] = new Term[defaultWords.size()];
        for (int j = 0; j < defaultWords.size(); ++j) {
          terms[i][j] = new Term(field.field, defaultWords.get(j));
        }
      }
    }
    return terms;
  }

  private List<String> parseToTokens(String content) throws IOException {
    List<String> tokens = new ArrayList<String>();
    TokenStream stream  = analyzer.tokenStream("", new StringReader(content));
    CharTermAttribute term = stream.getAttribute(CharTermAttribute.class);
    stream.reset();
    while(stream.incrementToken()) {
      tokens.add(term.toString());
    }
    return tokens;
  }

  @Override
  public String toString(String field) {
    StringBuffer buffer = new StringBuffer();
    buffer.append("FlexibleQuery(");
    for (Term[] fieldTerms: terms) {
      if (fieldTerms.length != 0)
        buffer.append("(" + fieldTerms[0].field() + ":");
      List<String> texts = new ArrayList<String>();
      for (Term term : fieldTerms) {
        texts.add(term.text());
      }
      buffer.append(StringUtils.join(texts, ","));
      buffer.append(")");
    }
    buffer.append(")");
    return buffer.toString();
  }

  @Override
  public String toString() {
    StringBuffer buffer = new StringBuffer();
    buffer.append("FlexibleQuery(");
    for (Term[] fieldTerms: terms) {
      if (fieldTerms.length != 0)
        buffer.append("(" + fieldTerms[0].field() + ":");
      List<String> texts = new ArrayList<String>();
      for (Term term : fieldTerms) {
        texts.add(term.text());
      }
      buffer.append(StringUtils.join(texts, ","));
      buffer.append(")");
    }
    buffer.append(")");
    return buffer.toString();
  }

  @Override
  public Weight createWeight(IndexSearcher searcher) throws IOException {
    return new FlexibleWeight(this, searcher);
  }
}
