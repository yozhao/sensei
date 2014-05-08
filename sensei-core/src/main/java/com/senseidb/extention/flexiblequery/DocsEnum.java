package com.senseidb.extention.flexiblequery;

import org.apache.lucene.index.Term;

import java.io.IOException;

abstract public class DocsEnum {
  abstract public Term term();
  abstract public boolean next() throws IOException;
  abstract public boolean skipTo(int target) throws IOException;
  abstract public int doc();
  abstract public int freq();
  abstract public int position();
  abstract public long cost();
  abstract public String toString();
}
