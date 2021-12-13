/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.solr.search;

import java.io.IOException;
import java.util.Objects;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ConstantScoreScorer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;

public class DocSetQuery extends Query {
  protected final DocSet docSet;

  public DocSetQuery(DocSet docSet) {
    this.docSet = docSet;
  }

  public DocSet getDocSet() {
    return docSet;
  }

  @Override
  public String toString(String field) {
    return "DocSetQuery(" + field + ")";
  }

  @Override
  public void visit(QueryVisitor visitor) {
    visitor.visitLeaf(this);
  }

  @Override
  public boolean equals(Object obj) {
    return sameClassAs(obj) && Objects.equals(docSet, getClass().cast(obj).docSet);
  }

  @Override
  public int hashCode() {
    return classHash() * 31 + docSet.hashCode();
  }

  @Override
  public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
    return new Weight(this) {

      @Override
      public Explanation explain(LeafReaderContext context, int doc) throws IOException {
        final Scorer scorer = scorer(context);
        final boolean match = (scorer != null && scorer.iterator().advance(doc) == doc);
        if (match) {
          assert scorer.score() == 0f;
          return Explanation.match(0f, "Match on id " + doc);
        } else {
          return Explanation.match(0f, "No match on id " + doc);
        }
      }

      @Override
      public Scorer scorer(LeafReaderContext context) throws IOException {
        final DocIdSetIterator iterator = docSet.iterator(context);
        if (iterator == null) {
          return null;
        }
        return new ConstantScoreScorer(this, 0f, scoreMode, iterator);
      }

      @Override
      public boolean isCacheable(LeafReaderContext ctx) {
        return false;
      }
    };
  }
}
