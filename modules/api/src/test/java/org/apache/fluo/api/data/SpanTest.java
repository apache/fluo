/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.fluo.api.data;

import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.api.data.Span;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for {@link Span}
 */
public class SpanTest {

  String rw1s = "rw1";
  String cf1s = "cf1";
  String cq1s = "cq1";
  String cv1s = "cv1";
  long ts1 = 51;
  String rw2s = "rw2";
  String cf2s = "cf2";
  String cq2s = "cq2";
  String cv2s = "cv2";
  long ts2 = 51;
  Bytes rw1b = Bytes.of(rw1s);
  Bytes cf1b = Bytes.of(cf1s);
  Bytes cq1b = Bytes.of(cq1s);
  Bytes cv1b = Bytes.of(cv1s);
  Bytes rw2b = Bytes.of(rw2s);
  Bytes cf2b = Bytes.of(cf2s);
  Bytes cq2b = Bytes.of(cq2s);
  Bytes cv2b = Bytes.of(cv2s);

  @Test
  public void testRowRange() {
    // Test with Bytes input
    Assert.assertEquals(new Span(rw1b, true, rw2b, false),
        Span.newBuilder().startRow(rw1b).endRow(rw2b).exclusive().build());
    Assert.assertEquals(new Span(rw1b, false, rw2b, false),
        Span.newBuilder().startRow(rw1b).exclusive().endRow(rw2b).exclusive().build());
    Assert.assertEquals(new Span(rw1b, true, rw2b, true),
        Span.newBuilder().startRow(rw1b).endRow(rw2b).build());
    Assert.assertEquals(new Span(rw1b, false, rw2b, true),
        Span.newBuilder().startRow(rw1b).exclusive().endRow(rw2b).build());

    // Test with String input
    Assert.assertEquals(new Span(rw1b, true, rw2b, false),
        Span.newBuilder().startRow(rw1s).endRow(rw2s).exclusive().build());
    Assert.assertEquals(new Span(rw1b, false, rw2b, false),
        Span.newBuilder().startRow(rw1s).exclusive().endRow(rw2s).exclusive().build());
    Assert.assertEquals(new Span(rw1b, true, rw2b, true),
        Span.newBuilder().startRow(rw1s).endRow(rw2s).build());
    Assert.assertEquals(new Span(rw1b, false, rw2b, true),
        Span.newBuilder().startRow(rw1s).exclusive().endRow(rw2s).build());
  }

  @Test
  public void testInfiniteRanges() {
    RowColumn rc1 = new RowColumn(rw1b, new Column(cf1b));
    RowColumn frc1 = rc1.following();
    RowColumn rc2 = new RowColumn(rw2b, new Column(cf2b));
    RowColumn frc2 = rc2.following();

    Assert.assertEquals(new Span(RowColumn.EMPTY, true, frc2, false),
        Span.newBuilder().endRow(rw2b).fam(cf2b).build());
    Assert.assertEquals(new Span(RowColumn.EMPTY, true, rc2, false),
        Span.newBuilder().endRow(rw2b).fam(cf2b).exclusive().build());
    Assert.assertEquals(new Span(rc1, true, RowColumn.EMPTY, true),
        Span.newBuilder().startRow(rw1b).fam(cf1b).build());
    Assert.assertEquals(new Span(frc1, true, RowColumn.EMPTY, true),
        Span.newBuilder().startRow(rw1b).fam(cf1b).exclusive().build());
  }

  @Test
  public void testRowCFRange() {
    RowColumn rc1 = new RowColumn(rw1b, new Column(cf1b));
    RowColumn frc1 = rc1.following();
    RowColumn rc2 = new RowColumn(rw2b, new Column(cf2b));
    RowColumn frc2 = rc2.following();

    Assert.assertEquals(new Span(rc1, true, frc2, false),
        Span.newBuilder().startRow(rw1b).fam(cf1b).endRow(rw2b).fam(cf2b).build());
    Assert.assertEquals(new Span(rc1, true, rc2, false),
        Span.newBuilder().startRow(rw1b).fam(cf1b).endRow(rw2b).fam(cf2b).exclusive().build());
    Assert.assertEquals(new Span(frc1, true, frc2, false),
        Span.newBuilder().startRow(rw1b).fam(cf1b).exclusive().endRow(rw2b).fam(cf2b).build());
    Assert.assertEquals(new Span(frc1, true, rc2, false), Span.newBuilder().startRow(rw1b).fam(cf1b)
        .exclusive().endRow(rw2b).fam(cf2b).exclusive().build());
  }

  @Test
  public void testRowCFCQRange() {
    RowColumn rc1 = new RowColumn(rw1b, new Column(cf1b, cq1b));
    RowColumn frc1 = rc1.following();
    RowColumn rc2 = new RowColumn(rw2b, new Column(cf2b, cq2b));
    RowColumn frc2 = rc2.following();

    Assert.assertEquals(new Span(rc1, true, frc2, false), Span.newBuilder().startRow(rw1b).fam(cf1b)
        .qual(cq1b).endRow(rw2b).fam(cf2b).qual(cq2b).build());
    Assert.assertEquals(new Span(rc1, true, rc2, false), Span.newBuilder().startRow(rw1b).fam(cf1b)
        .qual(cq1b).endRow(rw2b).fam(cf2b).qual(cq2b).exclusive().build());
    Assert.assertEquals(new Span(frc1, true, frc2, false), Span.newBuilder().startRow(rw1b)
        .fam(cf1b).qual(cq1b).exclusive().endRow(rw2b).fam(cf2b).qual(cq2b).build());
    Assert.assertEquals(new Span(frc1, true, rc2, false), Span.newBuilder().startRow(rw1b).fam(cf1b)
        .qual(cq1b).exclusive().endRow(rw2b).fam(cf2b).qual(cq2b).exclusive().build());
  }

  @Test
  public void testRowCFCQCVRange() {
    RowColumn rc1 = new RowColumn(rw1b, new Column(cf1b, cq1b, Bytes.of(cv1s)));
    RowColumn frc1 = rc1.following();
    RowColumn rc2 = new RowColumn(rw2b, new Column(cf2b, cq2b, Bytes.of(cv2s)));
    RowColumn frc2 = rc2.following();

    Assert.assertEquals(new Span(rc1, true, frc2, false), Span.newBuilder().startRow(rw1b).fam(cf1b)
        .qual(cq1b).vis(cv1b).endRow(rw2b).fam(cf2b).qual(cq2b).vis(cv2b).build());
    Assert.assertEquals(new Span(rc1, true, rc2, false), Span.newBuilder().startRow(rw1b).fam(cf1b)
        .qual(cq1b).vis(cv1b).endRow(rw2b).fam(cf2b).qual(cq2b).vis(cv2b).exclusive().build());
    Assert.assertEquals(new Span(frc1, true, frc2, false),
        Span.newBuilder().startRow(rw1b).fam(cf1b).qual(cq1b).vis(cv1b).exclusive().endRow(rw2b)
            .fam(cf2b).qual(cq2b).vis(cv2b).build());
    Assert.assertEquals(new Span(frc1, true, rc2, false),
        Span.newBuilder().startRow(rw1b).fam(cf1b).qual(cq1b).vis(cv1b).exclusive().endRow(rw2b)
            .fam(cf2b).qual(cq2b).vis(cv2b).exclusive().build());
  }

  @Test
  public void testExactSpan() {
    Span s = Span.exact(rw1b);
    Assert.assertEquals(rw1b, s.getStart().getRow());
    Assert.assertEquals(Column.EMPTY, s.getStart().getColumn());
    Assert.assertTrue(s.isStartInclusive());
    Assert.assertEquals(new RowColumn(rw1b).following().getRow(), s.getEnd().getRow());
    Assert.assertEquals(Column.EMPTY, s.getEnd().getColumn());
    Assert.assertFalse(s.isEndInclusive());

    s = Span.exact(rw1b, Column.EMPTY);
    Assert.assertEquals(rw1b, s.getStart().getRow());
    Assert.assertEquals(Column.EMPTY, s.getStart().getColumn());
    Assert.assertTrue(s.isStartInclusive());
    Assert.assertEquals(new RowColumn(rw1b).following().getRow(), s.getEnd().getRow());
    Assert.assertEquals(Column.EMPTY, s.getEnd().getColumn());
    Assert.assertFalse(s.isEndInclusive());

    s = Span.exact(rw1b, new Column(cf1b));
    Assert.assertEquals(rw1b, s.getStart().getRow());
    Assert.assertEquals(cf1b, s.getStart().getColumn().getFamily());
    Assert.assertFalse(s.getStart().getColumn().isQualifierSet());
    Assert.assertFalse(s.getStart().getColumn().isVisibilitySet());
    Assert.assertTrue(s.isStartInclusive());
    Assert.assertEquals(rw1b, s.getEnd().getRow());
    Assert.assertEquals(new RowColumn(rw1b, new Column(cf1b)).following().getColumn().getFamily(),
        s.getEnd().getColumn().getFamily());
    Assert.assertFalse(s.getEnd().getColumn().isQualifierSet());
    Assert.assertFalse(s.getEnd().getColumn().isVisibilitySet());
    Assert.assertFalse(s.isEndInclusive());

    s = Span.exact(rw1b, new Column(cf1b, cq1b));
    Assert.assertEquals(rw1b, s.getStart().getRow());
    Assert.assertEquals(cf1b, s.getStart().getColumn().getFamily());
    Assert.assertEquals(cq1b, s.getStart().getColumn().getQualifier());
    Assert.assertFalse(s.getStart().getColumn().isVisibilitySet());
    Assert.assertTrue(s.isStartInclusive());
    Assert.assertEquals(rw1b, s.getEnd().getRow());
    Assert.assertEquals(cf1b, s.getEnd().getColumn().getFamily());
    Assert.assertEquals(
        new RowColumn(rw1b, new Column(cf1b, cq1b)).following().getColumn().getQualifier(),
        s.getEnd().getColumn().getQualifier());
    Assert.assertFalse(s.getEnd().getColumn().isVisibilitySet());
    Assert.assertFalse(s.isEndInclusive());

    s = Span.exact(rw1b, new Column(cf1b, cq1b, cv1b));
    Assert.assertEquals(rw1b, s.getStart().getRow());
    Assert.assertEquals(cf1b, s.getStart().getColumn().getFamily());
    Assert.assertEquals(cq1b, s.getStart().getColumn().getQualifier());
    Assert.assertEquals(cv1b, s.getStart().getColumn().getVisibility());
    Assert.assertTrue(s.isStartInclusive());
    Assert.assertEquals(rw1b, s.getEnd().getRow());
    Assert.assertEquals(cf1b, s.getEnd().getColumn().getFamily());
    Assert.assertEquals(cq1b, s.getEnd().getColumn().getQualifier());
    Assert.assertEquals(
        new RowColumn(rw1b, new Column(cf1b, cq1b, cv1b)).following().getColumn().getVisibility(),
        s.getEnd().getColumn().getVisibility());
    Assert.assertFalse(s.isEndInclusive());
  }

  @Test
  public void testPrefixSpan() {
    Span s = Span.prefix(rw1b);
    Assert.assertEquals(rw1b, s.getStart().getRow());
    Assert.assertEquals(Column.EMPTY, s.getStart().getColumn());
    Assert.assertTrue(s.isStartInclusive());
    Assert.assertEquals(rw2b, s.getEnd().getRow());
    Assert.assertEquals(Column.EMPTY, s.getEnd().getColumn());
    Assert.assertFalse(s.isEndInclusive());

    s = Span.prefix(rw1b, new Column());
    Assert.assertEquals(rw1b, s.getStart().getRow());
    Assert.assertEquals(Column.EMPTY, s.getStart().getColumn());
    Assert.assertTrue(s.isStartInclusive());
    Assert.assertEquals(rw2b, s.getEnd().getRow());
    Assert.assertEquals(Column.EMPTY, s.getEnd().getColumn());
    Assert.assertFalse(s.isEndInclusive());

    s = Span.prefix(rw1b, new Column(cf1b));
    Assert.assertEquals(rw1b, s.getStart().getRow());
    Assert.assertEquals(cf1b, s.getStart().getColumn().getFamily());
    Assert.assertFalse(s.getStart().getColumn().isQualifierSet());
    Assert.assertFalse(s.getStart().getColumn().isVisibilitySet());
    Assert.assertTrue(s.isStartInclusive());
    Assert.assertEquals(rw1b, s.getStart().getRow());
    Assert.assertEquals(cf2b, s.getEnd().getColumn().getFamily());
    Assert.assertFalse(s.getEnd().getColumn().isQualifierSet());
    Assert.assertFalse(s.getEnd().getColumn().isVisibilitySet());
    Assert.assertFalse(s.isEndInclusive());

    s = Span.prefix(rw1b, new Column(cf1b, cq1b));
    Assert.assertEquals(rw1b, s.getStart().getRow());
    Assert.assertEquals(cf1b, s.getStart().getColumn().getFamily());
    Assert.assertEquals(cq1b, s.getStart().getColumn().getQualifier());
    Assert.assertFalse(s.getStart().getColumn().isVisibilitySet());
    Assert.assertTrue(s.isStartInclusive());
    Assert.assertEquals(rw1b, s.getStart().getRow());
    Assert.assertEquals(cf1b, s.getEnd().getColumn().getFamily());
    Assert.assertEquals(cq2b, s.getEnd().getColumn().getQualifier());
    Assert.assertFalse(s.getEnd().getColumn().isVisibilitySet());
    Assert.assertFalse(s.isEndInclusive());

    s = Span.prefix(rw1b, new Column(cf1b, cq1b, cv1b));
    Assert.assertEquals(rw1b, s.getStart().getRow());
    Assert.assertEquals(cf1b, s.getStart().getColumn().getFamily());
    Assert.assertEquals(cq1b, s.getStart().getColumn().getQualifier());
    Assert.assertEquals(cv1b, s.getStart().getColumn().getVisibility());
    Assert.assertTrue(s.isStartInclusive());
    Assert.assertEquals(rw1b, s.getStart().getRow());
    Assert.assertEquals(cf1b, s.getEnd().getColumn().getFamily());
    Assert.assertEquals(cq1b, s.getEnd().getColumn().getQualifier());
    Assert.assertEquals(cv2b, s.getEnd().getColumn().getVisibility());
    Assert.assertFalse(s.isEndInclusive());
  }
}
