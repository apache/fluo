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
package org.apache.fluo.api.client.scanner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.ColumnValue;
import org.apache.fluo.api.data.RowColumnValue;
import org.junit.Assert;
import org.junit.Test;

/**
 * @since 1.2.0
 */
public class ScannerStreamTest {

  @Test
  public void testCellScannerStream() {
    Set<RowColumnValue> rowCols = new HashSet<>();
    Set<RowColumnValue> empty = new HashSet<>();

    rowCols.add(new RowColumnValue("r1", new Column("f1", "q1"), "v1"));
    rowCols.add(new RowColumnValue("r1", new Column("f2", "q3"), "v2"));
    rowCols.add(new RowColumnValue("r2", new Column("f1", "q1"), "v3"));
    rowCols.add(new RowColumnValue("r2", new Column("f1", "q2"), "v4"));
    rowCols.add(new RowColumnValue("r4", new Column("f2", "q5"), "v5"));

    CellScannerImpl cellScanner = new CellScannerImpl(rowCols);

    Set<RowColumnValue> expected = rowCols.stream()
        .filter(rcv -> rcv.getColumn().getsFamily().equals("f2")).collect(Collectors.toSet());
    Set<RowColumnValue> actualSubSet = cellScanner.stream()
        .filter(rcv -> rcv.getColumn().getsFamily().equals("f2")).collect(Collectors.toSet());

    Assert.assertNotEquals(empty, actualSubSet);
    Assert.assertNotEquals(empty, cellScanner.stream().collect(Collectors.toSet()));
    Assert.assertEquals(rowCols, cellScanner.stream().collect(Collectors.toSet()));
    Assert.assertEquals(expected, actualSubSet);
  }

  @Test
  public void testColumnScannerStream() {
    Set<ColumnValue> colsVal = new HashSet<>();
    Set<ColumnValue> empty = new HashSet<>();

    Bytes row = Bytes.of("123");
    colsVal.add(new ColumnValue(new Column("f1", "q1"), Bytes.of("v1")));
    colsVal.add(new ColumnValue(new Column("f2", "q3"), Bytes.of("v2")));
    colsVal.add(new ColumnValue(new Column("f1", "q1"), Bytes.of("v3")));
    colsVal.add(new ColumnValue(new Column("f1", "q2"), Bytes.of("v4")));
    colsVal.add(new ColumnValue(new Column("f2", "q5"), Bytes.of("v5")));

    ColumnScanner colScanner = new ColumnScannerImpl(row, colsVal);

    Set<ColumnValue> expected = colsVal.stream()
        .filter(cv -> cv.getColumn().getsFamily().equals("f2")).collect(Collectors.toSet());
    Set<ColumnValue> colSubSet = colScanner.stream()
        .filter(cv -> cv.getColumn().getsFamily().equals("f2")).collect(Collectors.toSet());

    Assert.assertNotEquals(empty, colSubSet);
    Assert.assertNotEquals(empty, colScanner.stream().collect(Collectors.toSet()));
    Assert.assertEquals(colsVal, colScanner.stream().collect(Collectors.toSet()));
    Assert.assertEquals(expected, colSubSet);
  }

  @Test
  public void testRowScannerStream() {
    List<ColumnScanner> rows = new ArrayList<>();
    Set<ColumnValue> cv1 = new HashSet<>();
    Set<ColumnValue> cv2 = new HashSet<>();

    Bytes row1 = Bytes.of("555555555");
    cv1.add(new ColumnValue(new Column("firstname"), Bytes.of("Chris")));
    cv1.add(new ColumnValue(new Column("lastname"), Bytes.of("McTague")));
    cv1.add(new ColumnValue(new Column("age"), Bytes.of("21")));

    rows.add(new ColumnScannerImpl(row1, cv1));

    Bytes row2 = Bytes.of("55234234");
    cv2.add(new ColumnValue(new Column("firstname"), Bytes.of("Hulk")));
    cv2.add(new ColumnValue(new Column("lastname"), Bytes.of("Hogan")));
    cv2.add(new ColumnValue(new Column("age"), Bytes.of("60")));

    rows.add(new ColumnScannerImpl(row2, cv2));

    RowScannerImpl rsi = new RowScannerImpl(rows);

    Set<Person> people = rsi.stream().map(cs -> toPerson(cs)).collect(Collectors.toSet());

    Set<Person> expected = new HashSet<>();
    expected.add(new Person("Chris", "McTague", 21, 555555555));
    expected.add(new Person("Hulk", "Hogan", 60, 55234234));

    Assert.assertEquals(expected, people);
  }

  private static Person toPerson(ColumnScanner cs) {
    Person p = new Person();
    p.id = Integer.parseInt(cs.getsRow());

    for (ColumnValue cv : cs) {
      switch (cv.getColumn().getsFamily()) {
        case "firstname": {
          p.firstname = cv.getsValue();
          break;
        }
        case "lastname": {
          p.lastname = cv.getsValue();
          break;
        }
        case "age": {
          p.age = Integer.parseInt(cv.getsValue());
          break;
        }
        default: {
          throw new IllegalArgumentException("unknown column " + cv.getColumn());
        }
      }
    }
    return p;
  }

  private static class CellScannerImpl implements CellScanner {
    private Collection<RowColumnValue> rcv;

    CellScannerImpl(Collection<RowColumnValue> rcv) {
      this.rcv = rcv;
    }

    @Override
    public Iterator<RowColumnValue> iterator() {
      return rcv.iterator();
    }
  }

  private static class RowScannerImpl implements RowScanner {
    private Collection<ColumnScanner> scan;

    RowScannerImpl(Collection<ColumnScanner> scan) {
      this.scan = scan;
    }

    @Override
    public Iterator<ColumnScanner> iterator() {
      return scan.iterator();
    }
  }

  private static class ColumnScannerImpl implements ColumnScanner {
    private Collection<ColumnValue> scan;
    private Bytes row;

    ColumnScannerImpl(Bytes row, Collection<ColumnValue> scan) {
      this.row = row;
      this.scan = scan;
    }

    @Override
    public Iterator<ColumnValue> iterator() {
      return scan.iterator();
    }

    @Override
    public Bytes getRow() {
      return row;
    }

    @Override
    public String getsRow() {
      return row.toString();
    }
  }

  private static class Person {
    private String firstname;
    private String lastname;
    private int age;
    private int id;

    Person() {
      this.firstname = "";
      this.lastname = "";
      this.age = 0;
      this.id = 0;
    }

    Person(String f, String l, int a, int id) {
      this.firstname = f;
      this.lastname = l;
      this.age = a;
      this.id = id;
    }

    @Override
    public int hashCode() {
      return Objects.hash(firstname, lastname, age, id);
    }

    @Override
    public boolean equals(Object o) {
      if (o == this) {
        return true;
      }

      if (o instanceof Person) {
        Person p = (Person) o;
        return (this.firstname.equals(p.firstname)) && (this.lastname.equals(p.lastname))
            && (this.age == p.age) && (this.id == p.id);
      }
      return false;
    }
  }
}
