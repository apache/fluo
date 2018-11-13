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

package org.apache.fluo.core.util;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.function.Predicate;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterators;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.fluo.api.client.scanner.CellScanner;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumnValue;
import org.apache.fluo.core.impl.Notification;

import static java.util.stream.Collectors.toSet;

public class NotificationScanner implements CellScanner {

  private Iterable<Entry<Key, Value>> scanner;
  private Predicate<RowColumnValue> filter;

  private static Predicate<RowColumnValue> createColumnFilter(Collection<Column> allColumns) {
    if (allColumns.isEmpty()) {
      return rcv -> true;
    } else {
      Set<Bytes> families = allColumns.stream().filter(col -> !col.isQualifierSet())
          .map(col -> col.getFamily()).collect(toSet());
      Set<Column> columns =
          allColumns.stream().filter(col -> col.isQualifierSet()).collect(toSet());

      if (families.isEmpty()) {
        return rcv -> columns.contains(rcv.getColumn());
      } else if (columns.isEmpty()) {
        return rcv -> families.contains(rcv.getColumn().getFamily());
      } else {
        return rcv -> families.contains(rcv.getColumn().getFamily())
            || columns.contains(rcv.getColumn());
      }
    }
  }

  NotificationScanner(Scanner scanner, Collection<Column> columns) {
    this(scanner, createColumnFilter(columns));
  }

  NotificationScanner(Scanner scanner, Predicate<RowColumnValue> filter) {
    scanner.clearColumns();
    Notification.configureScanner(scanner);
    this.scanner = scanner;
    this.filter = filter;
  }

  @VisibleForTesting
  NotificationScanner(Iterable<Entry<Key, Value>> scanner, Collection<Column> columns) {
    this.scanner = scanner;
    this.filter = createColumnFilter(columns);
  }

  @Override
  public Iterator<RowColumnValue> iterator() {
    Iterator<RowColumnValue> iter = Iterators.transform(scanner.iterator(), entry -> {
      Notification n = Notification.from(entry.getKey());
      return new RowColumnValue(n.getRow(), n.getColumn(), Bytes.of(entry.getValue().get()));
    });

    return Iterators.filter(iter, rcv -> filter.test(rcv));
  }
}
