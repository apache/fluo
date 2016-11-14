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

package org.apache.fluo.core.log;

import java.util.Iterator;

import com.google.common.collect.Iterators;
import org.apache.fluo.api.client.scanner.ColumnScanner;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.ColumnValue;
import org.apache.fluo.core.util.Hex;
import org.slf4j.Logger;

public class TracingColumnScanner implements ColumnScanner {

  static final Logger log = TracingCellScanner.log;

  private final ColumnScanner cs;
  private final long txid;
  private final String scanId;
  private final String encRow;

  TracingColumnScanner(ColumnScanner cs, long txid, String scanId) {
    this.cs = cs;
    this.txid = txid;
    this.scanId = scanId;
    this.encRow = Hex.encNonAscii(cs.getRow());
  }

  @Override
  public Iterator<ColumnValue> iterator() {
    return Iterators.transform(
        cs.iterator(),
        cv -> {
          log.trace("txid: {} scanId: {} next()-> {} {} {}", txid, scanId, encRow,
              Hex.encNonAscii(cv.getColumn()), Hex.encNonAscii(cv.getValue()));
          return cv;
        });
  }

  @Override
  public Bytes getRow() {
    return cs.getRow();
  }

  @Override
  public String getsRow() {
    return cs.getsRow();
  }

}
