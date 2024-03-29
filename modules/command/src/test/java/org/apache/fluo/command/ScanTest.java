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

package org.apache.fluo.command;

import com.beust.jcommander.JCommander;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.data.RowColumn;
import org.apache.fluo.api.data.Span;
import org.apache.fluo.core.impl.SnapshotScanner;
import org.apache.fluo.core.util.ScanUtil;
import org.junit.Assert;
import org.junit.Test;

/**
 * Unit test for ScanUtil
 */
public class ScanTest {

  private SnapshotScanner.Opts parseArgs(String args) {
    FluoScan scan = new FluoScan();
    JCommander jcommand = new JCommander(scan);
    jcommand.parse(args.split(" "));
    ScanUtil.ScanOpts opts = scan.getScanOpts();
    return new SnapshotScanner.Opts(ScanUtil.getSpan(opts), ScanUtil.getColumns(opts), false,
        Authorizations.EMPTY);
  }

  @Test
  public void testValidInput() {
    SnapshotScanner.Opts config;

    config = parseArgs("-a app");
    Assert.assertEquals(RowColumn.EMPTY, config.getSpan().getStart());
    Assert.assertEquals(RowColumn.EMPTY, config.getSpan().getEnd());
    Assert.assertEquals(0, config.getColumns().size());

    config = parseArgs("-a app -s start -e end -c col1,col2");
    Assert.assertEquals(new RowColumn("start"), config.getSpan().getStart());
    Assert.assertEquals(new RowColumn("end").following(), config.getSpan().getEnd());
    Assert.assertEquals(2, config.getColumns().size());
    Assert.assertTrue(config.getColumns().contains(new Column("col1")));
    Assert.assertTrue(config.getColumns().contains(new Column("col2")));

    config = parseArgs("-a app -s start -c cf:cq");
    Assert.assertEquals(new RowColumn("start"), config.getSpan().getStart());
    Assert.assertEquals(RowColumn.EMPTY, config.getSpan().getEnd());
    Assert.assertEquals(1, config.getColumns().size());
    Assert.assertTrue(config.getColumns().contains(new Column("cf", "cq")));

    config = parseArgs("-a app -e end");
    Assert.assertEquals(RowColumn.EMPTY, config.getSpan().getStart());
    Assert.assertEquals(new RowColumn("end").following(), config.getSpan().getEnd());
    Assert.assertEquals(0, config.getColumns().size());

    config = parseArgs("-a app -p myprefix");
    Assert.assertEquals(Span.prefix("myprefix"), config.getSpan());
    Assert.assertEquals(0, config.getColumns().size());

    config = parseArgs("-a app -r exactRow");
    Assert.assertEquals(Span.exact("exactRow"), config.getSpan());
    Assert.assertEquals(0, config.getColumns().size());

    config = parseArgs("-a app -c cf1:cq1,cf2:cq2");
    Assert.assertEquals(RowColumn.EMPTY, config.getSpan().getStart());
    Assert.assertEquals(RowColumn.EMPTY, config.getSpan().getEnd());
    Assert.assertEquals(2, config.getColumns().size());
    Assert.assertTrue(config.getColumns().contains(new Column("cf1", "cq1")));
    Assert.assertTrue(config.getColumns().contains(new Column("cf2", "cq2")));
  }

  @Test
  public void testBadInputs() {
    for (String extraArg : new String[] {"-r exactRow", "-s start", "-e end", "-s start -e end"}) {
      try {
        parseArgs("-a app -p prefix " + extraArg);
        Assert.fail();
      } catch (IllegalArgumentException e) {
      }
    }

    for (String extraArg : new String[] {"-p prefix", "-s start", "-e end", "-s start -e end"}) {
      try {
        parseArgs("-a app -r exactRow " + extraArg);
        Assert.fail();
      } catch (IllegalArgumentException e) {
      }
    }

    try {
      parseArgs("-a app -c col1,cf:cq:oops");
      Assert.fail();
    } catch (IllegalArgumentException e) {
    }
  }
}
