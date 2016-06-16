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

package org.apache.fluo.core.types;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;

import com.google.common.collect.ImmutableSet;
import org.apache.fluo.api.data.Bytes;
import org.apache.fluo.api.data.Column;
import org.apache.fluo.api.types.StringEncoder;
import org.apache.fluo.api.types.TypeLayer;
import org.apache.fluo.api.types.TypedSnapshot;
import org.apache.fluo.api.types.TypedSnapshotBase.Value;
import org.apache.fluo.api.types.TypedTransactionBase;
import org.junit.Assert;
import org.junit.Test;

public class TypeLayerTest {

  @Test
  public void testColumns() throws Exception {
    TypeLayer tl = new TypeLayer(new StringEncoder());

    MockTransactionBase tt =
        new MockTransactionBase("r1,cf1:cq1,v1", "r1,cf1:cq2,v2", "r1,cf1:cq3,9", "r2,cf2:7,12",
            "r2,cf2:8,13", "13,9:17,20", "13,9:18,20", "13,9:19,20", "13,9:20,20");

    TypedTransactionBase ttx = tl.wrap(tt);

    Map<Column, Value> results =
        ttx.get().row("r2")
            .columns(ImmutableSet.of(new Column("cf2", "6"), new Column("cf2", "7")));

    Assert.assertNull(results.get(new Column("cf2", "6")).toInteger());
    Assert.assertEquals(0, results.get(new Column("cf2", "6")).toInteger(0));
    Assert.assertEquals(12, (int) results.get(new Column("cf2", "7")).toInteger());
    Assert.assertEquals(12, results.get(new Column("cf2", "7")).toInteger(0));

    Assert.assertEquals(1, results.size());

    results =
        ttx.get()
            .row("r2")
            .columns(
                ImmutableSet.of(new Column("cf2", "6"), new Column("cf2", "7"), new Column("cf2",
                    "8")));

    Assert.assertNull(results.get(new Column("cf2", "6")).toInteger());
    Assert.assertEquals(0, results.get(new Column("cf2", "6")).toInteger(0));
    Assert.assertEquals(12, (int) results.get(new Column("cf2", "7")).toInteger());
    Assert.assertEquals(12, results.get(new Column("cf2", "7")).toInteger(0));
    Assert.assertEquals(13, (int) results.get(new Column("cf2", "8")).toInteger());
    Assert.assertEquals(13, results.get(new Column("cf2", "8")).toInteger(0));

    Assert.assertEquals(2, results.size());

    // test var args
    Map<Column, Value> results2 =
        ttx.get().row("r2")
            .columns(new Column("cf2", "6"), new Column("cf2", "7"), new Column("cf2", "8"));
    Assert.assertEquals(results, results2);
  }

  @Test
  public void testVis() throws Exception {
    TypeLayer tl = new TypeLayer(new StringEncoder());

    MockTransactionBase tt = new MockTransactionBase("r1,cf1:cq1:A,v1", "r1,cf1:cq2:A&B,v2");

    TypedTransactionBase ttx = tl.wrap(tt);

    Assert.assertNull(ttx.get().row("r1").fam("cf1").qual("cq1").toString());
    Assert.assertEquals("v1", ttx.get().row("r1").fam("cf1").qual("cq1").vis("A").toString());
    Assert.assertEquals("v1", ttx.get().row("r1").fam("cf1").qual("cq1").vis("A".getBytes())
        .toString());
    Assert.assertEquals("v1", ttx.get().row("r1").fam("cf1").qual("cq1").vis(Bytes.of("A"))
        .toString());
    Assert.assertEquals("v1",
        ttx.get().row("r1").fam("cf1").qual("cq1").vis(ByteBuffer.wrap("A".getBytes())).toString());

    Assert.assertNull("v1", ttx.get().row("r1").fam("cf1").qual("cq1").vis("A&B").toString());
    Assert.assertNull("v1", ttx.get().row("r1").fam("cf1").qual("cq1").vis("A&B".getBytes())
        .toString());
    Assert.assertNull("v1", ttx.get().row("r1").fam("cf1").qual("cq1").vis(Bytes.of("A&B"))
        .toString());
    Assert.assertNull("v1",
        ttx.get().row("r1").fam("cf1").qual("cq1").vis(ByteBuffer.wrap("A&B".getBytes()))
            .toString());

    Assert.assertEquals("v3", ttx.get().row("r1").fam("cf1").qual("cq1").vis("A&B").toString("v3"));
    Assert.assertEquals("v3", ttx.get().row("r1").fam("cf1").qual("cq1").vis("A&B".getBytes())
        .toString("v3"));
    Assert.assertEquals("v3", ttx.get().row("r1").fam("cf1").qual("cq1").vis(Bytes.of("A&B"))
        .toString("v3"));
    Assert.assertEquals(
        "v3",
        ttx.get().row("r1").fam("cf1").qual("cq1").vis(ByteBuffer.wrap("A&B".getBytes()))
            .toString("v3"));

    ttx.mutate().row("r1").fam("cf1").qual("cq1").vis("A&B").set(3);
    ttx.mutate().row("r1").fam("cf1").qual("cq1").vis("A&C".getBytes()).set(4);
    ttx.mutate().row("r1").fam("cf1").qual("cq1").vis(Bytes.of("A&D")).set(5);
    ttx.mutate().row("r1").fam("cf1").qual("cq1").vis(ByteBuffer.wrap("A&F".getBytes())).set(7);

    Assert.assertEquals(MockTransactionBase.toRCVM("r1,cf1:cq1:A&B,3", "r1,cf1:cq1:A&C,4",
        "r1,cf1:cq1:A&D,5", "r1,cf1:cq1:A&F,7"), tt.setData);
    tt.setData.clear();

    ttx.mutate().row("r1").fam("cf1").qual("cq1").vis("A&B").delete();
    ttx.mutate().row("r1").fam("cf1").qual("cq1").vis("A&C".getBytes()).delete();
    ttx.mutate().row("r1").fam("cf1").qual("cq1").vis(Bytes.of("A&D")).delete();
    ttx.mutate().row("r1").fam("cf1").qual("cq1").vis(ByteBuffer.wrap("A&F".getBytes())).delete();

    Assert.assertEquals(MockTransactionBase.toRCM("r1,cf1:cq1:A&B", "r1,cf1:cq1:A&C",
        "r1,cf1:cq1:A&D", "r1,cf1:cq1:A&F"), tt.deletes);
    tt.deletes.clear();
    Assert.assertEquals(0, tt.setData.size());
    Assert.assertEquals(0, tt.weakNotifications.size());

  }

  @Test
  public void testBuildColumn() {
    TypeLayer tl = new TypeLayer(new StringEncoder());

    Assert.assertEquals(new Column("f0", "q0"), tl.bc().fam("f0".getBytes()).qual("q0".getBytes())
        .vis());
    Assert.assertEquals(new Column("f0", "q0"), tl.bc().fam("f0").qual("q0").vis());
    Assert.assertEquals(new Column("5", "7"), tl.bc().fam(5).qual(7).vis());
    Assert.assertEquals(new Column("5", "7"), tl.bc().fam(5l).qual(7l).vis());
    Assert.assertEquals(new Column("5", "7"), tl.bc().fam(Bytes.of("5")).qual(Bytes.of("7")).vis());
    Assert.assertEquals(new Column("5", "7"),
        tl.bc().fam(ByteBuffer.wrap("5".getBytes())).qual(ByteBuffer.wrap("7".getBytes())).vis());

    Assert.assertEquals(new Column("f0", "q0", "A&B"),
        tl.bc().fam("f0".getBytes()).qual("q0".getBytes()).vis("A&B"));
    Assert.assertEquals(new Column("f0", "q0", "A&C"),
        tl.bc().fam("f0").qual("q0").vis("A&C".getBytes()));
    Assert.assertEquals(new Column("5", "7", "A&D"), tl.bc().fam(5).qual(7).vis(Bytes.of("A&D")));
    Assert.assertEquals(new Column("5", "7", "A&D"),
        tl.bc().fam(5).qual(7).vis(ByteBuffer.wrap("A&D".getBytes())));
  }

  @Test
  public void testRead() throws Exception {
    TypeLayer tl = new TypeLayer(new StringEncoder());

    MockSnapshot ms =
        new MockSnapshot("r1,cf1:cq1,v1", "r1,cf1:cq2,v2", "r1,cf1:cq3,9", "r2,cf2:7,12",
            "r2,cf2:8,13", "13,9:17,20", "13,9:18,20", "13,9:19,20", "13,9:20,20",
            "r3,cf3:cq3,28.195", "r4,cf4:cq4,true");

    TypedSnapshot tts = tl.wrap(ms);

    Assert.assertEquals("v1", tts.get().row("r1").fam("cf1").qual("cq1").toString());
    Assert.assertEquals("v1", tts.get().row("r1").fam("cf1").qual("cq1").toString("b"));
    Assert.assertEquals("13", tts.get().row("r2").fam("cf2").qual("8").toString());
    Assert.assertEquals("13", tts.get().row("r2").fam("cf2").qual("8").toString("b"));
    Assert.assertEquals("28.195", tts.get().row("r3").fam("cf3").qual("cq3").toString());
    Assert.assertEquals("28.195", tts.get().row("r3").fam("cf3").qual("cq3").toString("b"));
    Assert.assertEquals("true", tts.get().row("r4").fam("cf4").qual("cq4").toString());
    Assert.assertEquals("true", tts.get().row("r4").fam("cf4").qual("cq4").toString("b"));

    // try converting to different types
    Assert.assertEquals("13", tts.get().row("r2").fam("cf2").qual(8).toString());
    Assert.assertEquals("13", tts.get().row("r2").fam("cf2").qual(8).toString("b"));
    Assert.assertEquals((Integer) 13, tts.get().row("r2").fam("cf2").qual(8).toInteger());
    Assert.assertEquals(13, tts.get().row("r2").fam("cf2").qual(8).toInteger(14));
    Assert.assertEquals((Long) 13l, tts.get().row("r2").fam("cf2").qual(8).toLong());
    Assert.assertEquals(13l, tts.get().row("r2").fam("cf2").qual(8).toLong(14l));
    Assert.assertEquals("13", new String(tts.get().row("r2").fam("cf2").qual(8).toBytes()));
    Assert.assertEquals("13",
        new String(tts.get().row("r2").fam("cf2").qual(8).toBytes("14".getBytes())));
    Assert
        .assertEquals("13", new String(tts.get().row("r2").col(new Column("cf2", "8")).toBytes()));
    Assert.assertEquals("13",
        new String(tts.get().row("r2").col(new Column("cf2", "8")).toBytes("14".getBytes())));
    Assert.assertEquals("13",
        Bytes.of(tts.get().row("r2").col(new Column("cf2", "8")).toByteBuffer()).toString());
    Assert.assertEquals(
        "13",
        Bytes.of(
            tts.get().row("r2").col(new Column("cf2", "8"))
                .toByteBuffer(ByteBuffer.wrap("14".getBytes()))).toString());

    // test non-existent
    Assert.assertNull(tts.get().row("r2").fam("cf3").qual(8).toInteger());
    Assert.assertEquals(14, tts.get().row("r2").fam("cf3").qual(8).toInteger(14));
    Assert.assertNull(tts.get().row("r2").fam("cf3").qual(8).toLong());
    Assert.assertEquals(14l, tts.get().row("r2").fam("cf3").qual(8).toLong(14l));
    Assert.assertNull(tts.get().row("r2").fam("cf3").qual(8).toString());
    Assert.assertEquals("14", tts.get().row("r2").fam("cf3").qual(8).toString("14"));
    Assert.assertNull(tts.get().row("r2").fam("cf3").qual(8).toBytes());
    Assert.assertEquals("14",
        new String(tts.get().row("r2").fam("cf3").qual(8).toBytes("14".getBytes())));
    Assert.assertNull(tts.get().row("r2").col(new Column("cf3", "8")).toBytes());
    Assert.assertEquals("14",
        new String(tts.get().row("r2").col(new Column("cf3", "8")).toBytes("14".getBytes())));
    Assert.assertNull(tts.get().row("r2").col(new Column("cf3", "8")).toByteBuffer());
    Assert.assertEquals(
        "14",
        Bytes.of(
            tts.get().row("r2").col(new Column("cf3", "8"))
                .toByteBuffer(ByteBuffer.wrap("14".getBytes()))).toString());

    // test float & double
    Assert.assertEquals((Float) 28.195f, tts.get().row("r3").fam("cf3").qual("cq3").toFloat());
    Assert.assertEquals(28.195f, tts.get().row("r3").fam("cf3").qual("cq3").toFloat(39.383f), 0.0);
    Assert.assertEquals((Double) 28.195d, tts.get().row("r3").fam("cf3").qual("cq3").toDouble());
    Assert.assertEquals(28.195d, tts.get().row("r3").fam("cf3").qual("cq3").toDouble(39.383d), 0.0);

    // test boolean
    Assert.assertEquals(true, tts.get().row("r4").fam("cf4").qual("cq4").toBoolean());
    Assert.assertEquals(true, tts.get().row("r4").fam("cf4").qual("cq4").toBoolean());
    Assert.assertEquals(true, tts.get().row("r4").fam("cf4").qual("cq4").toBoolean(false));
    Assert.assertEquals(true, tts.get().row("r4").fam("cf4").qual("cq4").toBoolean(false));

    // try different types for row
    Assert.assertEquals("20", tts.get().row(13).fam("9").qual("17").toString());
    Assert.assertEquals("20", tts.get().row(13l).fam("9").qual("17").toString());
    Assert.assertEquals("20", tts.get().row("13").fam("9").qual("17").toString());
    Assert.assertEquals("20", tts.get().row("13".getBytes()).fam("9").qual("17").toString());
    Assert.assertEquals("20", tts.get().row(ByteBuffer.wrap("13".getBytes())).fam("9").qual("17")
        .toString());

    // try different types for cf
    Assert.assertEquals("20", tts.get().row("13").fam(9).qual("17").toString());
    Assert.assertEquals("20", tts.get().row("13").fam(9l).qual("17").toString());
    Assert.assertEquals("20", tts.get().row("13").fam("9").qual("17").toString());
    Assert.assertEquals("20", tts.get().row("13").fam("9".getBytes()).qual("17").toString());
    Assert.assertEquals("20", tts.get().row("13").fam(ByteBuffer.wrap("9".getBytes())).qual("17")
        .toString());

    // try different types for cq
    Assert.assertEquals("20", tts.get().row("13").fam("9").qual("17").toString());
    Assert.assertEquals("20", tts.get().row("13").fam("9").qual(17l).toString());
    Assert.assertEquals("20", tts.get().row("13").fam("9").qual(17).toString());
    Assert.assertEquals("20", tts.get().row("13").fam("9").qual("17".getBytes()).toString());
    Assert.assertEquals("20", tts.get().row("13").fam("9").qual(ByteBuffer.wrap("17".getBytes()))
        .toString());

    ms.close();
    tts.close();
  }

  @Test
  public void testWrite() throws Exception {

    TypeLayer tl = new TypeLayer(new StringEncoder());

    MockTransactionBase tt =
        new MockTransactionBase("r1,cf1:cq1,v1", "r1,cf1:cq2,v2", "r1,cf1:cq3,9", "r2,cf2:7,12",
            "r2,cf2:8,13", "13,9:17,20", "13,9:18,20", "13,9:19,20", "13,9:20,20");

    TypedTransactionBase ttx = tl.wrap(tt);

    // test increments data
    ttx.mutate().row("13").fam("9").qual("17").increment(1);
    ttx.mutate().row("13").fam("9").qual(18).increment(2);
    ttx.mutate().row("13").fam("9").qual(19l).increment(3);
    ttx.mutate().row("13").fam("9").qual("20".getBytes()).increment(4);
    ttx.mutate().row("13").fam("9").qual(Bytes.of("21")).increment(5); // increment non existent
    ttx.mutate().row("13").col(new Column("9", "22")).increment(6); // increment non existent
    ttx.mutate().row("13").fam("9").qual(ByteBuffer.wrap("23".getBytes())).increment(7); // increment
                                                                                         // non
                                                                                         // existent

    Assert.assertEquals(MockTransactionBase.toRCVM("13,9:17,21", "13,9:18,22", "13,9:19,23",
        "13,9:20,24", "13,9:21,5", "13,9:22,6", "13,9:23,7"), tt.setData);
    tt.setData.clear();

    // test increments long
    ttx.mutate().row("13").fam("9").qual("17").increment(1l);
    ttx.mutate().row("13").fam("9").qual(18).increment(2l);
    ttx.mutate().row("13").fam("9").qual(19l).increment(3l);
    ttx.mutate().row("13").fam("9").qual("20".getBytes()).increment(4l);
    ttx.mutate().row("13").fam("9").qual(Bytes.of("21")).increment(5l); // increment non existent
    ttx.mutate().row("13").col(new Column("9", "22")).increment(6l); // increment non existent
    ttx.mutate().row("13").fam("9").qual(ByteBuffer.wrap("23".getBytes())).increment(7l); // increment
                                                                                          // non
                                                                                          // existent

    Assert.assertEquals(MockTransactionBase.toRCVM("13,9:17,21", "13,9:18,22", "13,9:19,23",
        "13,9:20,24", "13,9:21,5", "13,9:22,6", "13,9:23,7"), tt.setData);
    tt.setData.clear();

    // test setting data
    ttx.mutate().row("13").fam("9").qual("16").set();
    ttx.mutate().row("13").fam("9").qual("17").set(3);
    ttx.mutate().row("13").fam("9").qual(18).set(4l);
    ttx.mutate().row("13").fam("9").qual(19l).set("5");
    ttx.mutate().row("13").fam("9").qual("20".getBytes()).set("6".getBytes());
    ttx.mutate().row("13").col(new Column("9", "21")).set("7".getBytes());
    ttx.mutate().row("13").fam("9").qual(ByteBuffer.wrap("22".getBytes()))
        .set(ByteBuffer.wrap("8".getBytes()));
    ttx.mutate().row("13").fam("9").qual("23").set(2.54f);
    ttx.mutate().row("13").fam("9").qual("24").set(-6.135d);
    ttx.mutate().row("13").fam("9").qual("25").set(false);

    Assert.assertEquals(MockTransactionBase.toRCVM("13,9:16,", "13,9:17,3", "13,9:18,4",
        "13,9:19,5", "13,9:20,6", "13,9:21,7", "13,9:22,8", "13,9:23,2.54", "13,9:24,-6.135",
        "13,9:25,false"), tt.setData);
    tt.setData.clear();

    // test deleting data
    ttx.mutate().row("13").fam("9").qual("17").delete();
    ttx.mutate().row("13").fam("9").qual(18).delete();
    ttx.mutate().row("13").fam("9").qual(19l).delete();
    ttx.mutate().row("13").fam("9").qual("20".getBytes()).delete();
    ttx.mutate().row("13").col(new Column("9", "21")).delete();
    ttx.mutate().row("13").fam("9").qual(ByteBuffer.wrap("22".getBytes())).delete();

    Assert
        .assertEquals(MockTransactionBase.toRCM("13,9:17", "13,9:18", "13,9:19", "13,9:20",
            "13,9:21", "13,9:22"), tt.deletes);
    tt.deletes.clear();
    Assert.assertEquals(0, tt.setData.size());
    Assert.assertEquals(0, tt.weakNotifications.size());

    // test weak notifications
    ttx.mutate().row("13").fam("9").qual("17").weaklyNotify();
    ttx.mutate().row("13").fam("9").qual(18).weaklyNotify();
    ttx.mutate().row("13").fam("9").qual(19l).weaklyNotify();
    ttx.mutate().row("13").fam("9").qual("20".getBytes()).weaklyNotify();
    ttx.mutate().row("13").col(new Column("9", "21")).weaklyNotify();
    ttx.mutate().row("13").fam("9").qual(ByteBuffer.wrap("22".getBytes())).weaklyNotify();

    Assert
        .assertEquals(MockTransactionBase.toRCM("13,9:17", "13,9:18", "13,9:19", "13,9:20",
            "13,9:21", "13,9:22"), tt.weakNotifications);
    tt.weakNotifications.clear();
    Assert.assertEquals(0, tt.setData.size());
    Assert.assertEquals(0, tt.deletes.size());
  }

  @Test
  public void testMultiRow() throws Exception {
    TypeLayer tl = new TypeLayer(new StringEncoder());

    MockTransactionBase tt =
        new MockTransactionBase("11,cf1:cq1,1", "11,cf1:cq2,2", "12,cf1:cq1,3", "12,cf1:cq2,4",
            "13,cf1:cq1,5", "13,cf1:cq2,6");

    TypedTransactionBase ttx = tl.wrap(tt);

    Bytes br1 = Bytes.of("11");
    Bytes br2 = Bytes.of("12");
    Bytes br3 = Bytes.of("13");

    Column c1 = new Column("cf1", "cq1");
    Column c2 = new Column("cf1", "cq2");

    Map<Bytes, Map<Column, Value>> map1 =
        ttx.get().rows(Arrays.asList(br1, br2)).columns(c1).toBytesMap();

    Assert.assertEquals(map1, ttx.get().rows(br1, br2).columns(c1).toBytesMap());

    Assert.assertEquals("1", map1.get(br1).get(c1).toString());
    Assert.assertEquals("1", map1.get(br1).get(c1).toString("5"));
    Assert.assertEquals((Long) (1l), map1.get(br1).get(c1).toLong());
    Assert.assertEquals(1l, map1.get(br1).get(c1).toLong(5));
    Assert.assertEquals((Integer) (1), map1.get(br1).get(c1).toInteger());
    Assert.assertEquals(1, map1.get(br1).get(c1).toInteger(5));

    Assert.assertEquals("5", map1.get(br3).get(c1).toString("5"));
    Assert.assertNull(map1.get(br3).get(c1).toString());
    Assert.assertEquals(5l, map1.get(br3).get(c1).toLong(5l));
    Assert.assertNull(map1.get(br3).get(c1).toLong());
    Assert.assertEquals(5, map1.get(br1).get(c2).toInteger(5));
    Assert.assertNull(map1.get(br1).get(c2).toInteger());

    Assert.assertEquals(2, map1.size());
    Assert.assertEquals(1, map1.get(br1).size());
    Assert.assertEquals(1, map1.get(br2).size());
    Assert.assertEquals("3", map1.get(br2).get(c1).toString());

    Map<String, Map<Column, Value>> map2 =
        ttx.get().rowsString(Arrays.asList("11", "13")).columns(c1).toStringMap();

    Assert.assertEquals(map2, ttx.get().rowsString("11", "13").columns(c1).toStringMap());

    Assert.assertEquals(2, map2.size());
    Assert.assertEquals(1, map2.get("11").size());
    Assert.assertEquals(1, map2.get("13").size());
    Assert.assertEquals((Long) (1l), map2.get("11").get(c1).toLong());
    Assert.assertEquals(5l, map2.get("13").get(c1).toLong(6));

    Map<Long, Map<Column, Value>> map3 =
        ttx.get().rowsLong(Arrays.asList(11l, 13l)).columns(c1).toLongMap();

    Assert.assertEquals(map3, ttx.get().rowsLong(11l, 13l).columns(c1).toLongMap());

    Assert.assertEquals(2, map3.size());
    Assert.assertEquals(1, map3.get(11l).size());
    Assert.assertEquals(1, map3.get(13l).size());
    Assert.assertEquals((Long) (1l), map3.get(11l).get(c1).toLong());
    Assert.assertEquals(5l, map3.get(13l).get(c1).toLong(6));

    Map<Integer, Map<Column, Value>> map4 =
        ttx.get().rowsInteger(Arrays.asList(11, 13)).columns(c1).toIntegerMap();

    Assert.assertEquals(map4, ttx.get().rowsInteger(11, 13).columns(c1).toIntegerMap());

    Assert.assertEquals(2, map4.size());
    Assert.assertEquals(1, map4.get(11).size());
    Assert.assertEquals(1, map4.get(13).size());
    Assert.assertEquals((Long) (1l), map4.get(11).get(c1).toLong());
    Assert.assertEquals(5l, map4.get(13).get(c1).toLong(6));

    Map<Integer, Map<Column, Value>> map5 =
        ttx.get().rowsBytes(Arrays.asList("11".getBytes(), "13".getBytes())).columns(c1)
            .toIntegerMap();

    Assert.assertEquals(map5, ttx.get().rowsBytes("11".getBytes(), "13".getBytes()).columns(c1)
        .toIntegerMap());

    Assert.assertEquals(2, map5.size());
    Assert.assertEquals(1, map5.get(11).size());
    Assert.assertEquals(1, map5.get(13).size());
    Assert.assertEquals((Long) (1l), map5.get(11).get(c1).toLong());
    Assert.assertEquals(5l, map5.get(13).get(c1).toLong(6));

    Map<Integer, Map<Column, Value>> map6 =
        ttx.get()
            .rowsByteBuffers(
                Arrays.asList(ByteBuffer.wrap("11".getBytes()), ByteBuffer.wrap("13".getBytes())))
            .columns(c1).toIntegerMap();

    Assert.assertEquals(
        map6,
        ttx.get()
            .rowsByteBuffers(ByteBuffer.wrap("11".getBytes()), ByteBuffer.wrap("13".getBytes()))
            .columns(c1).toIntegerMap());

    Assert.assertEquals(2, map6.size());
    Assert.assertEquals(1, map6.get(11).size());
    Assert.assertEquals(1, map6.get(13).size());
    Assert.assertEquals((Long) (1l), map6.get(11).get(c1).toLong());
    Assert.assertEquals(5l, map6.get(13).get(c1).toLong(6));

  }

  @Test
  public void testBasic() throws Exception {
    TypeLayer tl = new TypeLayer(new StringEncoder());

    MockTransactionBase tt =
        new MockTransactionBase("r1,cf1:cq1,v1", "r1,cf1:cq2,v2", "r1,cf1:cq3,9", "r2,cf2:7,12",
            "r2,cf2:8,13", "13,9:17,20", "13,9:18,20", "13,9:19,20", "13,9:20,20");

    TypedTransactionBase ttx = tl.wrap(tt);

    Assert.assertEquals(Bytes.of("12"), ttx.get(Bytes.of("r2"), new Column("cf2", "7")));
    Assert.assertNull(ttx.get(Bytes.of("r2"), new Column("cf2", "9")));

    Map<Column, Bytes> map =
        ttx.get(Bytes.of("r2"), ImmutableSet.of(new Column("cf2", "7"), new Column("cf2", "8")));
    Assert.assertEquals(2, map.size());
    Assert.assertEquals("12", map.get(new Column("cf2", "7")).toString());
    Assert.assertEquals("13", map.get(new Column("cf2", "8")).toString());

    map = ttx.get(Bytes.of("r6"), ImmutableSet.of(new Column("cf2", "7"), new Column("cf2", "8")));
    Assert.assertEquals(0, map.size());

    ttx.set(Bytes.of("r6"), new Column("cf2", "7"), Bytes.of("3"));
    Assert.assertEquals(MockTransactionBase.toRCVM("r6,cf2:7,3"), tt.setData);
    tt.setData.clear();

    Map<Bytes, Map<Column, Bytes>> map2 =
        ttx.get(ImmutableSet.of(Bytes.of("r1"), Bytes.of("r2")),
            ImmutableSet.of(new Column("cf1", "cq1"), new Column("cf2", "8")));
    Assert.assertEquals(MockTransactionBase.toRCVM("r1,cf1:cq1,v1", "r2,cf2:8,13"), map2);

    ttx.delete(Bytes.of("r6"), new Column("cf2", "7"));
    Assert.assertEquals(MockTransactionBase.toRCM("r6,cf2:7"), tt.deletes);
    tt.deletes.clear();

    ttx.setWeakNotification(Bytes.of("r6"), new Column("cf2", "8"));
    Assert.assertEquals(MockTransactionBase.toRCM("r6,cf2:8"), tt.weakNotifications);
    tt.weakNotifications.clear();

  }
}
