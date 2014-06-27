package org.fluo.impl;

import java.util.HashMap;
import java.util.Map;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.junit.Assert;
import org.junit.Test;

import org.fluo.api.AbstractObserver;
import org.fluo.api.Column;
import org.fluo.api.Transaction;
import org.fluo.api.config.ObserverConfiguration;
import org.fluo.api.types.StringEncoder;
import org.fluo.api.types.TypeLayer;

public class ObserverConfigIT extends Base {

  private static TypeLayer tl = new TypeLayer(new StringEncoder());

  public static class ConfigurableObserver extends AbstractObserver {

    private ByteSequence outputCQ;
    private boolean setWeakNotification = false;

    @Override
    public void init(Map<String,String> config) {
      outputCQ = new ArrayByteSequence(config.get("outputCQ"));
      String swn = config.get("setWeakNotification");
      if (swn != null && swn.equals("true"))
        setWeakNotification = true;
    }

    @Override
    public void process(Transaction tx, ByteSequence row, Column col) throws Exception {

      ByteSequence in = tx.get(row, col);
      tx.delete(row, col);

      Column outCol = new Column(col.getFamily(), outputCQ);

      tx.set(row, outCol, in);

      if (setWeakNotification)
        tx.setWeakNotification(row, outCol);
    }
  }

  Map<String,String> newMap(String... args) {
    HashMap<String,String> ret = new HashMap<String,String>();
    for (int i = 0; i < args.length; i += 2)
      ret.put(args[i], args[i + 1]);
    return ret;
  }

  protected Map<Column,ObserverConfiguration> getObservers() {
    Map<Column,ObserverConfiguration> observers = new HashMap<Column,ObserverConfiguration>();

    observers.put(tl.newColumn("fam1", "col1"), new ObserverConfiguration(ConfigurableObserver.class.getName()).setParameters(newMap("outputCQ", "col2")));

    observers.put(tl.newColumn("fam1", "col2"),
        new ObserverConfiguration(ConfigurableObserver.class.getName()).setParameters(newMap("outputCQ", "col3", "setWeakNotification", "true")));
    return observers;
  }

  protected Map<Column,ObserverConfiguration> getWeakObservers() {
    Map<Column,ObserverConfiguration> observers = new HashMap<Column,ObserverConfiguration>();
    observers.put(tl.newColumn("fam1", "col3"), new ObserverConfiguration(ConfigurableObserver.class.getName()).setParameters(newMap("outputCQ", "col4")));
    return observers;
  }

  @Test
  public void testObserverConfig() throws Exception {

    TestTransaction tx1 = new TestTransaction(config);
    tx1.mutate().row("r1").fam("fam1").qual("col1").set("abcdefg");
    tx1.commit();

    runWorker();

    TestTransaction tx2 = new TestTransaction(config);
    Assert.assertNull(tx2.get().row("r1").fam("fam1").qual("col1").toString());
    Assert.assertNull(tx2.get().row("r1").fam("fam1").qual("col2").toString());
    Assert.assertNull(tx2.get().row("r1").fam("fam1").qual("col3").toString());
    Assert.assertEquals("abcdefg", tx2.get().row("r1").fam("fam1").qual("col4").toString());
  }

}
