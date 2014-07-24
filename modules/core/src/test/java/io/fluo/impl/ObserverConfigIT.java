package io.fluo.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import io.fluo.api.AbstractObserver;
import io.fluo.api.Bytes;
import io.fluo.api.Column;
import io.fluo.api.Observer.NotificationType;
import io.fluo.api.Transaction;
import io.fluo.api.config.ObserverConfiguration;
import io.fluo.api.types.StringEncoder;
import io.fluo.api.types.TypeLayer;

public class ObserverConfigIT extends Base {

  private static TypeLayer tl = new TypeLayer(new StringEncoder());

  public static class ConfigurableObserver extends AbstractObserver {

    private ObservedColumn observedColumn;
    private Bytes outputCQ;
    private boolean setWeakNotification = false;

    @Override
    public void init(Map<String,String> config) {
      String ocTokens[] = config.get("observedCol").split(":");
      observedColumn = new ObservedColumn(tl.newColumn(ocTokens[0], ocTokens[1]), NotificationType.valueOf(ocTokens[2]));
      outputCQ = Bytes.wrap(config.get("outputCQ"));
      String swn = config.get("setWeakNotification");
      if (swn != null && swn.equals("true"))
        setWeakNotification = true;
    }

    @Override
    public void process(Transaction tx, Bytes row, Column col) throws Exception {

      Bytes in = tx.get(row, col);
      tx.delete(row, col);

      Column outCol = new Column(col.getFamily(), outputCQ);

      tx.set(row, outCol, in);

      if (setWeakNotification)
        tx.setWeakNotification(row, outCol);
    }

    @Override
    public ObservedColumn getObservedColumn() {
      return observedColumn;
    }
  }

  Map<String,String> newMap(String... args) {
    HashMap<String,String> ret = new HashMap<String,String>();
    for (int i = 0; i < args.length; i += 2)
      ret.put(args[i], args[i + 1]);
    return ret;
  }

  @Override
  protected List<ObserverConfiguration> getObservers() {
    List<ObserverConfiguration> observers = new ArrayList<ObserverConfiguration>();

    observers.add(new ObserverConfiguration(ConfigurableObserver.class.getName()).setParameters(newMap("observedCol", "fam1:col1:" + NotificationType.STRONG,
        "outputCQ", "col2")));

    observers.add(new ObserverConfiguration(ConfigurableObserver.class.getName()).setParameters(newMap("observedCol", "fam1:col2:" + NotificationType.STRONG,
        "outputCQ", "col3", "setWeakNotification", "true")));

    observers.add(new ObserverConfiguration(ConfigurableObserver.class.getName()).setParameters(newMap("observedCol", "fam1:col3:" + NotificationType.WEAK,
        "outputCQ", "col4")));

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
