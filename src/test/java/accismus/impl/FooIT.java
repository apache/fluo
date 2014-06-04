package accismus.impl;

import org.junit.Assert;
import org.junit.Test;

import accismus.api.Column;
import accismus.api.exceptions.CommitException;
import accismus.api.types.StringEncoder;
import accismus.api.types.TypeLayer;
import accismus.impl.TransactionImpl.CommitData;

public class FooIT extends Base {
  static TypeLayer typeLayer = new TypeLayer(new StringEncoder());

  @Test
  public void testAck() throws Exception {
    TestTransaction tx = new TestTransaction(config);

    Column balanceCol = typeLayer.newColumn("account", "balance");
    Column addrCol = typeLayer.newColumn("account", "addr");

    tx.set().row("bob").col(balanceCol).val("10");
    tx.set().row("joe").col(balanceCol).val("20");
    tx.set().row("jill").col(balanceCol).val("60");

    tx.commit();

    TestTransaction tx1 = new TestTransaction(config, "bob", balanceCol);
    TestTransaction tx2 = new TestTransaction(config, "bob", balanceCol);

    tx1.get().row("bob").col(balanceCol).toString();
    tx2.get().row("bob").col(balanceCol).toString();

    tx1.get().row("bob").col(addrCol).toString();
    tx2.get().row("bob").col(addrCol).toString();

    tx1.set().row("bob").col(addrCol).val("1 loop pl");
    tx2.set().row("bob").col(addrCol).val("1 loop pl");

    CommitData cd = tx1.createCommitData();
    Assert.assertTrue(tx1.preCommit(cd));
    
    printTable();

    System.out.println();

    try {
      tx2.commit();
    } catch (CommitException ce) {

    }

    printTable();

    long commitTs = OracleClient.getInstance(config).getTimestamp();
    Assert.assertTrue(tx1.commitPrimaryColumn(cd, commitTs));
    tx1.finishCommit(cd, commitTs);

    TestTransaction tx3 = new TestTransaction(config, "bob", balanceCol);
    tx3.set().row("bob").col(addrCol).val("2 loop pl");
    tx3.commit();

  }
}
