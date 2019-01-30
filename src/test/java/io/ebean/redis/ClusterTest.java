package io.ebean.redis;

import domain.EFoo;
import io.ebean.Ebean;
import io.ebean.EbeanServer;
import io.ebean.EbeanServerFactory;
import io.ebean.config.ServerConfig;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class ClusterTest {


  private EbeanServer createOther() {

    ServerConfig config = new ServerConfig();
    config.loadFromProperties();
    config.setDefaultServer(false);
    config.setName("other");
    config.setDdlGenerate(false);
    config.setDdlRun(false);

    return EbeanServerFactory.create(config);
  }

  @Test
  public void test() throws InterruptedException {

    // ensure the default server exists first
    Ebean.getDefaultServer();

    EbeanServer other = createOther();

    for (int i = 0; i < 10; i++) {
      EFoo foo = new EFoo("name " + i);
      foo.save();
    }


    other.getServerCacheManager().clearAll();

    DuelCache dualCache = (DuelCache) other.getServerCacheManager().getBeanCache(EFoo.class);

    EFoo foo0 = other.find(EFoo.class, 1);
    assertCounts(dualCache, 0, 1, 0, 1);

    other.find(EFoo.class, 1);
    assertCounts(dualCache, 1, 1, 0, 1);

    other.find(EFoo.class, 1);
    assertCounts(dualCache, 2, 1, 0, 1);

    other.find(EFoo.class, 1);
    assertCounts(dualCache, 3, 1, 0, 1);

    other.find(EFoo.class, 2);
    assertCounts(dualCache, 3, 2, 0, 2);


    foo0.setName("name2");
    foo0.save();
    allowAsyncMessaging();

    EFoo foo3 = other.find(EFoo.class, 1);
    assertThat(foo3.getName()).isEqualTo("name2");
    assertCounts(dualCache, 3, 3, 1, 2);


    foo0.setName("name3");
    foo0.save();
    allowAsyncMessaging();

    foo3 = other.find(EFoo.class, 1);
    assertThat(foo3.getName()).isEqualTo("name3");
    assertCounts(dualCache, 3, 4, 2, 2);

    System.out.println("done");
  }

  private void assertCounts(DuelCache dualCache, int nearHits, int nearMiss, int remoteHit, int remoteMiss) {

    assertThat(dualCache.getNearHitCount()).isEqualTo(nearHits);
    assertThat(dualCache.getNearMissCount()).isEqualTo(nearMiss);
    assertThat(dualCache.getRemoteHitCount()).isEqualTo(remoteHit);
    assertThat(dualCache.getRemoteMissCount()).isEqualTo(remoteMiss);
  }

  private void allowAsyncMessaging() throws InterruptedException {
    Thread.sleep(200);
  }
}
