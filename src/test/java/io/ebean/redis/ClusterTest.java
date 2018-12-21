package io.ebean.redis;

import domain.EFoo;
import io.ebean.Ebean;
import io.ebean.EbeanServer;
import io.ebean.EbeanServerFactory;
import io.ebean.config.ServerConfig;
import org.junit.Test;

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
  public void test() {

    EbeanServer server = Ebean.getDefaultServer();

    EbeanServer other = createOther();

    for (int i = 0; i < 10; i++) {
      EFoo foo = new EFoo("name "+i);
      foo.save();
    }


    other.getServerCacheManager().clearAll();

    EFoo foo0 = other.find(EFoo.class, 1);
    EFoo foo1 = other.find(EFoo.class, 1);
    EFoo foo2 = other.find(EFoo.class, 1);

    other.getServerCacheManager().clearAll();

    System.out.println("done");

    EFoo foo3 = other.find(EFoo.class, 1);

    System.out.println("done");

  }
}
