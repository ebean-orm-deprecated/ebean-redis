package io.ebean.redis;

import domain.EFoo;
import io.ebean.Ebean;
import org.junit.Test;

import java.time.LocalDate;

public class IntegrationTest {

  @Test
  public void test() throws InterruptedException {

//    EbeanServer server = Ebean.getDefaultServer();

    EFoo foo = new EFoo("Jack");
    foo.save();

    foo.setName("fo1");
    foo.setLocalDate(LocalDate.now());
    foo.update();

    Thread.sleep(300);
    int id = 1;

    EFoo one = Ebean.find(EFoo.class)
      .setId(id)
      .findOne();

    System.out.println("got "+one);


    EFoo one2 = Ebean.find(EFoo.class)
      .where().like("name", "fo%")
      .setUseQueryCache(true)
      .findOne();

    System.out.println("one2 "+one2);

    one2 = Ebean.find(EFoo.class)
      .where().like("name", "fo%")
      .setUseQueryCache(true)
      .findOne();

    System.out.println("one2 "+one2);

    foo.setName("fo2");
    foo.setLocalDate(LocalDate.now());
    foo.update();

    Thread.sleep(200);

    one2 = Ebean.find(EFoo.class)
      .where().like("name", "fo%")
      .setUseQueryCache(true)
      .findOne();

    System.out.println("one2 "+one2);

    one2 = Ebean.find(EFoo.class)
      .where().like("name", "fo%")
      .setUseQueryCache(true)
      .findOne();

    System.out.println("one2 "+one2);

    System.out.println("done");
  }
}
