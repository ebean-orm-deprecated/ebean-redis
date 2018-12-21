package io.ebean.redis;

import io.ebean.cache.ServerCache;
import io.ebean.cache.ServerCacheStatistics;
import io.ebean.meta.MetricType;
import io.ebean.metric.MetricFactory;
import io.ebean.metric.TimedMetric;
import io.ebean.redis.encode.Encode;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;
import redis.clients.jedis.util.SafeEncoder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

class RedisCache implements ServerCache {

  private static final String CURSOR_0 = "0";
  private static final byte[] CURSOR_0_BYTES = SafeEncoder.encode(CURSOR_0);

  private final JedisPool jedisPool;
  private final String cacheKey;
  private final Encode keyEncode;
  private final Encode valueEncode;

  private final TimedMetric metricGet;
  private final TimedMetric metricGetAll;
  private final TimedMetric metricPut;
  private final TimedMetric metricPutAll;
  private final TimedMetric metricRemove;
  private final TimedMetric metricRemoveAll;
  private final TimedMetric metricClear;

  RedisCache(JedisPool jedisPool, String cacheKey, Encode keyEncode, Encode valueEncode) {
    this.jedisPool = jedisPool;
    this.cacheKey = cacheKey;
    this.keyEncode = keyEncode;
    this.valueEncode = valueEncode;

    String prefix = "ebean.l2cache.";

    MetricFactory metricFactory = MetricFactory.get();

    metricGet = metricFactory.createTimedMetric(MetricType.L2, prefix + "get." + cacheKey);
    metricGetAll = metricFactory.createTimedMetric(MetricType.L2, prefix + "getMany." + cacheKey);
    metricPut = metricFactory.createTimedMetric(MetricType.L2, prefix + "put." + cacheKey);
    metricPutAll = metricFactory.createTimedMetric(MetricType.L2, prefix + "putMany." + cacheKey);
    metricRemove = metricFactory.createTimedMetric(MetricType.L2, prefix + "remove." + cacheKey);
    metricRemoveAll = metricFactory.createTimedMetric(MetricType.L2, prefix + "removeMany." + cacheKey);
    metricClear = metricFactory.createTimedMetric(MetricType.L2, prefix + "clear." + cacheKey);
  }

  private byte[] key(Object id) {
    return keyEncode.encode(id);
  }

  private byte[] value(Object data) {
    if (data == null) {
      return null;
    }
    return valueEncode.encode(data);
  }

  private Object valueDecode(byte[] data) {
    if (data == null) {
      return null;
    }
    return valueEncode.decode(data);
  }

  @Override
  public Map<Object, Object> getAll(Set<Object> keys) {

    long start = System.nanoTime();
    Map<Object, Object> map = new LinkedHashMap<>();

    List<Object> keyList = new ArrayList<>(keys);
    try (Jedis resource = jedisPool.getResource()) {
      List<byte[]> valsAsBytes = resource.mget(keysAsBytes(keyList));
      for (int i = 0; i < keyList.size(); i++) {
        map.put(keyList.get(i), valueDecode(valsAsBytes.get(i)));
      }
      metricGetAll.addSinceNanos(start, keyList.size());
      return map;
    }
  }

  @Override
  public Object get(Object id) {
    long start = System.nanoTime();
    try (Jedis resource = jedisPool.getResource()) {
      Object val = valueDecode(resource.get(key(id)));
      metricGet.addSinceNanos(start);
      return val;
    }
  }


  @Override
  public void put(Object id, Object value) {
    long start = System.nanoTime();
    try (Jedis resource = jedisPool.getResource()) {
      resource.set(key(id), value(value));
      metricPut.addSinceNanos(start);
    }
  }

  @Override
  public void putAll(Map<Object, Object> keyValues) {
    long start = System.nanoTime();
    try (Jedis resource = jedisPool.getResource()) {
      byte[][] raw = new byte[keyValues.size() * 2][];
      int pos = 0;
      for (Map.Entry<Object, Object> entry : keyValues.entrySet()) {
        raw[pos++] = key(entry.getKey());
        raw[pos++] = value(entry.getValue());
      }
      resource.mset(raw);
      metricPutAll.addSinceNanos(start, keyValues.size());
    }
  }

  @Override
  public void remove(Object id) {
    long start = System.nanoTime();
    try (Jedis resource = jedisPool.getResource()) {
      resource.del(key(id));
      metricRemove.addSinceNanos(start);
    }
  }

  @Override
  public void removeAll(Set<Object> keys) {
    long start = System.nanoTime();
    try (Jedis resource = jedisPool.getResource()) {
      resource.del(keysAsBytes(keys));
      metricRemoveAll.addSinceNanos(start, keys.size());
    }
  }

  private byte[][] keysAsBytes(Collection<Object> keys) {
    byte[][] raw = new byte[keys.size()][];
    int pos = 0;
    for (Object id : keys) {
      raw[pos++] = key(id);
    }
    return raw;
  }

  @Override
  public void clear() {
    long start = System.nanoTime();
    try (Jedis resource = jedisPool.getResource()) {

      ScanParams params = new ScanParams();
      params.match(cacheKey + ":*");

      String next;
      byte[] nextCursor = CURSOR_0_BYTES;
      do {
        ScanResult<byte[]> scanResult = resource.scan(nextCursor, params);
        List<byte[]> keys = scanResult.getResult();
        nextCursor = scanResult.getCursorAsBytes();

        if (!keys.isEmpty()) {
          byte[][] raw = new byte[keys.size()][];
          for (int i = 0; i < keys.size(); i++) {
            raw[i] = keys.get(i);
          }
          resource.del(raw);
        }

        next = SafeEncoder.encode(nextCursor);
      } while (!next.equals("0"));

      metricClear.addSinceNanos(start);
    }
  }

  @Override
  public int size() {
    return 0;
  }

  @Override
  public int getHitRatio() {
    return 0;
  }

  @Override
  public ServerCacheStatistics getStatistics(boolean reset) {
    return null;
  }
}
