package io.ebean.redis;

import io.ebean.cache.ServerCache;
import io.ebean.cache.ServerCacheStatistics;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DuelCache implements ServerCache, NearCacheInvalidate {

  private final ServerCache near;
  private final ServerCache remote;
  private final NearCacheNotify cacheNotify;
  private final String cacheKey;

  public DuelCache(ServerCache near, ServerCache remote, String cacheKey, NearCacheNotify cacheNotify) {
    this.near = near;
    this.remote = remote;
    this.cacheKey = cacheKey;
    this.cacheNotify = cacheNotify;
  }

  @Override
  public void invalidateKeys(Set<Object> keySet) {
    near.removeAll(keySet);
  }

  @Override
  public void invalidateKey(Object id) {
    near.remove(id);
  }

  @Override
  public void invalidateClear() {
    near.clear();
  }

  @Override
  public Map<Object, Object> getAll(Set<Object> keys) {

    Map<Object, Object> resultMap = near.getAll(keys);

    Set<Object> localKeys = resultMap.keySet();

    Set<Object> remainingKeys = new HashSet<>();
    for (Object key : keys) {
      if (!localKeys.contains(key)) {
        remainingKeys.add(key);
      }
    }
    if (!remainingKeys.isEmpty()) {
      // fetch missing ones from remote cache and merge results
      Map<Object, Object> remoteMap = remote.getAll(remainingKeys);
      if (!remoteMap.isEmpty()) {
        near.putAll(remoteMap);
        resultMap.putAll(remoteMap);
      }
    }

    return resultMap;
  }

  @Override
  public Object get(Object id) {
    Object val = near.get(id);
    if (val != null) {
      return val;
    }
    Object remoteVal = remote.get(id);
    if (remoteVal != null) {
      near.put(id, remoteVal);
    }
    return remoteVal;
  }

  @Override
  public void putAll(Map<Object, Object> keyValues) {
    near.putAll(keyValues);
    remote.putAll(keyValues);
    cacheNotify.invalidateKeys(cacheKey, keyValues.keySet());
  }

  @Override
  public void put(Object id, Object value) {

    near.put(id, value);
    remote.put(id, value);
    cacheNotify.invalidateKey(cacheKey, id);
  }

  @Override
  public void removeAll(Set<Object> keys) {

    near.removeAll(keys);
    remote.removeAll(keys);
    cacheNotify.invalidateKeys(cacheKey, keys);
  }

  @Override
  public void remove(Object id) {
    near.remove(id);
    remote.remove(id);
    cacheNotify.invalidateKey(cacheKey, id);
  }

  @Override
  public void clear() {
    near.clear();
    remote.clear();
    cacheNotify.invalidateClear(cacheKey);
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
