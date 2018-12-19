package io.ebean.redis;

import io.ebean.BackgroundExecutor;
import io.ebean.cache.ServerCache;
import io.ebean.cache.ServerCacheConfig;
import io.ebean.cache.ServerCacheFactory;
import io.ebean.cache.ServerCacheNotification;
import io.ebean.cache.ServerCacheNotify;
import io.ebean.config.ServerConfig;
import io.ebean.redis.encode.EncodeBeanData;
import io.ebean.redis.encode.EncodeManyIdsData;
import io.ebean.redis.encode.EncodePrefixKey;
import io.ebean.redis.encode.EncodeSerializable;
import io.ebean.redis.topic.DaemonTopic;
import io.ebean.redis.topic.DaemonTopicRunner;
import io.ebeaninternal.server.cache.DefaultServerCacheConfig;
import io.ebeaninternal.server.cache.DefaultServerQueryCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPubSub;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

class RedisCacheFactory implements ServerCacheFactory {

  private static final Logger queryLogger = LoggerFactory.getLogger("org.avaje.ebean.cache.QUERY");

  private static final Logger logger = LoggerFactory.getLogger("org.avaje.ebean.cache.CACHE");

  private static final Logger tableModLogger = LoggerFactory.getLogger("io.ebean.cache.TABLEMODS");

  private static final String CHANNEL_NAME = "ebean.l2cache";

  private final ConcurrentHashMap<String, RQueryCache> queryCaches = new ConcurrentHashMap<>();

  private final EncodeManyIdsData encodeCollectionIdData = new EncodeManyIdsData();
  private final EncodeBeanData encodeBeanData = new EncodeBeanData();
  private final EncodeSerializable encodeSerializable = new EncodeSerializable();

  private final BackgroundExecutor executor;
  private final JedisPool jedisPool;

  private final DaemonTopicRunner daemonTopicRunner;

  private ServerCacheNotify listener;

  RedisCacheFactory(ServerConfig serverConfig, BackgroundExecutor executor) {
    this.executor = executor;
    this.jedisPool = getJedisPool(serverConfig);
    this.daemonTopicRunner = new DaemonTopicRunner(jedisPool, new CacheDaemonTopic());
    daemonTopicRunner.run();
  }

  private JedisPool getJedisPool(ServerConfig serverConfig) {
    JedisPool jedisPool = serverConfig.getServiceObject(JedisPool.class);
    if (jedisPool != null) {
      return jedisPool;
    }
    RedisConfig redisConfig = serverConfig.getServiceObject(RedisConfig.class);
    if (redisConfig == null) {
      redisConfig = new RedisConfig();
    }
    redisConfig.loadProperties(serverConfig.getProperties());
    return redisConfig.createPool();
  }

  @Override
  public ServerCache createCache(ServerCacheConfig config) {
    if (config.isQueryCache()) {
      return createQueryCache(config);
    }
    return createNormalCache(config);
  }

  private ServerCache createNormalCache(ServerCacheConfig config) {

    boolean nearCache = config.getCacheOptions().isNearCache();

    String cacheKey = config.getCacheKey();
    EncodePrefixKey encodeKey = new EncodePrefixKey(cacheKey);
    switch (config.getType()) {
      case NATURAL_KEY:
        return new RedisCache(jedisPool, cacheKey, encodeKey, encodeSerializable);
      case BEAN:
        return new RedisCache(jedisPool, cacheKey, encodeKey, encodeBeanData);
      case COLLECTION_IDS:
        return new RedisCache(jedisPool, cacheKey, encodeKey, encodeCollectionIdData);
      default:
        throw new IllegalArgumentException("Unexpected cache type? " + config.getType());
    }
  }

  private ServerCache createQueryCache(ServerCacheConfig config) {
    synchronized (this) {
      RQueryCache cache = queryCaches.get(config.getCacheKey());
      if (cache == null) {
        logger.debug("create query cache [{}]", config.getCacheKey());
        cache = new RQueryCache(new DefaultServerCacheConfig(config));
        cache.periodicTrim(executor);
        queryCaches.put(config.getCacheKey(), cache);
      }
      return cache;
    }
  }

  @Override
  public ServerCacheNotify createCacheNotify(ServerCacheNotify listener) {
    this.listener = listener;
    return new RServerCacheNotify();
  }

  private void sendQueryCacheInvalidation(String name) {
    try (Jedis resource = jedisPool.getResource()) {
      resource.publish(CHANNEL_NAME, "queryCache:" + name);
    }
  }

  private void sendTableMod(String formattedMsg) {
    try (Jedis resource = jedisPool.getResource()) {
      resource.publish(CHANNEL_NAME, "tableMod:" + formattedMsg);
    }
  }


  class RQueryCache extends DefaultServerQueryCache {

    public RQueryCache(DefaultServerCacheConfig config) {
      super(config);
    }

    @Override
    public void clear() {
      super.clear();
      sendQueryCacheInvalidation(name);
    }

    /**
     * Process the invalidation message coming from the cluster.
     */
    private void invalidate() {
      queryLogger.debug("   CLEAR {}(*) - cluster invalidate", name);
      super.clear();
    }
  }

  class RServerCacheNotify implements ServerCacheNotify {

    @Override
    public void notify(ServerCacheNotification tableModifications) {

      Set<String> dependentTables = tableModifications.getDependentTables();
      if (dependentTables != null && !dependentTables.isEmpty()) {

        StringBuilder msg = new StringBuilder(50);
        msg.append(tableModifications.getModifyTimestamp()).append(",");

        for (String table : dependentTables) {
          msg.append(table).append(",");
        }

        String formattedMsg = msg.toString();
        if (tableModLogger.isDebugEnabled()) {
          tableModLogger.debug("Publish TableMods - {}", formattedMsg);
        }
        sendTableMod(formattedMsg);
      }
    }
  }

  /**
   * Clear the query cache if we have it.
   */
  private void queryCacheInvalidate(String key) {
    RQueryCache queryCache = queryCaches.get(key);
    if (queryCache != null) {
      queryCache.invalidate();
    }
  }

  /**
   * Process a remote dependent table modify event.
   */
  private void processTableNotify(String rawMessage) {

    if (logger.isDebugEnabled()) {
      logger.debug("processTableNotify {}", rawMessage);
    }

    String[] split = rawMessage.split(",");
    long modTimestamp = Long.parseLong(split[0]);

    Set<String> tables = new HashSet<>();
    tables.addAll(Arrays.asList(split).subList(1, split.length));

    listener.notify(new ServerCacheNotification(modTimestamp, tables));
  }

  class CacheDaemonTopic implements DaemonTopic {

    @Override
    public void subscribe(Jedis jedis) {
      jedis.subscribe(new ChannelSubscriber(), CHANNEL_NAME);
    }

    @Override
    public void notifyConnected() {
      logger.info("Established connection to Redis");
    }

    /**
     * Handles updates to the features (via redis topic notifications).
     */
    private class ChannelSubscriber extends JedisPubSub {

      @Override
      public void onMessage(String channel, String message) {
        try {
          String[] split = message.split(":");
          switch (split[0]) {
            case "tableMod":
              processTableNotify(split[1]);
              break;
            case "queryCache":
              queryCacheInvalidate(split[1]);
              break;
            default:
              logger.debug("unknown message type on redis channel message[{}] ", message);
          }
        } catch (Exception e) {
          logger.error("Error handling message["+message+"]", e);
        }
      }

    }
  }

}
