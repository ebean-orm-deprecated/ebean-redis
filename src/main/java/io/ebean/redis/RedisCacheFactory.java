package io.ebean.redis;

import io.ebean.BackgroundExecutor;
import io.ebean.cache.ServerCache;
import io.ebean.cache.ServerCacheConfig;
import io.ebean.cache.ServerCacheFactory;
import io.ebean.cache.ServerCacheNotification;
import io.ebean.cache.ServerCacheNotify;
import io.ebean.config.ServerConfig;
import io.ebean.meta.MetricType;
import io.ebean.metric.MetricFactory;
import io.ebean.metric.TimedMetric;
import io.ebean.redis.encode.EncodeBeanData;
import io.ebean.redis.encode.EncodeManyIdsData;
import io.ebean.redis.encode.EncodePrefixKey;
import io.ebean.redis.encode.EncodeSerializable;
import io.ebean.redis.topic.DaemonTopic;
import io.ebean.redis.topic.DaemonTopicRunner;
import io.ebeaninternal.server.cache.DefaultServerCache;
import io.ebeaninternal.server.cache.DefaultServerCacheConfig;
import io.ebeaninternal.server.cache.DefaultServerQueryCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.util.SafeEncoder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

class RedisCacheFactory implements ServerCacheFactory {

  private static final Logger queryLogger = LoggerFactory.getLogger("io.ebean.cache.QUERY");

  private static final Logger logger = LoggerFactory.getLogger("io.ebean.cache.CACHE");

  private static final Logger tableModLogger = LoggerFactory.getLogger("io.ebean.cache.TABLEMODS");

  private static final int MSG_NEARCACHE_CLEAR = 1;
  private static final int MSG_NEARCACHE_KEYS = 2;
  private static final int MSG_NEARCACHE_KEY = 3;

  /**
   * Channel for standard L2 cache messages.
   */
  private static final String CHANNEL_L2 = "ebean.l2cache";

  /**
   * Channel specifically for near cache invalidation messages.
   */
  private static final String CHANNEL_NEAR = "ebean.l2near";

  private static final byte[] CHANNEL_L2_BYTES = SafeEncoder.encode(CHANNEL_L2);
  private static final byte[] CHANNEL_NEAR_BYTES = SafeEncoder.encode(CHANNEL_NEAR);

  private final ConcurrentHashMap<String, RQueryCache> queryCaches = new ConcurrentHashMap<>();

  private final Map<String, NearCacheInvalidate> nearCacheMap = new ConcurrentHashMap<>();

  private final EncodeManyIdsData encodeManyIdsData = new EncodeManyIdsData();
  private final EncodeBeanData encodeBeanData = new EncodeBeanData();
  private final EncodeSerializable encodeSerializable = new EncodeSerializable();

  private final BackgroundExecutor executor;

  private final JedisPool jedisPool;

  private final DaemonTopicRunner daemonTopicRunner;

  private final NearCacheNotify nearCacheNotify;

  private final TimedMetric metricMsgOut;
  private final TimedMetric metricMsgIn;

  private final String serverId = ModId.id();

  private ServerCacheNotify listener;


  RedisCacheFactory(ServerConfig serverConfig, BackgroundExecutor executor) {
    this.executor = executor;
    this.jedisPool = getJedisPool(serverConfig);
    this.nearCacheNotify = new DNearCacheNotify();
    this.metricMsgOut = MetricFactory.get().createTimedMetric(MetricType.L2, "ebean.l2cache.msgout");
    this.metricMsgIn = MetricFactory.get().createTimedMetric(MetricType.L2, "ebean.l2cache.msgin");

    this.daemonTopicRunner = new DaemonTopicRunner(jedisPool, new CacheDaemonTopic());
    daemonTopicRunner.run();
  }

  /**
   * Return the JedisPool to use (only 1 at this stage).
   */
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

    ServerCache redisCache = createRedisCache(config);

    boolean nearCache = config.getCacheOptions().isNearCache();
    if (!nearCache) {
      return redisCache;
    }

    String cacheKey = config.getCacheKey();

    DefaultServerCache near = new DefaultServerCache(new DefaultServerCacheConfig(config));
    DuelCache duelCache = new DuelCache(near, redisCache, cacheKey, nearCacheNotify);
    nearCacheMap.put(cacheKey, duelCache);

    return duelCache;
  }

  private ServerCache createRedisCache(ServerCacheConfig config) {

    String cacheKey = config.getCacheKey();
    EncodePrefixKey encodeKey = new EncodePrefixKey(cacheKey);

    switch (config.getType()) {
      case NATURAL_KEY:
        return new RedisCache(jedisPool, cacheKey, encodeKey, encodeSerializable);
      case BEAN:
        return new RedisCache(jedisPool, cacheKey, encodeKey, encodeBeanData);
      case COLLECTION_IDS:
        return new RedisCache(jedisPool, cacheKey, encodeKey, encodeManyIdsData);
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
      resource.publish(CHANNEL_L2, "queryCache:" + name);
    }
  }

  private void sendTableMod(String formattedMsg) {
    try (Jedis resource = jedisPool.getResource()) {
      resource.publish(CHANNEL_L2, "tableMod:" + formattedMsg);
    }
  }

  /**
   * Query cache implementation using Redis channel for message notifications.
   */
  private class RQueryCache extends DefaultServerQueryCache {

    RQueryCache(DefaultServerCacheConfig config) {
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

  /**
   * Publish table modifications using Redis channel (to other cluster members)
   */
  private class RServerCacheNotify implements ServerCacheNotify {

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

    Set<String> tables = new HashSet<>(Arrays.asList(split).subList(1, split.length));
    listener.notify(new ServerCacheNotification(modTimestamp, tables));
  }


  /**
   * Near cache notification using a specific Redis channel (CHANNEL_NEAR).
   */
  private class DNearCacheNotify implements NearCacheNotify {

    @Override
    public void invalidateKeys(String cacheKey, Set<Object> keySet) {
      try {
        sendMessage(messageInvalidateKeys(cacheKey, keySet));
      } catch (IOException e) {
        logger.error("failed to transmit invalidateKeys() message", e);
      }
    }

    @Override
    public void invalidateKey(String cacheKey, Object id) {
      try {
        sendMessage(messageInvalidateKey(cacheKey, id));
      } catch (IOException e) {
        logger.error("failed to transmit invalidateKeys() message", e);
      }
    }

    @Override
    public void invalidateClear(String cacheKey) {
      try {
        sendMessage(messageInvalidateClear(cacheKey));
      } catch (IOException e) {
        logger.error("failed to transmit invalidateKeys() message", e);
      }
    }

    private void sendMessage(byte[] message) {
      long nanos = System.nanoTime();
      try {
        try (Jedis resource = jedisPool.getResource()) {
          resource.publish(CHANNEL_NEAR_BYTES, message);
        }
      } finally {
        metricMsgOut.addSinceNanos(nanos);
      }
    }

    private byte[] messageInvalidateKeys(String cacheKey, Set<Object> keySet) throws IOException {
      ByteArrayOutputStream ba = new ByteArrayOutputStream(100);
      ObjectOutputStream os = new ObjectOutputStream(ba);
      os.writeUTF(serverId);
      os.writeInt(MSG_NEARCACHE_KEYS);
      os.writeUTF(cacheKey);
      os.writeInt(keySet.size());
      for (Object key : keySet) {
        os.writeObject(key);
      }
      os.flush();
      os.close();
      return ba.toByteArray();
    }

    private byte[] messageInvalidateKey(String cacheKey, Object id) throws IOException {
      ByteArrayOutputStream ba = new ByteArrayOutputStream(100);
      ObjectOutputStream os = new ObjectOutputStream(ba);
      os.writeUTF(serverId);
      os.writeInt(MSG_NEARCACHE_KEY);
      os.writeUTF(cacheKey);
      os.writeObject(id);
      os.flush();
      os.close();
      return ba.toByteArray();
    }

    private byte[] messageInvalidateClear(String cacheKey) throws IOException {
      ByteArrayOutputStream ba = new ByteArrayOutputStream(100);
      ObjectOutputStream os = new ObjectOutputStream(ba);
      os.writeUTF(serverId);
      os.writeInt(MSG_NEARCACHE_CLEAR);
      os.writeUTF(cacheKey);
      os.flush();
      os.close();
      return ba.toByteArray();
    }
  }

  /**
   * Redis channel listener that supports reconnection etc.
   */
  private class CacheDaemonTopic implements DaemonTopic {

    @Override
    public void subscribe(Jedis jedis) {
      jedis.subscribe(new ChannelSubscriber(), CHANNEL_L2_BYTES, CHANNEL_NEAR_BYTES);
    }

    @Override
    public void notifyConnected() {
      logger.info("Established connection to Redis");
    }

    /**
     * Handles updates to the features (via redis topic notifications).
     */
    private class ChannelSubscriber extends BinaryJedisPubSub {

      @Override
      public void onMessage(byte[] channel, byte[] message) {
        long nanos = System.nanoTime();
        try {
          String channelName = SafeEncoder.encode(channel);
          if (channelName.equals(CHANNEL_L2)) {
            processL2Message(SafeEncoder.encode(message));
          } else {
            processNearCacheMessage(message);
          }
        } finally {
          metricMsgIn.addSinceNanos(nanos);
        }
      }

      private void processNearCacheMessage(byte[] message) {

        String cacheKey = null;
        try {
          ObjectInputStream oi = new ObjectInputStream(new ByteArrayInputStream(message));
          String sourceServerId = oi.readUTF();
          int msgType = oi.readInt();
          cacheKey = oi.readUTF();
          if (sourceServerId.equals(serverId)) {
            // ignore this message as we are the server that sent it
            return;
          }
          if (logger.isDebugEnabled()) {
            logger.debug("processNearCacheMessage serverId:{} type:{} cacheKey:{}", sourceServerId, msgType, cacheKey);
          }

          switch (msgType) {
            case MSG_NEARCACHE_CLEAR:
              nearCacheInvalidateClear(cacheKey);
              break;

            case MSG_NEARCACHE_KEY:
              Object key = oi.readObject();
              nearCacheInvalidateKey(cacheKey, key);
              break;

            case MSG_NEARCACHE_KEYS:
              int count = oi.readInt();
              Set<Object> keys = new LinkedHashSet<>();
              for (int i = 0; i < count; i++) {
                keys.add(oi.readObject());
              }
              nearCacheInvalidateKeys(cacheKey, keys);
              break;

            default:
              throw new IllegalStateException("Unexpected message type ? " + msgType);
          }

        } catch (IOException | ClassNotFoundException e) {
          logger.error("failed to decode near cache message [" + SafeEncoder.encode(message) + "] for cache:" + cacheKey, e);
          if (cacheKey != null) {
            nearCacheInvalidateClear(cacheKey);
          }
        }
      }

      private void processL2Message(String message) {
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
              logger.error("Unknown L2 message type[{}] on redis channel - message[{}] ", split[0], message);
          }
        } catch (Exception e) {
          logger.error("Error handling L2 message[" + message + "]", e);
        }
      }
    }
  }

  /**
   * Invalidate key for a local near cache.
   */
  private void nearCacheInvalidateKey(String cacheKey, Object key) {

    NearCacheInvalidate invalidate = nearCacheMap.get(cacheKey);
    if (invalidate == null) {
      warnNearCacheNotFound(cacheKey);
    } else {
      invalidate.invalidateKey(key);
    }
  }

  /**
   * Invalidate keys for a local near cache.
   */
  private void nearCacheInvalidateKeys(String cacheKey, Set<Object> keys) {
    NearCacheInvalidate invalidate = nearCacheMap.get(cacheKey);
    if (invalidate == null) {
      warnNearCacheNotFound(cacheKey);
    } else {
      invalidate.invalidateKeys(keys);
    }
  }

  /**
   * Invalidate clear for a local near cache.
   */
  private void nearCacheInvalidateClear(String cacheKey) {
    NearCacheInvalidate invalidate = nearCacheMap.get(cacheKey);
    if (invalidate == null) {
      warnNearCacheNotFound(cacheKey);
    } else {
      invalidate.invalidateClear();
    }
  }

  private void warnNearCacheNotFound(String cacheKey) {
    logger.warn("No near cache found for cacheKey [" + cacheKey + "] yet - probably on startup");
  }

}
