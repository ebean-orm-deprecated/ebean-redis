package io.ebean.redis;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Properties;

/**
 * Deployment configuration for redis.
 */
public class RedisConfig {

  private String server = "localhost";

  private int port = 6379;

  private int maxTotal = 200;

  private int maxIdle = 200;

  private int minIdle = 1;

  private long maxWaitMillis = -1L;

  private boolean blockWhenExhausted = true;

  /**
   * Return a new JedisPool based on the configuration.
   */
  public JedisPool createPool() {

    JedisPoolConfig poolConfig = new JedisPoolConfig();
    poolConfig.setMaxTotal(maxTotal);
    poolConfig.setMaxIdle(maxIdle);
    poolConfig.setMinIdle(minIdle);
    poolConfig.setMaxWaitMillis(maxWaitMillis);
    poolConfig.setBlockWhenExhausted(blockWhenExhausted);

    return new JedisPool(poolConfig, server, port);
  }

  public String getServer() {
    return server;
  }

  public void setServer(String server) {
    this.server = server;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public int getMaxTotal() {
    return maxTotal;
  }

  public void setMaxTotal(int maxTotal) {
    this.maxTotal = maxTotal;
  }

  public int getMaxIdle() {
    return maxIdle;
  }

  public void setMaxIdle(int maxIdle) {
    this.maxIdle = maxIdle;
  }

  public int getMinIdle() {
    return minIdle;
  }

  public void setMinIdle(int minIdle) {
    this.minIdle = minIdle;
  }

  public long getMaxWaitMillis() {
    return maxWaitMillis;
  }

  public void setMaxWaitMillis(long maxWaitMillis) {
    this.maxWaitMillis = maxWaitMillis;
  }

  public boolean isBlockWhenExhausted() {
    return blockWhenExhausted;
  }

  public void setBlockWhenExhausted(boolean blockWhenExhausted) {
    this.blockWhenExhausted = blockWhenExhausted;
  }

  public void loadProperties(Properties properties) {
    this.server = properties.getProperty("ebean.redis.server", server);
    this.port = getInt(properties, "ebean.redis.port", port);
    this.minIdle = getInt(properties, "ebean.redis.minIdle", minIdle);
    this.maxIdle = getInt(properties, "ebean.redis.maxIdle", maxIdle);
    this.maxTotal = getInt(properties, "ebean.redis.maxTotal", maxTotal);
    this.maxWaitMillis = getLong(properties, "ebean.redis.maxWaitMillis", maxWaitMillis);
    this.blockWhenExhausted = getBool(properties, "ebean.redis.blockWhenExhausted", blockWhenExhausted);
  }

  private int getInt(Properties properties, String key, int defaulValue) {
    String val = properties.getProperty(key);
    if (val != null) {
      return Integer.parseInt(val.trim());
    }
    return defaulValue;
  }

  private long getLong(Properties properties, String key, long defaultValue) {
    String val = properties.getProperty(key);
    if (val != null) {
      return Long.parseLong(val.trim());
    }
    return defaultValue;
  }

  private boolean getBool(Properties properties, String key, boolean defaulValue) {
    String val = properties.getProperty(key);
    if (val != null) {
      return Boolean.parseBoolean(val.trim());
    }
    return defaulValue;
  }
}
