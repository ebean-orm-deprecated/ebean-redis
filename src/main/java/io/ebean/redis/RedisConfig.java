package io.ebean.redis;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Properties;

/**
 * Deployment configuration for redis.
 */
public class RedisConfig {

	private int redisPort = 6379;

	private String redisServer = "localhost";

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

		return new JedisPool(poolConfig, redisServer, redisPort);
	}


	public void setRedisPort(int redisPort) {
		this.redisPort = redisPort;
	}

	public String getRedisServer() {
		return redisServer;
	}

	public void setRedisServer(String redisServer) {
		this.redisServer = redisServer;
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

	public void loadProperties(Properties properties) {

	}
}
