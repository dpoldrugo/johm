package redis.clients.johm;

import java.io.IOException;
import java.util.Random;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.Assert;
import org.junit.Before;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.embedded.RedisServer;

public class JOhmTestBase extends Assert {
    protected JedisPool jedisPool;
    protected volatile static boolean benchmarkMode;
    private RedisServer redisServer;
    private int redisPort;

    @Before
    public void startUp() throws IOException {
        redisPort = new Random(System.currentTimeMillis()).nextInt(50000);
        //startEmbeddedRedis();
        startJedisEngine();
    }

    protected void startEmbeddedRedis() throws IOException {
        redisServer = new RedisServer(redisPort);
        redisServer.start();
    }

    protected void startJedisEngine() {
        if (benchmarkMode) {
            jedisPool = new JedisPool(new GenericObjectPoolConfig(), "localhost",
                    redisPort, 2000);
        } else {
            jedisPool = new JedisPool(new GenericObjectPoolConfig(), "localhost", redisPort);
        }
        JOhm.setPool(jedisPool);
        purgeRedis();
    }

    protected void purgeRedis() {
        Jedis jedis = jedisPool.getResource();
        jedis.flushAll();
        jedisPool.returnResource(jedis);
    }
}