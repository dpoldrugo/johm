package redis.clients.johm;

import java.io.IOException;

import org.apache.commons.pool.impl.GenericObjectPool.Config;
import org.junit.Assert;
import org.junit.Before;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Protocol;
import redis.embedded.RedisServer;

public class JOhmTestBase extends Assert {
    protected JedisPool jedisPool;
    protected volatile static boolean benchmarkMode;
    private RedisServer redisServer;

    @Before
    public void startUp() throws IOException {
        startEmbeddedRedis();
        startJedisEngine();
    }

    protected void startEmbeddedRedis() throws IOException {
        redisServer = new RedisServer(Protocol.DEFAULT_PORT);
        redisServer.start();
    }

    protected void startJedisEngine() {
        if (benchmarkMode) {
            jedisPool = new JedisPool(new Config(), "localhost",
                    Protocol.DEFAULT_PORT, 2000);
        } else {
            jedisPool = new JedisPool(new Config(), "localhost");
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