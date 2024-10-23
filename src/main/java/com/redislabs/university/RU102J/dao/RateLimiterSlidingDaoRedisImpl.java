package com.redislabs.university.RU102J.dao;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import java.time.ZonedDateTime;

public class RateLimiterSlidingDaoRedisImpl implements RateLimiter {

    private final JedisPool jedisPool;
    private final long windowSizeMS;
    private final long maxHits;

    public RateLimiterSlidingDaoRedisImpl(JedisPool pool, long windowSizeMS,
                                          long maxHits) {
        this.jedisPool = pool;
        this.windowSizeMS = windowSizeMS;
        this.maxHits = maxHits;
    }

    // Challenge #7
    @Override
    public void hit(String name) throws RateLimitExceededException {
        // START CHALLENGE #7
        try (Jedis jedis = jedisPool.getResource()) {
            String key = getKey(name);
            long currentTimeStamp = ZonedDateTime.now().toInstant().toEpochMilli();

            Transaction transaction = jedis.multi();
            String member = currentTimeStamp + "-" + Math.random();
            transaction.zadd(key, currentTimeStamp, member);

            long windowStart = currentTimeStamp - windowSizeMS;
            transaction.zremrangeByScore(key, 0, windowStart);

            Response<Long> hitCount = transaction.zcard(key);

            transaction.exec();

            if (hitCount.get() > maxHits) {
                throw new RateLimitExceededException();
            }
        }
        // END CHALLENGE #7
    }

    private String getKey(String name) {
        return RedisSchema.getSlidingWindowLimiterKey(windowSizeMS, name, maxHits);
    }
}
