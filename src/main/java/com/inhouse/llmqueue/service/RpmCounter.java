package com.inhouse.llmqueue.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class RpmCounter {

    private final StringRedisTemplate redis;

    private static final String KEY_PREFIX = "rpm:";
    private static final long WINDOW_SECONDS = 60;

    /**
     * Record one dispatch event for this model.
     * Uses a sorted set: score = timestamp in ms, member = unique id per event.
     * Old entries expire naturally via pruning in count().
     */
    public void record(String modelName) {
        String key = KEY_PREFIX + modelName;
        long now = System.currentTimeMillis();
        // member = timestamp:nanoTime ensures uniqueness even for same-ms events
        String member = now + ":" + System.nanoTime();
        redis.opsForZSet().add(key, member, now);
        redis.expire(key, WINDOW_SECONDS + 10, TimeUnit.SECONDS);
    }

    /**
     * Count dispatches in the last 60 seconds for this model.
     * Also prunes entries older than 60s to keep the set small.
     */
    public long count(String modelName) {
        String key = KEY_PREFIX + modelName;
        long now = System.currentTimeMillis();
        long windowStart = now - (WINDOW_SECONDS * 1000);

        // Prune entries older than 60s
        redis.opsForZSet().removeRangeByScore(key, 0, windowStart - 1);

        // Count entries within the window
        Long count = redis.opsForZSet().count(key, windowStart, now);
        return count != null ? count : 0L;
    }
}
