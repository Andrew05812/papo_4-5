package com.example;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DeletingCdcHandler<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger log = LoggerFactory.getLogger(DeletingCdcHandler.class);
    private JedisPool jedisPool;
    private final Map<String, String> idToRedisKey = new ConcurrentHashMap<>();
    private String pkField;

    private static String getStr(Map<String, ?> configs, String key, String def) {
        Object v = configs.get(key);
        return (v != null) ? v.toString() : def;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        pkField = getStr(configs, "pk.field", "id");
        String redisHost = getStr(configs, "redis.host", "redis");
        int redisPort = Integer.parseInt(getStr(configs, "redis.port", "6379"));
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(10);
        poolConfig.setMaxIdle(5);
        poolConfig.setMinIdle(1);
        jedisPool = new JedisPool(poolConfig, redisHost, redisPort, 2000);
        log.info("DeletingCdcHandler initialized (Redis at {}:{}, pkField={})", redisHost, redisPort, pkField);
    }

    private String prefixFromTopic(String topic) {
        int dot = topic.lastIndexOf('.');
        return (dot >= 0 ? topic.substring(dot + 1) : topic) + ":";
    }

    @Override
    public R apply(R record) {
        String prefix = prefixFromTopic(record.topic());
        if (record.value() == null) {
            String id = extractIdFromKey(record.key());
            if (id != null) {
                String redisKey = idToRedisKey.remove(id);
                if (redisKey != null) {
                    try (Jedis jedis = jedisPool.getResource()) {
                        jedis.del(redisKey);
                        log.info("Deleted key '{}' from Redis", redisKey);
                    } catch (Exception e) {
                        log.error("Failed to delete key '{}' from Redis", redisKey, e);
                    }
                }
            }
            return null;
        }
        String pkValue = extractField(record.value(), pkField);
        String id = extractField(record.value(), "id");
        if (id != null && pkValue != null) {
            idToRedisKey.put(id, prefix + pkValue);
        }
        return record;
    }

    private String extractIdFromKey(Object key) {
        if (key == null) return null;
        if (key instanceof Struct) {
            try { return ((Struct) key).getString("id"); } catch (Exception e) { return null; }
        }
        return extractJsonField(key.toString(), "id");
    }

    private String extractField(Object value, String fieldName) {
        if (value == null) return null;
        if (value instanceof Struct) {
            try { return ((Struct) value).getString(fieldName); } catch (Exception e) { return null; }
        }
        return extractJsonField(value.toString(), fieldName);
    }

    private String extractJsonField(String json, String fieldName) {
        String search = "\"" + fieldName + "\":\"";
        int idx = json.indexOf(search);
        if (idx != -1) {
            int start = idx + search.length();
            int end = json.indexOf('"', start);
            if (end != -1) return json.substring(start, end);
        }
        return null;
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef()
                .define("pk.field", ConfigDef.Type.STRING, "id", ConfigDef.Importance.MEDIUM, "PK field for Redis key suffix")
                .define("redis.host", ConfigDef.Type.STRING, "redis", ConfigDef.Importance.HIGH, "Redis host")
                .define("redis.port", ConfigDef.Type.STRING, "6379", ConfigDef.Importance.MEDIUM, "Redis port");
    }

    @Override
    public void close() {
        if (jedisPool != null) jedisPool.close();
    }
}
