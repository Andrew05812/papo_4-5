package com.example;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.HashMap;
import java.util.Map;

public class RedisHashCdcHandler<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger log = LoggerFactory.getLogger(RedisHashCdcHandler.class);
    private JedisPool jedisPool;
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
        log.info("RedisHashCdcHandler initialized (Redis at {}:{}, pkField={})", redisHost, redisPort, pkField);
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
                String redisKey = prefix + id;
                try (Jedis jedis = jedisPool.getResource()) {
                    jedis.del(redisKey);
                    log.info("DEL key '{}' from Redis", redisKey);
                } catch (Exception e) {
                    log.error("Failed to DEL key '{}' from Redis", redisKey, e);
                }
            }
            return null;
        }
        Map<String, String> fieldMap = extractFields(record.value());
        String pkValue = fieldMap.get(pkField);
        if (pkValue == null) {
            pkValue = extractIdFromKey(record.key());
        }
        if (pkValue != null && !fieldMap.isEmpty()) {
            String redisKey = prefix + pkValue;
            try (Jedis jedis = jedisPool.getResource()) {
                jedis.hset(redisKey, fieldMap);
                log.info("HSET key '{}' with {} fields", redisKey, fieldMap.size());
            } catch (Exception e) {
                log.error("Failed to HSET key '{}' to Redis", redisKey, e);
            }
        }
        return null;
    }

    private Map<String, String> extractFields(Object value) {
        Map<String, String> result = new HashMap<>();
        if (value == null) return result;
        if (value instanceof Struct) {
            Struct struct = (Struct) value;
            for (Field field : struct.schema().fields()) {
                Object v = struct.get(field);
                if (v != null) {
                    result.put(field.name(), v.toString());
                }
            }
        } else {
            extractJsonFields(value.toString(), result);
        }
        return result;
    }

    private void extractJsonFields(String json, Map<String, String> result) {
        json = json.trim();
        if (json.startsWith("{")) {
            int depth = 0;
            int start = -1;
            String key = null;
            for (int i = 0; i < json.length(); i++) {
                char c = json.charAt(i);
                if (c == '{' || c == '[') depth++;
                else if (c == '}' || c == ']') depth--;
                if (depth == 1) {
                    if (c == '"' && start == -1) {
                        start = i + 1;
                    } else if (c == '"' && start != -1 && key == null) {
                        key = json.substring(start, i);
                        start = -1;
                    } else if (c == ':' && key != null) {
                        start = i + 1;
                    } else if ((c == ',' || c == '}') && key != null && start != -1) {
                        String val = json.substring(start, i).trim();
                        if (val.startsWith("\"") && val.endsWith("\"") && val.length() >= 2) {
                            val = val.substring(1, val.length() - 1);
                        }
                        if (!val.equals("null") && !val.startsWith("{") && !val.startsWith("[")) {
                            result.put(key, val);
                        }
                        key = null;
                        start = -1;
                    }
                }
            }
        }
    }

    private String extractIdFromKey(Object key) {
        if (key == null) return null;
        if (key instanceof Struct) {
            try { return ((Struct) key).getString("id"); } catch (Exception e) { return null; }
        }
        return extractJsonField(key.toString(), "id");
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
