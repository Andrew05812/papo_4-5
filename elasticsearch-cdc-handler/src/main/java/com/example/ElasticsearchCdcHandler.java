package com.example;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.List;

public class ElasticsearchCdcHandler<R extends ConnectRecord<R>> implements Transformation<R> {

    private static final Logger log = LoggerFactory.getLogger(ElasticsearchCdcHandler.class);
    private String esHost;
    private int esPort;
    private String esUser;
    private String esPass;

    private static String getStr(Map<String, ?> configs, String key, String def) {
        Object v = configs.get(key);
        return (v != null) ? v.toString() : def;
    }

    @Override
    public void configure(Map<String, ?> configs) {
        esHost = getStr(configs, "es.host", "elasticsearch");
        esPort = Integer.parseInt(getStr(configs, "es.port", "9200"));
        esUser = getStr(configs, "es.user", "elastic");
        esPass = getStr(configs, "es.pass", "elastic_pass123");
        log.info("ElasticsearchCdcHandler initialized (ES at {}:{})", esHost, esPort);
    }

    @Override
    public R apply(R record) {
        String topic = record.topic();
        String indexName = extractIndexName(topic);
        String docId = extractIdFromKey(record.key());

        if (record.value() == null) {
            if (indexName != null && docId != null) {
                deleteFromElasticsearch(indexName, docId);
                log.info("ES DELETE: {}/{}", indexName, docId);
            }
            return null;
        }

        if (indexName != null && docId != null) {
            Map<String, Object> doc = extractDocument(record.value());
            if (doc != null && !doc.isEmpty()) {
                upsertToElasticsearch(indexName, docId, doc);
                log.info("ES UPSERT: {}/{}", indexName, docId);
            }
        }
        return null;
    }

    private Map<String, Object> extractDocument(Object value) {
        if (value instanceof Struct) {
            Struct struct = (Struct) value;
            Map<String, Object> doc = new LinkedHashMap<>();
            for (Field field : (List<Field>) struct.schema().fields()) {
                Object v = struct.get(field);
                if (v instanceof Struct) {
                    Map<String, Object> nested = new LinkedHashMap<>();
                    for (Field nf : (List<Field>) ((Struct) v).schema().fields()) {
                        nested.put(nf.name(), ((Struct) v).get(nf));
                    }
                    doc.put(field.name(), nested);
                } else {
                    doc.put(field.name(), v);
                }
            }
            return doc;
        }
        if (value instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) value;
            Map<String, Object> doc = new LinkedHashMap<>();
            for (Map.Entry<String, Object> e : map.entrySet()) {
                if (!"schema".equals(e.getKey()) && !"payload".equals(e.getKey())) {
                    doc.put(e.getKey(), e.getValue());
                }
            }
            Object payload = map.get("payload");
            if (payload instanceof Map) {
                @SuppressWarnings("unchecked")
                Map<String, Object> p = (Map<String, Object>) payload;
                doc.clear();
                doc.putAll(p);
            }
            return doc;
        }
        return null;
    }

    private String extractIndexName(String topic) {
        return topic;
    }

    private String extractIdFromKey(Object key) {
        if (key == null) return null;
        if (key instanceof Struct) {
            try { return ((Struct) key).getString("id"); } catch (Exception e) { return null; }
        }
        if (key instanceof Map) {
            Object id = ((Map) key).get("id");
            if (id instanceof Map) {
                Object payload = ((Map) id).get("payload");
                if (payload != null) return payload.toString();
            }
            if (id != null) return id.toString();
        }
        return extractJsonField(key.toString(), "id");
    }

    private void upsertToElasticsearch(String index, String id, Map<String, Object> doc) {
        try {
            URL url = new URL("http://" + esHost + ":" + esPort + "/" + index + "/_doc/" + id);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("PUT");
            conn.setDoOutput(true);
            String auth = esUser + ":" + esPass;
            conn.setRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString(auth.getBytes()));
            conn.setRequestProperty("Content-Type", "application/json");
            String json = mapToJson(doc);
            conn.getOutputStream().write(json.getBytes("UTF-8"));
            int code = conn.getResponseCode();
            if (code != 200 && code != 201) {
                log.warn("ES UPSERT for {}/{} returned {}", index, id, code);
            }
            conn.disconnect();
        } catch (Exception e) {
            log.error("Failed to upsert {}/{} to Elasticsearch", index, id, e);
        }
    }

    private void deleteFromElasticsearch(String index, String id) {
        try {
            URL url = new URL("http://" + esHost + ":" + esPort + "/" + index + "/_doc/" + id);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("DELETE");
            String auth = esUser + ":" + esPass;
            conn.setRequestProperty("Authorization", "Basic " + Base64.getEncoder().encodeToString(auth.getBytes()));
            int code = conn.getResponseCode();
            if (code != 200 && code != 404 && code != 204) {
                log.warn("ES DELETE for {}/{} returned {}", index, id, code);
            }
            conn.disconnect();
        } catch (Exception e) {
            log.error("Failed to delete {}/{} from Elasticsearch", index, id, e);
        }
    }

    private String mapToJson(Map<String, Object> map) {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (Map.Entry<String, Object> e : map.entrySet()) {
            if (!first) sb.append(",");
            first = false;
            sb.append("\"").append(e.getKey()).append("\":");
            Object v = e.getValue();
            if (v == null) {
                sb.append("null");
            } else if (v instanceof Number) {
                sb.append(v);
            } else if (v instanceof Boolean) {
                sb.append(v);
            } else if (v instanceof Map) {
                sb.append(mapToJson((Map<String, Object>) v));
            } else {
                sb.append("\"").append(v.toString().replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n")).append("\"");
            }
        }
        sb.append("}");
        return sb.toString();
    }

    private String extractJsonField(String json, String fieldName) {
        String search = "\"" + fieldName + "\":\"";
        int idx = json.indexOf(search);
        if (idx != -1) {
            int start = idx + search.length();
            int end = json.indexOf('"', start);
            if (end != -1) return json.substring(start, end);
        }
        search = "\"" + fieldName + "\":{\"id\":\"";
        idx = json.indexOf(search);
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
                .define("es.host", ConfigDef.Type.STRING, "elasticsearch", ConfigDef.Importance.HIGH, "Elasticsearch host")
                .define("es.port", ConfigDef.Type.STRING, "9200", ConfigDef.Importance.MEDIUM, "Elasticsearch port")
                .define("es.user", ConfigDef.Type.STRING, "elastic", ConfigDef.Importance.MEDIUM, "Elasticsearch user")
                .define("es.pass", ConfigDef.Type.STRING, "elastic_pass123", ConfigDef.Importance.MEDIUM, "Elasticsearch password");
    }

    @Override
    public void close() {}
}
