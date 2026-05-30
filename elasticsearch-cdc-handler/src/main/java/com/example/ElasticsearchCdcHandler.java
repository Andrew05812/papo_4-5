package com.example;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Base64;
import java.util.Map;

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
        if (record.value() == null) {
            String topic = record.topic();
            String indexName = extractIndexName(topic);
            String docId = extractIdFromKey(record.key());
            if (indexName != null && docId != null) {
                deleteFromElasticsearch(indexName, docId);
                log.info("Deleted doc '{}' from index '{}' in Elasticsearch", docId, indexName);
            }
            return null;
        }
        return record;
    }

    private String extractIndexName(String topic) {
        return topic;
    }

    private String extractIdFromKey(Object key) {
        if (key == null) return null;
        if (key instanceof Struct) {
            try { return ((Struct) key).getString("id"); } catch (Exception e) { return null; }
        }
        return extractJsonField(key.toString(), "id");
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
                log.warn("Elasticsearch DELETE for {}/{} returned {}", index, id, code);
            }
            conn.disconnect();
        } catch (Exception e) {
            log.error("Failed to delete {}/{} from Elasticsearch", index, id, e);
        }
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
                .define("es.host", ConfigDef.Type.STRING, "elasticsearch", ConfigDef.Importance.HIGH, "Elasticsearch host")
                .define("es.port", ConfigDef.Type.STRING, "9200", ConfigDef.Importance.MEDIUM, "Elasticsearch port")
                .define("es.user", ConfigDef.Type.STRING, "elastic", ConfigDef.Importance.MEDIUM, "Elasticsearch user")
                .define("es.pass", ConfigDef.Type.STRING, "elastic_pass123", ConfigDef.Importance.MEDIUM, "Elasticsearch password");
    }

    @Override
    public void close() {}
}
