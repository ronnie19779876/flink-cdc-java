package org.jdkxx.cdc.common.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;

@Slf4j
public class JsonSerializer {
    private final ObjectMapper mapper;

    JsonSerializer() {
        this.mapper = new ObjectMapper();
        // 在反序列化时忽略在 json 中存在但 Java 对象不存在的属性
        this.mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        // 在序列化时日期格式默认为 yyyy-MM-dd'T'HH:mm:ss.SSSZ
        this.mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        // 在序列化时忽略值为 null 的属性
        // mapper.setSerializationInclusion(Include.NON_NULL);
    }

    public String serialize(Object object) {
        try {
            return mapper.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            log.error("json parse error", e);
        }
        return null;
    }

    public byte[] serializeToBytes(Object object) {
        String json = serialize(object);
        if (json != null) {
            return json.getBytes(StandardCharsets.UTF_8);
        }
        return null;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    public static class Builder {
        public JsonSerializer build() {
            return new JsonSerializer();
        }
    }
}
