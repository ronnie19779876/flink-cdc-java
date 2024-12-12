package org.jdkxx.cdc.common.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

@Slf4j
public class JsonDeserializer<T> {
    private final ObjectMapper mapper;

    JsonDeserializer() {
        this.mapper = new ObjectMapper();
        // 在反序列化时忽略在 json 中存在但 Java 对象不存在的属性
        this.mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        // 在序列化时日期格式默认为 yyyy-MM-dd'T'HH:mm:ss.SSSZ
        this.mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        // 在序列化时忽略值为 null 的属性
        // mapper.setSerializationInclusion(Include.NON_NULL);
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public T deserialize(String json, Class<T> cls) {
        try {
            return mapper.readValue(json, cls);
        } catch (JsonProcessingException ex) {
            log.error("json parse error", ex);
        }
        return null;
    }

    public T deserialize(byte[] bytes, Class<T> cls) {
        return deserialize(StringUtils.toEncodedString(bytes, StandardCharsets.UTF_8), cls);
    }

    public T deserialize(InputStream is, Class<T> cls) {
        try {
            return deserialize(IOUtils.toByteArray(is), cls);
        } catch (IOException ex) {
            log.error("json parse error", ex);
        }
        return null;
    }

    public List<T> deserializeToList(String json, Class<T> cls) {
        JavaType javaType = mapper.getTypeFactory().constructParametricType(List.class, cls);
        try {
            return mapper.readValue(json, javaType);
        } catch (JsonProcessingException ex) {
            log.error("json parse error", ex);
        }
        return null;
    }

    public List<T> deserializeToList(InputStream is, Class<T> cls) {
        JavaType javaType = mapper.getTypeFactory().constructParametricType(List.class, cls);
        try {
            return mapper.readValue(is, javaType);
        } catch (IOException ex) {
            log.error("json parse error", ex);
        }
        return null;
    }

    public static <T> Builder<T> newBuilder() {
        return new Builder<>();
    }

    public static class Builder<T> {
        Builder() {
        }

        public JsonDeserializer<T> build() {
            return new JsonDeserializer<>();
        }
    }
}
