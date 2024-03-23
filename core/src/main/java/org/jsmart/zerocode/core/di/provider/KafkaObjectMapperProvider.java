package org.jsmart.zerocode.core.di.provider;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import jakarta.inject.Provider;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class KafkaObjectMapperProvider implements Provider<ObjectMapper> {

    @Override
    public ObjectMapper get() {

        // get default objectMapper and customize it for Kafka afterwards
        ObjectMapper objectMapper = new ObjectMapperProvider().get();
        objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY); // serialize all instance vars
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL); // ignore null values like Gson

        SimpleModule module = new SimpleModule();
        module.addSerializer(RecordHeaders.class, new RecordHeadersSerializer());
        module.addDeserializer(Headers.class, new HeadersDeserializer());
        module.setMixInAnnotation(ProducerRecord.class, ProducerRecordBuilder.class);
        objectMapper.registerModule(module);

        return objectMapper;
    }


    @JsonDeserialize(builder = ProducerRecordBuilder.class)
    public static class ProducerRecordBuilder<K, V> {
        private String topic;
        private Integer partition;
        private Headers headers;
        private K key;
        private V value;
        private Long timestamp;

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public void setPartition(Integer partition) {
            this.partition = partition;
        }

        public void setHeaders(Headers headers) {
            this.headers = headers;
        }

        public void setKey(K key) {
            this.key = key;
        }

        public void setValue(V value) {
            this.value = value;
        }

        public void setTimestamp(Long timestamp) {
            this.timestamp = timestamp;
        }

        public ProducerRecord<K, V> build() {
            String recordTopic = this.topic != null ? this.topic : "";
            return new ProducerRecord(recordTopic, this.partition, this.timestamp,
                    this.key, this.value, this.headers);
        }
    }


    public static class RecordHeadersSerializer extends StdSerializer<RecordHeaders> {

        public RecordHeadersSerializer() {
            this(null);
        }

        public RecordHeadersSerializer(Class<RecordHeaders> t) {
            super(t);
        }

        @Override
        public void serialize(RecordHeaders headers, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException {

            Map<String, String> headerMap = new HashMap<>();
            for (Header header : headers) {
                headerMap.put(header.key(), new String(header.value(), StandardCharsets.UTF_8));
            }
            jsonGenerator.writeObject(headerMap);
        }
    }

    private static class HeadersDeserializer extends JsonDeserializer<Headers> {
        @Override
        public Headers deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
            switch (jsonParser.currentToken()) {
                case VALUE_NULL:
                    return null;
                case VALUE_EMBEDDED_OBJECT:
                    final Object embeddedObject = jsonParser.getEmbeddedObject();
                    if (embeddedObject instanceof Headers) {
                        return (Headers) embeddedObject;
                    }
                    return null;
                default:
                    return null;
            }
        }
    }
}
