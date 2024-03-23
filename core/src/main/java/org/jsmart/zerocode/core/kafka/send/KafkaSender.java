package org.jsmart.zerocode.core.kafka.send;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.jsmart.zerocode.core.engine.preprocessor.ScenarioExecutionState;
import org.jsmart.zerocode.core.engine.preprocessor.ZeroCodeAssertionsProcessorImpl;
import org.jsmart.zerocode.core.kafka.delivery.DeliveryDetails;
import org.jsmart.zerocode.core.kafka.send.message.ProducerJsonRecord;
import org.jsmart.zerocode.core.kafka.send.message.ProducerJsonRecords;
import org.jsmart.zerocode.core.kafka.send.message.ProducerRawRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.net.URL;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.jsmart.zerocode.core.constants.ZerocodeConstants.FAILED;
import static org.jsmart.zerocode.core.constants.ZerocodeConstants.OK;
import static org.jsmart.zerocode.core.kafka.KafkaConstants.JSON;
import static org.jsmart.zerocode.core.kafka.KafkaConstants.PROTO;
import static org.jsmart.zerocode.core.kafka.KafkaConstants.RAW;
import static org.jsmart.zerocode.core.kafka.KafkaConstants.RECORD_TYPE_JSON_PATH;
import static org.jsmart.zerocode.core.kafka.helper.KafkaProducerHelper.createProducer;
import static org.jsmart.zerocode.core.kafka.helper.KafkaProducerHelper.prepareJsonRecordToSend;
import static org.jsmart.zerocode.core.kafka.helper.KafkaProducerHelper.prepareRecordToSend;
import static org.jsmart.zerocode.core.kafka.helper.KafkaProducerHelper.readRecordType;
import static org.jsmart.zerocode.core.kafka.helper.KafkaProducerHelper.validateProduceRecord;
import static org.jsmart.zerocode.core.utils.SmartUtils.prettyPrintJson;

@Singleton
public class KafkaSender {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSender.class);

    @Inject(optional = true)
    @Named("kafka.producer.properties")
    private String producerPropertyFile;

    @Inject
    private ZeroCodeAssertionsProcessorImpl zeroCodeAssertionsProcessor;

    @Inject
    @Named("KafkaMapper")
    private ObjectMapper objectMapper;


    public String send(String brokers, String topicName, String requestJson, ScenarioExecutionState scenarioExecutionState) throws JsonProcessingException {
        Producer<?, ?> producer = createProducer(brokers, this.producerPropertyFile);
        String deliveryDetails = null;

        String recordType = readRecordType(requestJson, RECORD_TYPE_JSON_PATH);

        try {
            switch (recordType) {
                case RAW:
                    ProducerRawRecords rawRecords = this.objectMapper.readValue(requestJson, ProducerRawRecords.class);

                    String fileName = rawRecords.getFile();
                    if (fileName != null) {
                        File file = this.validateAndGetFile(fileName);
                        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                            String line;
                            for (int i = 0; (line = br.readLine()) != null; i++) {
                                ProducerRecord record = this.objectMapper.readValue(line, ProducerRecord.class);

                                LOGGER.debug("From file:'{}', Sending record number: {}\n", fileName, i);
                                deliveryDetails = this.sendRaw(topicName, producer, record, rawRecords.getAsync());
                            }
                        } catch (Throwable ex) {
                            throw new RuntimeException(ex);
                        }
                    } else {
                        List<ProducerRecord> records = rawRecords.getRecords();
                        validateProduceRecord(records);
                        for (int i = 0; i < records.size(); i++) {
                            LOGGER.debug("Sending record number: {}\n", i);
                            deliveryDetails = this.sendRaw(topicName, producer, records.get(i), rawRecords.getAsync());
                        }
                    }

                    break;
                case PROTO:
                case JSON:
                    ProducerJsonRecords jsonRecords = this.objectMapper.readValue(requestJson, ProducerJsonRecords.class);

                    fileName = jsonRecords.getFile();
                    if (fileName != null) {
                        File file = this.validateAndGetFile(fileName);
                        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
                            String line;
                            for (int i = 0; (line = br.readLine()) != null; i++) {
                                line = this.zeroCodeAssertionsProcessor.resolveStringJson(line,
                                        scenarioExecutionState.getResolvedScenarioState());
                                ProducerJsonRecord record = this.objectMapper.readValue(line, ProducerJsonRecord.class);
                                LOGGER.debug("From file:'{}', Sending record number: {}\n", fileName, i);
                                deliveryDetails = this.sendJson(topicName, producer, record, jsonRecords.getAsync(), recordType, requestJson);
                            }
                        }
                    } else {
                        List<ProducerJsonRecord> records = jsonRecords.getRecords();
                        validateProduceRecord(records);
                        for (ProducerJsonRecord record : records) {
                            deliveryDetails = this.sendJson(topicName, producer, record, jsonRecords.getAsync(), recordType, requestJson);
                        }
                    }

                    break;
                default:
                    throw new RuntimeException("Unsupported recordType '" + recordType + "'. Chose RAW or JSON");
            }

        } catch (Exception e) {
            LOGGER.error("Error in sending record.", e);
            String failedStatus = this.objectMapper.writeValueAsString(new DeliveryDetails(FAILED, e.getMessage()));
            return prettyPrintJson(failedStatus);

        } finally {
            producer.close();
        }

        return prettyPrintJson(deliveryDetails);

    }

    private String sendRaw(String topicName,
                           Producer<?, ?> producer,
                           ProducerRecord recordToSend,
                           Boolean isAsync) throws InterruptedException, ExecutionException, JsonProcessingException {
        ProducerRecord qualifiedRecord = prepareRecordToSend(topicName, recordToSend);

        RecordMetadata metadata;
        if (Boolean.TRUE.equals(isAsync)) {
            LOGGER.debug("Asynchronous Producer sending record - {}", qualifiedRecord);
            metadata = (RecordMetadata) producer.send(qualifiedRecord, new ProducerAsyncCallback()).get();
        } else {
            LOGGER.debug("Synchronous Producer sending record - {}", qualifiedRecord);
            metadata = (RecordMetadata) producer.send(qualifiedRecord).get();
        }

        LOGGER.debug("Record was sent to partition- {}, with offset- {} ", metadata.partition(), metadata.offset());

        // --------------------------------------------------------------
        // Logs deliveryDetails, which shd be good enough for the caller
        // TODO- combine deliveryDetails into a list n return (if needed)
        // --------------------------------------------------------------
        String deliveryDetails = this.objectMapper.writeValueAsString(new DeliveryDetails(OK, metadata));

        LOGGER.debug("deliveryDetails- {}", deliveryDetails);
        return deliveryDetails;
    }

    private String sendJson(String topicName,
                            Producer<?, ?> producer,
                            ProducerJsonRecord recordToSend,
                            Boolean isAsync,
                            String recordType,
                            String requestJson) throws InterruptedException, ExecutionException, JsonProcessingException {
        ProducerRecord record = prepareJsonRecordToSend(topicName, recordToSend, recordType, requestJson);

        RecordMetadata metadata;
        if (Boolean.TRUE.equals(isAsync)) {
            LOGGER.debug("Asynchronous - Producer sending JSON record - {}", record);
            metadata = (RecordMetadata) producer.send(record, new ProducerAsyncCallback()).get();
        } else {
            LOGGER.debug("Producer sending JSON record - {}", record);
            metadata = (RecordMetadata) producer.send(record).get();
        }

        LOGGER.debug("Record was sent to partition- {}, with offset- {} ", metadata.partition(), metadata.offset());

        // --------------------------------------------------------------
        // Logs deliveryDetails, which shd be good enough for the caller
        // TODO- combine deliveryDetails into a list n return (if needed)
        // --------------------------------------------------------------
        String deliveryDetails = this.objectMapper.writeValueAsString(new DeliveryDetails(OK, metadata));

        LOGGER.debug("deliveryDetails- {}", deliveryDetails);

        return deliveryDetails;
    }


    private File validateAndGetFile(String fileName) {
        try {
            URL resource = this.getClass().getClassLoader().getResource(fileName);
            return new File(resource.getFile());
        } catch (Exception ex) {
            throw new RuntimeException("Error accessing file: `" + fileName + "' - " + ex);
        }
    }

    static class ProducerAsyncCallback implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception ex) {
            if (ex != null) {
                LOGGER.error("Asynchronous Producer failed with exception", ex);
            } else {
                LOGGER.debug("Asynchronous Producer call was successful");
            }
        }
    }
}
