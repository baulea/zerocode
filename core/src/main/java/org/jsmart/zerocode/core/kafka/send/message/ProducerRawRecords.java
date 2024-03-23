package org.jsmart.zerocode.core.kafka.send.message;

import com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;

import static java.util.Optional.ofNullable;

public class ProducerRawRecords {
    // -------------------------------------------------------
    // A single ProducerRecord can wrap header information too
    // for individual messages.
    // TODO- see batch for common headers per batch
    // -------------------------------------------------------
    private List<ProducerRecord> records;
    private Boolean async;
    private String recordType;
    private String file;

    /**
     * default constructor is needed for Jackson
     */
    @JsonCreator
    public ProducerRawRecords() {
    }

    public ProducerRawRecords(List<ProducerRecord> records, Boolean async,
                              String recordType, String file) {
        this.records = ofNullable(records).orElse(new ArrayList<>());
        this.async = async;
        this.recordType = recordType;
        this.file = file;
    }

    public List<ProducerRecord> getRecords() {
        return ofNullable(this.records).orElse(new ArrayList<>());
    }

    public Boolean getAsync() {
        return this.async;
    }

    public String getRecordType() {
        return this.recordType;
    }

    public String getFile() {
        return this.file;
    }

    public void setRecords(List<ProducerRecord> records) {
        this.records = records;
    }

    public void setAsync(Boolean async) {
        this.async = async;
    }

    public void setRecordType(String recordType) {
        this.recordType = recordType;
    }

    public void setFile(String file) {
        this.file = file;
    }

    @Override
    public String toString() {
        return "ProducerRawRecords{" +
                "records=" + this.records +
                ", async=" + this.async +
                ", recordType='" + this.recordType + '\'' +
                ", file='" + this.file + '\'' +
                '}';
    }
}
