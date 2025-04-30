package com.spark.streaming.kafka.producer.models;

public class SparkStructuredStreamingUniversalDataModel {

    private Object dataInstance;

    private String dataJson;

    public Object getDataInstance() {
        return dataInstance;
    }

    public void setDataInstance(Object dataInstance) {
        this.dataInstance = dataInstance;
    }

    public String getDataJson() {
        return dataJson;
    }

    public void setDataJson(String dataJson) {
        this.dataJson = dataJson;
    }
}
