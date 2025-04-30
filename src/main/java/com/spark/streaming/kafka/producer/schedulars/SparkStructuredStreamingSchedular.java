package com.spark.streaming.kafka.producer.schedulars;

import com.spark.streaming.kafka.producer.services.SparkStructuredStreamingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class SparkStructuredStreamingSchedular {

    @Autowired
    private SparkStructuredStreamingService kafkaProducerService;


}
