package com.spark.streaming.kafka.producer.rest.controllers;

import com.spark.streaming.kafka.producer.constant.SendMessageType;
import com.spark.streaming.kafka.producer.models.SparkStructuredStreamingUniversalDataModel;
import com.spark.streaming.kafka.producer.schedulars.thread.runnable.SparkStructuredStreamingEventRunnableTask;
import com.spark.streaming.kafka.producer.services.SparkStructuredStreamingService;
import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.web.bind.annotation.*;

import java.util.Date;
import java.util.List;
import java.util.Optional;

@RestController
@ResponseBody
@RequestMapping("/fiserv/spark/structured/streaming/api")
public class SparkStructuredStreamingRestController {

    @Autowired
    private SparkStructuredStreamingService kafkaProducerService;

    @Autowired
    private ThreadPoolTaskScheduler taskScheduler;

    private SparkStructuredStreamingEventRunnableTask eventRunnableTask;

    @PostConstruct
    public void initialize() {
        eventRunnableTask = new SparkStructuredStreamingEventRunnableTask();
        eventRunnableTask.setKafkaProducerService(kafkaProducerService);
    }

    @PostMapping("/sendSingleStringMessage")
    private ResponseEntity<String> sendSingleStringMessage(@RequestBody String message) {
        kafkaProducerService.sendStringMessage(message);
        return ResponseEntity.of(Optional.of("Message Published to Topic Successfully"));
    }

    @PostMapping("/sendMultiStringMessage")
    private ResponseEntity<String> sendMultiStringMessage(@RequestBody List<String> requestData) {
        requestData.forEach(data -> kafkaProducerService.sendStringMessage(data));
        return ResponseEntity.of(Optional.of("Messages Published to Topic Successfully"));
    }

    @PostMapping("/sendSingleCustomMessage")
    private ResponseEntity<String> sendSingleCustomMessage(@RequestBody SparkStructuredStreamingUniversalDataModel requestDataObject) {
        kafkaProducerService.sendCustomModelMessage(requestDataObject);
        return ResponseEntity.of(Optional.of("Message Published to Topic Successfully"));
    }


    @PostMapping("/sendMultiCustomMessage")
    private ResponseEntity<String> sendMultiCustomMessage(@RequestBody List<SparkStructuredStreamingUniversalDataModel> requestDataObjectList) {
        requestDataObjectList.forEach(data -> kafkaProducerService.sendCustomModelMessage(data));
        return ResponseEntity.of(Optional.of("Messages Published to Topic Successfully"));
    }

    @PostMapping("/scheduleSingleStringMessage")
    private ResponseEntity<String> scheduleSingleStringMessage(@RequestBody String requestData, @RequestParam String timeInMillis) {
        SparkStructuredStreamingEventRunnableTask.TaskRunner taskRunner = new SparkStructuredStreamingEventRunnableTask.TaskRunner();
        taskRunner.setJsonData(requestData);
        taskRunner.setSendMessageType(SendMessageType.SINGLE_STRING_MESSAGE);
        eventRunnableTask.setTaskRunner(taskRunner);
        taskScheduler.schedule(
                eventRunnableTask.getTaskRunner(),
                new Date(System.currentTimeMillis() + Integer.valueOf(timeInMillis))
        );
        return ResponseEntity.of(Optional.of("Schedular scheduleSingleStringMessage started Successfully"));
    }

    @PostMapping("/scheduleMultiStringMessage")
    private ResponseEntity<String> scheduleSingleStringMessage(@RequestBody List<String> requestData, @RequestParam String timeInMillis) {
        SparkStructuredStreamingEventRunnableTask.TaskRunner taskRunner = new SparkStructuredStreamingEventRunnableTask.TaskRunner();
        taskRunner.setRequestData(requestData);
        taskRunner.setSendMessageType(SendMessageType.MULTI_STRING_MESSAGE);
        eventRunnableTask.setTaskRunner(taskRunner);
        taskScheduler.schedule(
                eventRunnableTask.getTaskRunner(),
                new Date(System.currentTimeMillis() + Integer.valueOf(timeInMillis))
        );
        return ResponseEntity.of(Optional.of("Schedular scheduleSingleStringMessage started Successfully"));
    }

    @PostMapping("/scheduleSingleCustomMessage")
    private ResponseEntity<String> scheduleSingleCustomMessage(@RequestBody SparkStructuredStreamingUniversalDataModel requestDataObject, @RequestParam String timeInMillis) {
        SparkStructuredStreamingEventRunnableTask.TaskRunner taskRunner = new SparkStructuredStreamingEventRunnableTask.TaskRunner();
        taskRunner.setRequestDataObject(requestDataObject);
        taskRunner.setSendMessageType(SendMessageType.SINGLE_CUSTOM_MODEL_MESSAGE);
        eventRunnableTask.setTaskRunner(taskRunner);
        taskScheduler.schedule(
                eventRunnableTask.getTaskRunner(),
                new Date(System.currentTimeMillis() + Integer.valueOf(timeInMillis))
        );
        return ResponseEntity.of(Optional.of("Schedular scheduleSingleCustomMessage started Successfully"));
    }


    @PostMapping("/scheduleMultiCustomMessage")
    private ResponseEntity<String> scheduleMultiCustomMessage(@RequestBody List<SparkStructuredStreamingUniversalDataModel> requestDataObjectList, @RequestParam String timeInMillis) {
        SparkStructuredStreamingEventRunnableTask.TaskRunner taskRunner = new SparkStructuredStreamingEventRunnableTask.TaskRunner();
        taskRunner.setRequestDataObjectList(requestDataObjectList);
        taskRunner.setSendMessageType(SendMessageType.MULTI_CUSTOM_MODEL_MESSAGE);
        eventRunnableTask.setTaskRunner(taskRunner);
        taskScheduler.schedule(
                eventRunnableTask.getTaskRunner(),
                new Date(System.currentTimeMillis() + Integer.valueOf(timeInMillis))
        );
        return ResponseEntity.of(Optional.of("Schedular scheduleMultiCustomMessage started Successfully"));
    }


}
