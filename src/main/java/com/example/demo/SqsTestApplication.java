package com.example.demo;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringEscapeUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.aws.messaging.config.annotation.EnableSqs;
import org.springframework.cloud.aws.messaging.listener.annotation.SqsListener;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.s3.event.S3EventNotification.S3EventNotificationRecord;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.fasterxml.jackson.databind.ObjectMapper;

@SpringBootApplication
@EnableSqs
public class SqsTestApplication {

  @Autowired
  private AmazonS3 s3;

  ObjectMapper mapper = new ObjectMapper();

  @SqsListener("test-queue")
  public void receive(String message) throws IOException, ClassNotFoundException {
    List<S3EventNotificationRecord> records = readValue(
        readValue(message, SnsNotification.class).getMessage(), S3EventNotification.class)
            .getRecords();
    for (S3EventNotificationRecord record : records) {
      String key = record.getS3().getObject().getKey();
      S3Object object = s3
          .getObject(new GetObjectRequest("f4908ccb-df04-42aa-b8a8-185daf9fdff1", key));
      System.out.println(object);
      printString(object.getObjectContent());
    }
  }

  private void printString(InputStream objectContent) {
    BufferedReader reader = new BufferedReader(new InputStreamReader(objectContent));
    try {
      String line = reader.readLine();
      while (line != null) {
        Object object = mapper.readValue(line, Map.class);
        System.out.println(StringEscapeUtils.escapeJava(object.toString()));
        line = reader.readLine();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private <T> T readValue(String src, Class<T> valueType) {
    try {
      return mapper.readValue(src, valueType);
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  public static void main(String[] args) {
    SpringApplication.run(SqsTestApplication.class, args);
  }
}
