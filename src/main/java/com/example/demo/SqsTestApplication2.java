package com.example.demo;

import java.io.IOException;
import java.util.Collections;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.aws.messaging.config.QueueMessageHandlerFactory;
import org.springframework.cloud.aws.messaging.config.annotation.EnableSns;
import org.springframework.cloud.aws.messaging.config.annotation.EnableSqs;
import org.springframework.cloud.aws.messaging.listener.annotation.SqsListener;
import org.springframework.context.annotation.Bean;
import org.springframework.integration.annotation.MessagingGateway;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.messaging.converter.MappingJackson2MessageConverter;
import org.springframework.messaging.handler.annotation.support.PayloadArgumentResolver;
import org.springframework.messaging.handler.invocation.HandlerMethodArgumentResolver;

import com.amazonaws.services.s3.event.S3EventNotification;
import com.amazonaws.services.sqs.AmazonSQSAsync;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

//@SpringBootApplication
//@EnableSqs
//@EnableSns
public class SqsTestApplication2 {
	
	@Autowired
	private AmazonSQSAsync amazonSqs;

//	@Bean
//	public MessageChannel inputChannel() {
//		return new QueueChannel();
//	}
//	
//	@Bean(name = PollerMetadata.DEFAULT_POLLER)
//	  public PollerMetadata poller() {                               // 11
//	  	return Pollers.fixedDelay(1000).get();
//	  }
//
//	@Bean
//	public MessageProducer sqsMessageDrivenChannelAdapter() {
//		SqsMessageDrivenChannelAdapter adapter = new SqsMessageDrivenChannelAdapter(this.amazonSqs, "test-queue");
//		adapter.setOutputChannel(inputChannel());
//		return adapter;
//	}
	
	@Autowired
	private TestGateway gateway;
	
	@SqsListener("test-queue")
	public void receive(String message) {
		System.out.println(message);
		gateway.send(message);
	}
	
	@MessagingGateway(defaultRequestChannel = "inputChannel")
	public interface TestGateway {
		void send(String message);
	}

//	@Bean
//	public QueueMessageHandlerFactory queueMessageHandlerFactory() {
//		QueueMessageHandlerFactory factory = new QueueMessageHandlerFactory();
//		MappingJackson2MessageConverter messageConverter = new MappingJackson2MessageConverter();
//
//		//set strict content type match to false
//		messageConverter.setStrictContentTypeMatch(false);
//		factory.setArgumentResolvers(Collections.<HandlerMethodArgumentResolver>singletonList(new PayloadArgumentResolver(messageConverter)));
//		return factory;
//	}
	
	@Bean
	public IntegrationFlow testFlow() {
		ObjectMapper mapper = new ObjectMapper();
		return IntegrationFlows.from("inputChannel")
				.transform(String.class, p -> {
					try {
						SnsNotification sns = mapper.readValue(p, SnsNotification.class);
						S3EventNotification s3 = mapper.readValue(sns.getMessage(), S3EventNotification.class);
//						return mapper.readValue(sns, S3EventNotification.class);
//						return sns;
						return s3;
					} catch (IOException e) {
						e.printStackTrace();
						return null;
					}
				})
				.log()
				.get();
	}
	
	public static void main(String[] args) {
		SpringApplication.run(SqsTestApplication2.class, args);
	}
}
