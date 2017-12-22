package com.zhuowang.component;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaComponent {
	
	public static final String TOPIC_NAME = "hello_topic";

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;
	
	public void send(String data){
		kafkaTemplate.send(TOPIC_NAME, data);
	}
}
