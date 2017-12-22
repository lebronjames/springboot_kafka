package com.zhuowang.controller;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.ArrayType;
import com.fasterxml.jackson.databind.type.CollectionType;
import com.zhuowang.component.KafkaComponent;
import com.zhuowang.kafka.User;

@RestController
@RequestMapping("/kafka")
public class KafkaController {
	
	@Autowired
	private KafkaComponent kafkaComponent;

	@RequestMapping(value="/producer")
	public void producer(){
		ObjectMapper mapper= new ObjectMapper();
		User u1 = new User("lebron","1");
		User u2 = new User("james","2");

		try {
			User[] array = new User[]{u1,u2};
			String jsonArrayStr = mapper.writeValueAsString(array);
			System.out.println("jsonArrayStr:"+jsonArrayStr);
			kafkaComponent.send(jsonArrayStr);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
	}
	
	@KafkaListener(topics = {"hello_topic"})
	public void consumer(ConsumerRecord<?, ?> record){
		ObjectMapper mapper= new ObjectMapper();
		Optional<?> kafkaMessage = Optional.ofNullable(record.value());
        if (kafkaMessage.isPresent()) {
            Object message = kafkaMessage.get();
            System.out.println("listen: " + message);
            ArrayType arrayType = mapper.getTypeFactory().constructArrayType(User.class);
			User[] array1 = null;
			try {
				array1 = mapper.readValue(message.toString(), arrayType);
				System.out.println("jsonarray 转array,length:"+array1.length);
				for(User u : array1){
					System.out.println("--------------##id:"+u.getId()+", name:"+u.getName());
				}
			} catch (JsonParseException e) {
				e.printStackTrace();
			} catch (JsonMappingException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
        }
	}
	
	@RequestMapping(value="/test")
	public void test(){
		ObjectMapper mapper= new ObjectMapper();
		User u1 = new User("lebron","1");
		User u2 = new User("james","2");
		
		String jsonStr = null;
		String jsonArrayStr = null;
		String jsonListStr = null;
		String mapStr = null;
		Map<String,Object> map = new HashMap<String,Object>();
		try {
			jsonStr = mapper.writeValueAsString(u1);
			System.out.println("jsonStr:"+jsonStr);
			
			User[] array = new User[]{u1,u2};
			jsonArrayStr = mapper.writeValueAsString(array);
			System.out.println("jsonArrayStr:"+jsonArrayStr);
			
			List<User> list = Arrays.asList(u1,u2,new User("kebe","3"));
			jsonListStr = mapper.writeValueAsString(list);
			System.out.println("jsonListStr:"+jsonListStr);
			
			map.put("key1", u1);
			map.put("key2", u2);
			
			mapStr = mapper.writeValueAsString(map);
			System.out.println("mapStr:"+mapStr);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		
		try {
			User u = mapper.readValue(jsonStr, User.class);
			System.out.println("user:"+u.toString());
			//jsonArray转换成Array数组
			ArrayType arrayType = mapper.getTypeFactory().constructArrayType(User.class);
			User[] array1 = mapper.readValue(jsonArrayStr, arrayType);
			System.out.println("jsonarray 转array,length:"+array1.length);
			//jsonarray 转list
			CollectionType listType = mapper.getTypeFactory().constructCollectionType(ArrayList.class, User.class);
			List<User> list1 = mapper.readValue(jsonListStr, listType);
			System.out.println("jsonarray 转list,size:"+list1.size());
			
		} catch (JsonParseException e) {
			e.printStackTrace();
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) {
		new KafkaController().producer();
	}
}
