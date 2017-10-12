package com.hit.compare.misc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JackSon {  
	  
    /** 
     * Json字符串转对象 
     * @param <T> 
     * @param jsonStr 
     * @param clazz 
     * @return 
     * @throws Exception 
     */  
    public static <T> T jsonStrToBean(String jsonStr, Class<T> clazz) throws Exception {  
        ObjectMapper mapper = new ObjectMapper();  
        return mapper.readValue(jsonStr, clazz);  
    }  
  
    /** 
     * 对象转Json字符串 
     * @param bean 
     * @return 
     * @throws Exception 
     */  
    public static String beanToJsonStr(Object bean){  
        ObjectMapper mapper = new ObjectMapper();  
        try {
			return mapper.writeValueAsString(bean);
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}  
        return "";
    }  
}  