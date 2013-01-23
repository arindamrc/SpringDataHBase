package com.infy.hbase.context;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class ApplicationContextHelper {
	private static ApplicationContext applicationContext = null;
	private static Object applicationContextLock = new Object();
	private static ApplicationContext getApplicationContext() {
		synchronized (applicationContextLock) {
			if(applicationContext != null) {
				return applicationContext;
			}
			applicationContext = new ClassPathXmlApplicationContext("classpath:springData.xml");
			return applicationContext;
		}
	}
	public static Object getBean(String beanName) {
		if(applicationContext == null) {
			getApplicationContext();
		}
		return applicationContext.getBean(beanName);
	}
}
