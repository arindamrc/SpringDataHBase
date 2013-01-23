package com.infy.hbase.utilities;

/* © 2012 Infosys Technologies Limited, Bangalore, India. All rights reserved.
 * Version: 1.0
 * Except for any open source software components embedded in this Infosys
 * proprietary software program (“Program”), this Program is protected by
 * copyright laws, international treaties and other pending or existing
 * intellectual property rights in India, the United States and other countries.
 * Except as expressly permitted, any unauthorized reproduction, storage,
 * transmission in any form or by any means (including without limitation
 * electronic, mechanical, printing, photocopying, recording or otherwise),
 * or any distribution of this Program, or any portion of it, may result in
 * severe civil and criminal penalties, and will be prosecuted to the maximum
 * extent possible under the law.*/

import java.util.Properties;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.springframework.beans.factory.FactoryBean;

public class HBaseConfigurationFactoryBean implements FactoryBean<Configuration> {
	 private Properties hbaseProperties;

	    @Override
	    public Configuration getObject() throws Exception {
	        if (hbaseProperties != null) {
	            Configuration config = HBaseConfiguration.create();
	            Set<String> propertyNames = hbaseProperties.stringPropertyNames();
	            for (String propertyName : propertyNames) {
	                config.set(propertyName, hbaseProperties.getProperty(propertyName));
	            }
	            return config;
	        } else {
	            throw new RuntimeException("hbase properties cannot be null");
	        }
	    }

	    public Properties getHbaseProperties() {
	        return hbaseProperties;
	    }

	    public void setHbaseProperties(Properties hbaseProperties) {
	        this.hbaseProperties = hbaseProperties;
	    }

	    @SuppressWarnings("rawtypes")
	    @Override
	    public Class<HBaseConfiguration> getObjectType() {
	        return HBaseConfiguration.class;
	    }

	    @Override
	    public boolean isSingleton() {
	        return true;
	    }
}
