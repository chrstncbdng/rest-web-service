package com.svi.webservice.constants;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public enum ConfigEnum {
	//SOLR
	SOLR_SERVER("SOLR_SERVER"),
	SOLR_PERSON_DETAILS_CORE("SOLR_PERSON_DETAILS_CORE"),
	
	//CASSANDRA
	CASSANDRA_DOMAIN("CASSANDRA_DOMAIN"),
	CASSANDRA_KEYSPACE("CASSANDRA_KEYSPACE"),
	CASSANDRA_PERSON_DETAILS_TABLE("CASSANDRA_PERSON_DETAILS_TABLE"),
	
	//GFS
	GFS_PATH("GFS_PATH");
	
	private String value = "";
	private static Properties properties = null;
	
	private ConfigEnum(String value) {
		this.value = value;
	}
	
	public String value() {
		String propertyValue = "";
		
		if (properties == null) {
			try {
				properties = getProperty();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
		try {
			propertyValue = properties.getProperty(value).trim();
		} catch (Exception e) {}
		
		return propertyValue;
	}
	
	private Properties getProperty() throws IOException {
		Properties properties = new Properties();
		String filePath = "Properties" + File.separator + "MainProperties";
		FileInputStream file = new FileInputStream(filePath);
		properties.load(file);
		file.close();
		
		return properties;
	}

}
