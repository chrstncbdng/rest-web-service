package com.svi.webservice.utils;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.svi.webservice.constants.ConfigEnum;
import com.svi.webservice.objects.Person;

public class SolrUtil {

	private static String solrUrl = ConfigEnum.SOLR_SERVER.value() + ConfigEnum.SOLR_PERSON_DETAILS_CORE.value();
	private static SolrClient solrClient = new HttpSolrClient.Builder(solrUrl).build();
	
	public static boolean addRecord(Person person) throws SolrServerException, IOException {
		boolean success = false;
		SolrInputDocument solrDoc = new SolrInputDocument();
		
		solrDoc.addField("unique_id", UUID.randomUUID().toString());
		solrDoc.addField("id", person.getId());
		solrDoc.addField("first_name", person.getFirstName());
		solrDoc.addField("last_name", person.getLastName());
		solrDoc.addField("age", person.getAge());
		
		solrClient.add(solrDoc);
		UpdateResponse response = solrClient.commit();
		
		if (response.getStatus() == 0) {
			success = true;
		}
		
		return success;
	}
	
	public static boolean updateRecord(Person person) throws SolrServerException, IOException {
		boolean success = false;
		SolrDocument originalRecord = getOriginalRecord(person.getUniqueId());
		
		SolrInputDocument updatedRecord = new SolrInputDocument();
		
		originalRecord.forEach((key, value) -> {
			updatedRecord.addField(key, value);
		});
		
		//update person details to updatedRecord
		Field[] fields = Person.class.getDeclaredFields();
		for (Field field : fields) {
			field.setAccessible(true);
			String fieldName = field.getAnnotation(JsonProperty.class).value();
			try {
				if (updatedRecord.containsKey(fieldName)) {	
					updatedRecord.setField(fieldName, field.get(person).toString());
				}
			} catch (NullPointerException | IllegalArgumentException | IllegalAccessException e) {}
		}
		
		solrClient.add(updatedRecord);
		UpdateResponse response = solrClient.commit();
		
		if (response.getStatus() == 0) {
			success = true;
		}
		
		return success;
	}
	
	public static boolean deleteRecord(String uniqueId) throws SolrServerException, IOException {
		boolean success = false;
		
		solrClient.deleteByQuery("unique_id:" + uniqueId);
		UpdateResponse response = solrClient.commit();
		
		if (response.getStatus() == 0) {
			success = true;
		}
		
		return success;
	}
	
	public static List<String> getUniqueIds(String field, String value) throws SolrServerException, IOException {
		List<String> uniqueIds = new ArrayList<>();
		SolrDocumentList solrDocs = new SolrDocumentList();
		SolrQuery params = new SolrQuery();
		QueryResponse response;
		
		params.setQuery(field + ":" + value);
		params.addField("unique_id");
		params.setRows(Integer.MAX_VALUE);
		
		response = solrClient.query(params);
		solrDocs = response.getResults();
		
		for (SolrDocument solrDoc : solrDocs) {
			uniqueIds.add(getSolrStringValue(solrDoc, "unique_id"));
		}
		
		return uniqueIds;
		
	}
	
	private static String getSolrStringValue(SolrDocument doc, String fieldName) {
		if (doc.containsKey(fieldName)) {
			return doc.get(fieldName).toString();
		}
		
		return "";
	}
	
	public static boolean doesIdExists(String id) throws SolrServerException, IOException {
		boolean exists = false;
		SolrDocumentList solrDocs = new SolrDocumentList();
		SolrQuery params = new SolrQuery();
		QueryResponse response;
		
		params.setQuery("id:" + id);
		params.setRows(Integer.MAX_VALUE);
		
		response = solrClient.query(params);
		solrDocs = response.getResults();
		
		if (!solrDocs.isEmpty()) {
			exists = true;
		}
		
		return exists;
		
	}
	
	private static SolrDocument getOriginalRecord(String id) throws SolrServerException, IOException {
		SolrDocumentList results = new SolrDocumentList();
		SolrQuery params = new SolrQuery();
		QueryResponse response;
		
		params.setQuery("unique_id:" + id);
		params.addField("unique_id, id, first_name, last_name, age");
		
		response = solrClient.query(params);
		results = response.getResults();
		
		return results.get(0);
	}

}
