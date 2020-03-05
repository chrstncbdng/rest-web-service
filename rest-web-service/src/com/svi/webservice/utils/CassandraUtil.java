package com.svi.webservice.utils;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.exceptions.QueryValidationException;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.svi.webservice.constants.ConfigEnum;
import com.svi.webservice.objects.Person;

public class CassandraUtil {

	private static Cluster cluster = Cluster.builder().addContactPoint(ConfigEnum.CASSANDRA_DOMAIN.value()).build();
	private static Session session = cluster.connect(ConfigEnum.CASSANDRA_KEYSPACE.value());
	private static String table = ConfigEnum.CASSANDRA_PERSON_DETAILS_TABLE.value();
	
	public static boolean addRecord(Person person) throws JSONException, JsonProcessingException {
		ObjectMapper mapper = new ObjectMapper();
		JSONObject jsonObject = new JSONObject(mapper.writeValueAsString(person));
		
		List<String> keys = new ArrayList<String>();
		List<String> values = new ArrayList<String>();
		
		jsonObject.keys().forEachRemaining(key -> {
			keys.add(key);
			values.add(jsonObject.get(key).toString());
		});
		
		String query = "INSERT INTO " + table + " (" + String.join(", ", keys) 
				+ ") VALUES( '" + String.join("', '", values) + "')";
		
		try {
			session.execute(query);
			return true;
//		} catch (NoHostAvailableException | QueryExecutionException | QueryValidationException e) {
		} catch (Exception e) {
			System.out.println(e.toString());
			return false;
		}
		
	}
	
	public static boolean updateRecord(Person person) {
		boolean success = true;
		
		Field[] fields = Person.class.getDeclaredFields();
		
		try {
			for (Field field : fields) {
				field.setAccessible(true);
				String fieldName = field.getAnnotation(JsonProperty.class).value();
				if (field.get(person) != null || field.get(person) != "") {
					String query = "UPDATE " + table + " SET " + fieldName + "='" + field.get(person) + "' WHERE unique_id='" + person.getUniqueId() + "'";
					session.execute(query);
				}
			}
//		} catch (IllegalArgumentException | IllegalAccessException | NoHostAvailableException | QueryExecutionException | QueryValidationException e) {}
		} catch (Exception e) {
			System.out.println(e.toString());
			success = false;
		}
		
		return success;
		
	}
	
	public static List<Person> retrieveRecords(List<String> uniqueIds) throws NoHostAvailableException, QueryExecutionException, QueryValidationException {
		List<Person> persons = new ArrayList<>();
		
		for (String id : uniqueIds) {
			String query = "SELECT * FROM " + table + " WHERE unique_id='" + id + "'";
			ResultSet result = session.execute(query);
			
			Row row = result.one();
			//TODO REVISE
			Person person = new Person();
			person.setUniqueId(row.getString("unique_id"));
			person.setId(row.getString("id"));
			person.setFirstName(row.getString("first_name"));
			person.setLastName(row.getString("last_name"));
			person.setAge(row.getString("age"));
			person.setBirthDate(row.getString("birth_date"));
			person.setOccupation(row.getString("occupation"));
			
			persons.add(person);
		}
		
		return persons;
	}
	
	public static boolean deleteRecord(String id) {
		String query = "DELETE FROM " + table + " WHERE id='" + id + "'";
		try {
			session.execute(query);
			return true;
//		} catch (NoHostAvailableException | QueryExecutionException | QueryValidationException e) {
		} catch (Exception e) {
			System.out.println(e.toString());
			return false;
		}
	}

}
