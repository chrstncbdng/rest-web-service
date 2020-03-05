package com.svi.webservice.main;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;

import org.apache.solr.client.solrj.SolrServerException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.exceptions.QueryValidationException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.svi.webservice.objects.Person;
import com.svi.webservice.objects.Status;
import com.svi.webservice.utils.CassandraUtil;
import com.svi.webservice.utils.SolrUtil;

@Path("/person") 
public class PersonWebService {

	@Path("/add") 
	@POST
	public Response addPersonDetails(String jsonString) throws SolrServerException, IOException {
		Status status = new Status();
		ObjectMapper mapper = new ObjectMapper();
		Person person = mapper.readValue(jsonString, Person.class);
		
		if (!SolrUtil.doesIdExists(person.getId())) {
			if (SolrUtil.addRecord(person)) {
				if (CassandraUtil.addRecord(person)) {
					status.setSuccess(true);
					status.setMessage("Person successfully added to db");	
				} else {
					status.setSuccess(false);
					status.setMessage("Failed to add record to Cassandra");
				}
			} else {
				status.setSuccess(false);
				status.setMessage("Failed to add record to Solr");
			}
		} else {
			status.setSuccess(false);
			status.setMessage("Record with the same ID already exists");
		}
		
		return Response.ok(status).build();
		
	}
	
	@Path("/update")
	@POST
	public Response updatePersonDetails(String jsonString) throws SolrServerException, IOException {
		Status status = new Status();
		ObjectMapper mapper = new ObjectMapper();
		Person person = mapper.readValue(jsonString, Person.class);
		
		if (SolrUtil.updateRecord(person)) {
			if (CassandraUtil.updateRecord(person)) {
				status.setSuccess(true);
				status.setMessage("Person successfully updated to db");	
			} else {
				status.setSuccess(false);
				status.setMessage("Failed to update record to Cassandra");
			}
		} else {
			status.setSuccess(false);
			status.setMessage("Failed to update record to Solr");
		}
		
		return Response.ok(status).build();
	}
	
	@Path("/retrieve/{field}/{value}")
	@GET
	public Response retrieveDetails(@PathParam("field") String field, @PathParam("value") String value) throws SolrServerException, IOException {
		Status status = new Status();
		Response response = null;
		ResponseBuilder responseBuilder = null;
		List<String> uniqueIds = new ArrayList<>();
		
		uniqueIds = SolrUtil.getUniqueIds(field, value);
		
		if (!uniqueIds.isEmpty()) {
			try {
				List<Person> persons = CassandraUtil.retrieveRecords(uniqueIds);
				responseBuilder = Response.ok(new ObjectMapper().writeValueAsString(persons));
			} catch (NoHostAvailableException | QueryExecutionException | QueryValidationException e) {
				status.setSuccess(false);
				status.setMessage("Failed to retrieve records. " + e.getMessage());
				responseBuilder = Response.ok(status);
			}
		} else {
			status.setSuccess(false);
			status.setMessage("Retrieved records is empty");
			responseBuilder = Response.ok(status);
		}
		
		response = responseBuilder.build();
		return response;
	}
	
	@Path("/delete/{unique_id}")
	@DELETE
	public Response deleteRecord(@PathParam("unique_id") String id) throws SolrServerException, IOException {
		Status status = new Status();
		
		if (SolrUtil.deleteRecord(id)) {
			if (CassandraUtil.deleteRecord(id)) {
				status.setSuccess(true);
				status.setMessage("Person successfully deleted from db");	
			} else {
				status.setSuccess(true);
				status.setMessage("Failed to delete record from Cassandra");	
			}
		} else {
			status.setSuccess(false);
			status.setMessage("Failed to delete record from Solr");
		}
		
		return Response.ok(status).build();
	}

}
