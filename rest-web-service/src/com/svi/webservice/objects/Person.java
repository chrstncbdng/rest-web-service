package com.svi.webservice.objects;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Person {

	@JsonProperty("unique_id")
	private String uniqueId;
	
	@JsonProperty("id")
	private String id;
	
	@JsonProperty("first_name")
	private String firstName;
	
	@JsonProperty("last_name")
	private String lastName;
	
	@JsonProperty("age")
	private String age;
	
	@JsonProperty("birthdate")
	private String birthDate;
	
	@JsonProperty("address")
	private String address;
	
	@JsonProperty("occupation")
	private String occupation;
	
	public String getUniqueId() {
		return uniqueId;
	}
	
	public void setUniqueId(String uniqueId) {
		this.uniqueId = uniqueId;
	}
	
	public String getId() {
		return id;
	}
	
	public void setId(String id) {
		this.id = id;
	}
	
	public String getFirstName() {
		return firstName;
	}
	
	public void setFirstName(String firstName) {
		this.firstName = firstName;
	}
	
	public String getLastName() {
		return lastName;
	}
	
	public void setLastName(String lastName) {
		this.lastName = lastName;
	}
	
	public String getAge() {
		return age;
	}
	
	public void setAge(String age) {
		this.age = age;
	}
	
	public String getBirthDate() {
		return birthDate;
	}
	
	public void setBirthDate(String birthDate) {
		this.birthDate = birthDate;
	}
	
	public String getAddress() {
		return address;
	}
	
	public void setAddress(String address) {
		this.address = address;
	}
	
	public String getOccupation() {
		return occupation;
	}
	
	public void setOccupation(String occupation) {
		this.occupation = occupation;
	}
	
	public String toString() {
		return "Person [ID: " + this.getId() + ", Name: " + this.getFirstName() + " " + this.getLastName() +  ", Age: " + this.getAge() +"]";
	}

}
