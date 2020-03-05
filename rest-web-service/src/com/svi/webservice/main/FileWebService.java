package com.svi.webservice.main;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;

import org.glassfish.jersey.media.multipart.BodyPart;
import org.glassfish.jersey.media.multipart.FormDataMultiPart;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.svi.gfs.object.FileObject;
import com.svi.webservice.objects.Status;
import com.svi.webservice.utils.FileUtil;
import com.svi.webservice.utils.GfsUtil;

@Path("/file")
public class FileWebService {

	@Path("/upload/{key}")
	@POST
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response upload(FormDataMultiPart formData, @PathParam("key") String key) {
		Status status = new Status();
		List<BodyPart> files = formData.getBodyParts();
		List<FileObject> filesToUpload = new ArrayList<>();
		
		if (!files.isEmpty()) {
			for (BodyPart file : files) {
				try (InputStream inputStream = file.getEntityAs(InputStream.class)) {
					String fileName = file.getContentDisposition().getParameters().get("filename");
					FileObject fileToUpload = FileUtil.convertToFileObject(fileName, inputStream);
					filesToUpload.add(fileToUpload);
				} catch (Exception e) {
					System.out.println(e.toString());
				}
			}
			
			if (GfsUtil.uploadFiles(key, filesToUpload)) {
				status.setSuccess(true);
				status.setMessage("Files uploaded successfully.");
			} else {
				status.setSuccess(false);
				status.setMessage("Failed to upload files.");
			}
			
		} else {
			status.setSuccess(false);
			status.setMessage("No file/s found. Failed to upload.");
		}
		
		
		return Response.ok(status).build();
	}
	
	@Path("update/{key}")
	@POST
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response update(FormDataMultiPart formData, @PathParam("key") String key) {
		Status status = new Status();
		List<BodyPart> files = formData.getBodyParts();
		List<FileObject> filesToUpload = new ArrayList<>();
		
		if (!files.isEmpty()) {
			for (BodyPart file : files) {
				try (InputStream inputStream = file.getEntityAs(InputStream.class)) {
					String fileName = file.getContentDisposition().getParameters().get("filename");
					FileObject fileToUpload = FileUtil.convertToFileObject(fileName, inputStream);
					filesToUpload.add(fileToUpload);
				} catch (Exception e) {
					System.out.println(e.toString());
				}
			}
			
			if (GfsUtil.updateFiles(key, filesToUpload)) {
				status.setSuccess(true);
				status.setMessage("Files updated successfully.");
			} else {
				status.setSuccess(false);
				status.setMessage("Failed to update files.");
			}
			
		} else {
			status.setSuccess(false);
			status.setMessage("No file/s found. Failed to update.");
		}
		
		return Response.ok(status).build();
	}
	
	@Path("/retrieve/{key}")
	@GET
	public Response retrieve(@PathParam("key") String key) {
		Status status = new Status();
		Response response = null;
		ResponseBuilder responseBuilder = null;
		
		if (GfsUtil.hasKey(key)) {
			try {
				List<FileObject> files = GfsUtil.retrieveFiles(key);
				responseBuilder = Response.ok(new ObjectMapper().writeValueAsString(files));
			} catch (JsonProcessingException e) {
				status.setSuccess(false);
				status.setMessage("Failed to retrieve files. " + e.getMessage());
				responseBuilder = Response.ok(status);
			}
		} else {
			status.setSuccess(false);
			status.setMessage("Key not found. Failed to retrieve files.");
			responseBuilder = Response.ok(status);
		}
		
		response = responseBuilder.build();
		return response;
	}
	
	@Path("/download/{key}/{id}")
	@GET
	@Produces()
	public Response download(@PathParam("key") String key, @PathParam("id") String id) {
		Status status = new Status();
		Response response = null;
		ResponseBuilder responseBuilder = null;
		
		if (GfsUtil.hasKey(key)) {
			ByteArrayOutputStream baos = null;
			if (key == "all" || key == "*") {
				List<FileObject> files = GfsUtil.retrieveFiles(key);
				try {
					baos = FileUtil.convertToZipOutputStream(files);
					responseBuilder = Response.ok(baos.toByteArray());
					responseBuilder.header("Content-Disposition","attachment; filename=\""
							+ key + ".zip\"");
				} catch (IOException e) {
					status.setSuccess(false);
					status.setMessage("Failed to download file/s. " + e.getMessage());
					responseBuilder = Response.ok(status);
				}
			} else {
				FileObject file = GfsUtil.retrieveFile(key, id);
				if (file != null) {
					responseBuilder = Response.ok(file.getFileBlob());
					responseBuilder.header("Content-Disposition","attachment; filename=\""
							+ file.getFileName() + "." + file.getFileType() + "\"");
				} else {
					status.setSuccess(false);
					status.setMessage("ID does not exist. Failed to download file/s.");
					responseBuilder = Response.ok(status);
				}
			}
		} else {
			status.setSuccess(false);
			status.setMessage("Key does not exist. Failed to download file/s.");
			responseBuilder = Response.ok(status);
		}
		
		response = responseBuilder.build();
		return response;
	}
	
	@Path("/delete/{key}")
	@POST
	public Response delete(@PathParam("key") String key) {
		Status status = new Status();
		
		if (GfsUtil.deleteKey(key)) {
			status.setSuccess(true);
			status.setMessage("Key successfully deleted.");
		} else {
			status.setSuccess(false);
			status.setMessage("Failed to delete key.");
		}
		
		return Response.ok(status).build();
	}

}
