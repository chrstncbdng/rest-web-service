package com.svi.webservice.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.UUID;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import com.svi.gfs.object.FileObject;

public class FileUtil {

	public static FileObject convertToFileObject(String fileNameType, InputStream inputStream) throws IOException {
		FileObject file = new FileObject();
		String fileName = "";
		String fileType = "";
		String fileId = UUID.randomUUID().toString();
		int typeIndex = fileNameType.lastIndexOf('.');
		fileName = fileNameType.substring(0, typeIndex);
		if (typeIndex > 0) {
			fileType = fileNameType.substring(typeIndex);
		}
		
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		int size = inputStream.read();
		
		while (size != -1) {
			baos.write(size);
			size = inputStream.read();
		}
		
		file.setFileId(fileId);
		file.setFileName(fileName);
		file.setFileType(fileType);
		file.setFileBlob(baos.toByteArray());
		
		return file;
	}
	
	public static ByteArrayOutputStream convertToZipOutputStream(List<FileObject> files) throws IOException {
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		ZipOutputStream zos = new ZipOutputStream(baos);
		
		for (FileObject file : files) {
			zos.putNextEntry(new ZipEntry(file.getFileName() 
					+ "." + file.getFileType()));
			zos.write(file.getFileBlob());
			zos.closeEntry();
		}
		
		zos.close();
		return baos;
	}

}
