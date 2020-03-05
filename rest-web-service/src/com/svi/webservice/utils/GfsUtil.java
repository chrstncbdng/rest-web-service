package com.svi.webservice.utils;

import java.util.List;

import com.svi.gfs.main.Gfs;
import com.svi.gfs.object.FileObject;
import com.svi.webservice.constants.ConfigEnum;

public class GfsUtil {

	private static String gfsEnvironmentLoc = ConfigEnum.GFS_PATH.value();
	private static Gfs gfs = Gfs.newLocalStorageBuilder(gfsEnvironmentLoc).build();
	
	public static boolean uploadFiles(String key, List<FileObject> files) {
		boolean isUploaded = false;
		
		gfs.upload(key, files);
		isUploaded = gfs.exists(key);
		
		return isUploaded;
	}
	
	public static boolean updateFiles(String key, List<FileObject> files) {
		boolean isUpdated = false;
		int numFiles = gfs.retrieve(key).size();
		
		gfs.update(key, files);
		isUpdated = (gfs.retrieve(key).size() > numFiles);
		
		return isUpdated;
	}
	
	public static FileObject retrieveFile(String key, String id) {
		List<FileObject> files = gfs.retrieve(key);
		for(FileObject file : files) {
			if (file.getFileId() == id) {
				return file;
			}
		}
		
		return null;
	}
	
	public static List<FileObject> retrieveFiles(String key) {
		return gfs.retrieve(key);
	}
	
	public static boolean hasKey(String key) {
		return gfs.exists(key);
	}
	
	public static boolean deleteKey(String key) {
		gfs.delete(key);
		return gfs.exists(key);
	}

}
