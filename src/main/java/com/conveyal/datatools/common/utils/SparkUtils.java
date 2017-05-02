package com.conveyal.datatools.common.utils;

import spark.Response;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;

import com.amazonaws.services.s3.model.S3Object;

import static spark.Spark.halt;

/**
 * Created by landon on 12/15/16.
 */
public class SparkUtils {

    public static Object downloadFile(File file, String filename, Response res) {
        if(file == null) halt(404, "File is null");

        res.raw().setContentType("application/octet-stream");
        res.raw().setHeader("Content-Disposition", "attachment; filename=" + filename);

        try {
            BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(res.raw().getOutputStream());
            BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(file));

            byte[] buffer = new byte[1024];
            int len;
            while ((len = bufferedInputStream.read(buffer)) > 0) {
                bufferedOutputStream.write(buffer, 0, len);
            }

            bufferedOutputStream.flush();
            bufferedOutputStream.close();
        } catch (Exception e) {
            halt(500, "Error serving GTFS file");
        }

        return res.raw();
    }

    public static Object downloadFile(S3Object object, String fileName, Response res) {
	res.raw().setContentType("application/octet-stream");
	res.raw().setHeader("Content-Disposition", "attachment; filename=" + fileName);
	
	try {
	    BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(res.raw().getOutputStream());
	    BufferedInputStream bufferedInputStream = new BufferedInputStream(object.getObjectContent());
	    
	    byte[] buffer = new byte[1024];
            int len;
            while ((len = bufferedInputStream.read(buffer)) > 0) {
		bufferedOutputStream.write(buffer, 0, len);
	    }
	    
	    bufferedOutputStream.flush();
            bufferedOutputStream.close();
        } catch (Exception e) {
	    halt(500, "Error serving GTFS file");
	}
	
	return res.raw();
    }
    
    public static String formatJSON(String message, int code) {
        return String.format("{\"result\":\"ERR\",\"message\":\"%s\",\"code\":%d}", message, code);
    }
}
