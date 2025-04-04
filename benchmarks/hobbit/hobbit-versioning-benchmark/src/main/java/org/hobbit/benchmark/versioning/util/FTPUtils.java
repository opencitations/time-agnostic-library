package org.hobbit.benchmark.versioning.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.commons.net.ftp.FTPClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FTPUtils {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(FTPUtils.class);

	public static void sendToFtp(String inputDir, String outputDir, String fileExtention) {
		FTPClient client = new FTPClient();
		FileInputStream fis = null;
		
        try {
        	client.connect("hobbitdata.informatik.uni-leipzig.de");
        	LOGGER.info("connected: " + client.sendNoOp());
        	client.enterLocalPassiveMode();
        	String username = "****";
            String pwd = "****";
            LOGGER.info("login: " + client.login(username, pwd));
            
            LOGGER.info(client.printWorkingDirectory());
            // recursively create ftp folders if not exist
            boolean dirExists = true;
            String[] directories = outputDir.split("/");
            for (String dir : directories ) {
            	if (!dir.isEmpty() ) {
            		if (dirExists) {
            			dirExists = client.changeWorkingDirectory(dir);
            		}
	            	if (!dirExists) {
	                    if (!client.makeDirectory(dir)) {
	                    	throw new IOException("Unable to create remote directory '" + dir + "'.  error='" + client.getReplyString()+"'");
	                    }
	                    if (!client.changeWorkingDirectory(dir)) {
	                    	throw new IOException("Unable to change into newly created remote directory '" + dir + "'.  error='" + client.getReplyString()+"'");
	                    }
	            	}
            	}
            }

            LOGGER.info(client.printWorkingDirectory());
            File inputDirFile = new File(inputDir);
    		List<File> inputFiles = (List<File>) FileUtils.listFiles(inputDirFile, new String[] { fileExtention }, true);
            for (File file : inputFiles) {
            	fis = new FileInputStream(file);
            	client.deleteFile(file.getAbsolutePath());
            	client.storeFile(file.getName(), fis);
            }
            client.logout();
        } catch (IOException e) {
			e.printStackTrace();
        } finally {
        	try {
                if (fis != null) {
                    fis.close();
                }
                client.disconnect();
            } catch (IOException e) {
    			e.printStackTrace();
            }
        }
	}
}
