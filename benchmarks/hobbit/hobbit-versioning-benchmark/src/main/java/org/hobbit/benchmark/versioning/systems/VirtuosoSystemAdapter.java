/**
 * 
 */
package org.hobbit.benchmark.versioning.systems;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.io.FileUtils;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.hobbit.benchmark.versioning.util.SystemAdapterConstants;
import org.hobbit.core.components.AbstractSystemAdapter;
import org.hobbit.core.rabbit.RabbitMQUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author papv
 * 
 */
public class VirtuosoSystemAdapter extends AbstractSystemAdapter {
			
	private static final Logger LOGGER = LoggerFactory.getLogger(VirtuosoSystemAdapter.class);
	
	private AtomicInteger totalReceived = new AtomicInteger(0);
	private AtomicInteger totalSent = new AtomicInteger(0);
	private Semaphore allVersionDataReceivedMutex = new Semaphore(0);

	// used to check if bulk loading phase has finished in  order to proceed with the querying phase
	private boolean dataLoadingFinished = false;
	private int loadingNumber = 0;
	private String datasetFolderName;
	private String virtuosoContName = "localhost";
	
	private long spaceBefore = 0;
	private long spaceBefore2 = 0;

	@Override
    public void init() throws Exception {
		LOGGER.info("Initializing virtuoso test system...");
        super.init();	
        datasetFolderName = "/versioning/data/";
        File theDir = new File(datasetFolderName);
		theDir.mkdir();
		LOGGER.info("Virtuoso initialized successfully .");
		spaceBefore = Files.getFileStore(Paths.get("/")).getUsableSpace();
		spaceBefore2 = Files.getFileStore(Paths.get("/")).getUsableSpace();
    }

	/* (non-Javadoc)
	 * @see org.hobbit.core.components.TaskReceivingComponent#receiveGeneratedData(byte[])
	 */
	public void receiveGeneratedData(byte[] data) {		
		ByteBuffer dataBuffer = ByteBuffer.wrap(data);
		String fileName = RabbitMQUtils.readString(dataBuffer);

		// read the data contents
		byte[] dataContentBytes = new byte[dataBuffer.remaining()];
		dataBuffer.get(dataContentBytes, 0, dataBuffer.remaining());
		
		if (dataContentBytes.length != 0) {
			try {
				if (fileName.contains("/")) {
					fileName = fileName.replaceAll("[^/]*[/]", "");
				}
				FileUtils.writeByteArrayToFile(new File(datasetFolderName + File.separator + fileName), dataContentBytes);
			} catch (IOException e) {
				LOGGER.error("Exception while writing data file", e);
			}
		}
		
		if(totalReceived.incrementAndGet() == totalSent.get()) {
			allVersionDataReceivedMutex.release();
		}
	}

	/* (non-Javadoc)
	 * @see org.hobbit.core.components.TaskReceivingComponent#receiveGeneratedTask(java.lang.String, byte[])
	 */
	public void receiveGeneratedTask(String tId, byte[] data) {
		if(dataLoadingFinished) {
			LOGGER.info("Task " + tId + " received from task generator");
			if(tId.equals("0")) {
				try {
					long storageSpaceCost = spaceBefore2 - Files.getFileStore(Paths.get("/")).getUsableSpace();
					LOGGER.info("Overall Storage space cost: " + storageSpaceCost);
				} catch (IOException e) {
					LOGGER.error("An error occured while getting total usable space");
				}
			}
			// read the query
			ByteBuffer buffer = ByteBuffer.wrap(data);
			String queryText = RabbitMQUtils.readString(buffer);

			Query query = QueryFactory.create(queryText);
			QueryExecution qexec = QueryExecutionFactory.sparqlService("http://" + virtuosoContName + ":8890/sparql", query);
			ResultSet rs = null;

			try {
				rs = qexec.execSelect();
			} catch (Exception e) {
				LOGGER.error("Task " + tId + " failed to execute.", e);
			}
			
			ByteArrayOutputStream queryResponseBos = new ByteArrayOutputStream();
			ResultSetFormatter.outputAsJSON(queryResponseBos, rs);
			byte[] results = queryResponseBos.toByteArray();
			LOGGER.info("Task " + tId + " executed successfully.");
			qexec.close();
			
			try {
				sendResultToEvalStorage(tId, results);
				LOGGER.info("Results sent to evaluation storage.");
			} catch (IOException e) {
				LOGGER.error("Exception while sending storage space cost to evaluation storage.", e);
			}
		} 
	}
	
	private void loadVersion(String graphURI) {
		LOGGER.info("Loading data on " + graphURI + "...");
		try {
			String scriptFilePath = System.getProperty("user.dir") + File.separator + "load.sh";
			String[] command = {"/bin/bash", scriptFilePath, virtuosoContName, datasetFolderName, graphURI};
			Process p = new ProcessBuilder(command).redirectErrorStream(true).start();
			BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
			String line;
			while ((line = in.readLine()) != null) {
				LOGGER.info(line);		
			}
			p.waitFor();
			LOGGER.info(graphURI + " loaded successfully.");
			in.close();
		} catch (IOException e) {
            LOGGER.error("Exception while executing script for loading data.", e);
		} catch (InterruptedException e) {
            LOGGER.error("Exception while executing script for loading data.", e);
		}
	}
	
	@Override
    public void receiveCommand(byte command, byte[] data) {
    	if (command == SystemAdapterConstants.BULK_LOAD_DATA_GEN_FINISHED) {
    		ByteBuffer buffer = ByteBuffer.wrap(data);
            int numberOfMessages = buffer.getInt();
            boolean lastLoadingPhase = buffer.get() != 0;
   			LOGGER.info("Received signal that all data of version " + loadingNumber + " successfully sent from all data generators (#" + numberOfMessages + ")");

			// if all data have been received before BULK_LOAD_DATA_GEN_FINISHED command received
   			// release before acquire, so it can immediately proceed to bulk loading
   			if(totalReceived.get() == totalSent.addAndGet(numberOfMessages)) {
				allVersionDataReceivedMutex.release();
			}
			
			LOGGER.info("Wait for receiving all data of version " + loadingNumber + ".");
			try {
				allVersionDataReceivedMutex.acquire();
			} catch (InterruptedException e) {
				LOGGER.error("Exception while waitting for all data of version " + loadingNumber + " to be recieved.", e);
			}
			
			LOGGER.info("All data of version " + loadingNumber + " received. Proceed to the loading of such version.");
			loadVersion("http://graph.version." + loadingNumber);
			
			File theDir = new File(datasetFolderName);
			for (File f : theDir.listFiles()) {
				f.delete();
			}
			
			long storageSpaceCost = 0;
			try {
				storageSpaceCost = spaceBefore - Files.getFileStore(Paths.get("/")).getUsableSpace();
				spaceBefore = Files.getFileStore(Paths.get("/")).getUsableSpace();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			LOGGER.info("Storage space cost after loading of version " + loadingNumber +": "+ storageSpaceCost);
			
			LOGGER.info("Send signal to Benchmark Controller that all data of version " + loadingNumber + " successfully loaded.");
			try {
				sendToCmdQueue(SystemAdapterConstants.BULK_LOADING_DATA_FINISHED);
			} catch (IOException e) {
				LOGGER.error("Exception while sending signal that all data of version " + loadingNumber + " successfully loaded.", e);
			}
			
			loadingNumber++;
			dataLoadingFinished = lastLoadingPhase;
    	}
    	super.receiveCommand(command, data);
    }
	
	@Override
    public void close() throws IOException {
		LOGGER.info("Closing System Adapter...");
        // Always close the super class after yours!
        super.close();
        LOGGER.info("System Adapter closed successfully.");

    }
}