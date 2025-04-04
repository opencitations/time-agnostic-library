package org.hobbit.benchmark.versioning.systems;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
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
 * System adapter for the Time-Agnostic RDF Query System.
 * This adapter implements the interface required by the HOBBIT Versioning Benchmark.
 */
public class TimeAgnosticSystemAdapter extends AbstractSystemAdapter {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(TimeAgnosticSystemAdapter.class);
    
    private AtomicInteger totalReceived = new AtomicInteger(0);
    private AtomicInteger totalSent = new AtomicInteger(0);
    private Semaphore allVersionDataReceivedMutex = new Semaphore(0);

    // Used to check if bulk loading phase has finished in order to proceed with the querying phase
    private boolean dataLoadingFinished = false;
    private int loadingNumber = 0;
    private String datasetFolderName;
    private String systemEndpoint = "http://localhost:8080/sparql"; // Update with your system's endpoint
    
    private long spaceBefore = 0;
    private long spaceBefore2 = 0;

    @Override
    public void init() throws Exception {
        LOGGER.info("Initializing Time-Agnostic RDF Query System adapter...");
        super.init();   
        
        // Create data directory
        datasetFolderName = "/versioning/data/";
        File dataDir = new File(datasetFolderName);
        dataDir.mkdirs();
        
        // Start your time-agnostic RDF system here
        // This could include launching a process, Docker container, or configuring an API client
        startTimeAgnosticSystem();
        
        LOGGER.info("Time-Agnostic RDF Query System adapter initialized successfully.");
        spaceBefore = Files.getFileStore(Paths.get("/")).getUsableSpace();
        spaceBefore2 = Files.getFileStore(Paths.get("/")).getUsableSpace();
    }
    
    /**
     * Start the time-agnostic RDF query system.
     * Implement this method based on how your system should be started.
     */
    private void startTimeAgnosticSystem() {
        try {
            // Example implementation - replace with your system's setup logic
            LOGGER.info("Starting Time-Agnostic RDF Query System...");
            
            // Option 1: Run as a system command
            // ProcessBuilder pb = new ProcessBuilder("your-system-start-command", "--param", "value");
            // Process process = pb.start();
            
            // Option 2: Configure a client for an already running system
            // Connect to your system's API or configure client libraries
            
            LOGGER.info("Time-Agnostic RDF Query System started successfully.");
        } catch (Exception e) {
            LOGGER.error("Failed to start Time-Agnostic RDF Query System", e);
        }
    }

    @Override
    public void receiveGeneratedData(byte[] data) {       
        ByteBuffer dataBuffer = ByteBuffer.wrap(data);
        String fileName = RabbitMQUtils.readString(dataBuffer);

        // Read the data contents
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

    @Override
    public void receiveGeneratedTask(String taskId, byte[] data) {
        if(dataLoadingFinished) {
            LOGGER.info("Task " + taskId + " received from task generator");
            if(taskId.equals("0")) {
                try {
                    long storageSpaceCost = spaceBefore2 - Files.getFileStore(Paths.get("/")).getUsableSpace();
                    LOGGER.info("Overall Storage space cost: " + storageSpaceCost);
                } catch (IOException e) {
                    LOGGER.error("An error occurred while getting total usable space", e);
                }
            }
            
            // Read the query
            ByteBuffer buffer = ByteBuffer.wrap(data);
            String queryText = RabbitMQUtils.readString(buffer);
            LOGGER.info("Executing query: " + queryText);

            // Execute the query on your time-agnostic system
            ByteArrayOutputStream queryResponseBos = new ByteArrayOutputStream();
            try {
                // Adapt this to use your system's query execution logic
                executeQuery(queryText, queryResponseBos);
                byte[] results = queryResponseBos.toByteArray();
                
                LOGGER.info("Task " + taskId + " executed successfully.");
                
                sendResultToEvalStorage(taskId, results);
                LOGGER.info("Results sent to evaluation storage.");
            } catch (Exception e) {
                LOGGER.error("Task " + taskId + " failed to execute.", e);
                
                // Send empty results in case of failure
                try {
                    sendResultToEvalStorage(taskId, new byte[0]);
                } catch (IOException ioe) {
                    LOGGER.error("Error sending empty results for failed task", ioe);
                }
            }
        }
    }
    
    /**
     * Execute a SPARQL query on the time-agnostic system.
     * Implement this method to use your system's query execution logic.
     * 
     * @param queryText The SPARQL query text
     * @param outputStream Stream to write the JSON results to
     */
    private void executeQuery(String queryText, ByteArrayOutputStream outputStream) {
        // Modify this to use your time-agnostic system's query API
        try {
            // Example using Apache Jena (replace with your system's API)
            Query query = QueryFactory.create(queryText);
            QueryExecution qexec = QueryExecutionFactory.sparqlService(systemEndpoint, query);
            ResultSet rs = qexec.execSelect();
            ResultSetFormatter.outputAsJSON(outputStream, rs);
            qexec.close();
        } catch (Exception e) {
            LOGGER.error("Error executing query", e);
            throw e;
        }
    }
    
    private void loadVersion(String graphURI) {
        LOGGER.info("Loading data into version " + graphURI + "...");
        try {
            // Implement your system's version loading logic here
            // This should load the data from datasetFolderName into your time-agnostic system
            // with the appropriate versioning information
            
            // Example implementation:
            // 1. Get all RDF files in the data folder
            File dataDir = new File(datasetFolderName);
            File[] rdfFiles = dataDir.listFiles((dir, name) -> name.endsWith(".nt") || name.endsWith(".ttl"));
            
            if (rdfFiles != null) {
                for (File rdfFile : rdfFiles) {
                    // 2. Load each file into your system, associating it with the given graphURI/version
                    loadRdfFileToSystem(rdfFile, graphURI);
                }
            }
            
            LOGGER.info(graphURI + " loaded successfully.");
        } catch (Exception e) {
            LOGGER.error("Exception while loading data for version " + graphURI, e);
        }
    }
    
    /**
     * Load an RDF file into your time-agnostic system.
     * Implement this method based on your system's data loading API.
     * 
     * @param rdfFile The RDF file to load
     * @param graphUri The graph/version URI
     */
    private void loadRdfFileToSystem(File rdfFile, String graphUri) {
        try {
            // Example implementation - replace with your system's data loading logic
            LOGGER.info("Loading file " + rdfFile.getName() + " into version " + graphUri);
            
            // Option 1: Using system command
            // ProcessBuilder pb = new ProcessBuilder(
            //     "your-system-import-command",
            //     "--input", rdfFile.getAbsolutePath(),
            //     "--graph", graphUri
            // );
            // Process process = pb.start();
            // process.waitFor();
            
            // Option 2: Using client API
            // YourSystemClient client = new YourSystemClient(systemEndpoint);
            // client.importRdfFile(rdfFile, graphUri);
            
        } catch (Exception e) {
            LOGGER.error("Error loading RDF file " + rdfFile.getName(), e);
        }
    }
    
    @Override
    public void receiveCommand(byte command, byte[] data) {
        if (command == SystemAdapterConstants.BULK_LOAD_DATA_GEN_FINISHED) {
            ByteBuffer buffer = ByteBuffer.wrap(data);
            int numberOfMessages = buffer.getInt();
            boolean lastLoadingPhase = buffer.get() != 0;
            LOGGER.info("Received signal that all data of version " + loadingNumber + 
                        " successfully sent from all data generators (#" + numberOfMessages + ")");

            // If all data have been received before BULK_LOAD_DATA_GEN_FINISHED command received
            // release before acquire, so it can immediately proceed to bulk loading
            if(totalReceived.get() == totalSent.addAndGet(numberOfMessages)) {
                allVersionDataReceivedMutex.release();
            }
            
            LOGGER.info("Wait for receiving all data of version " + loadingNumber + ".");
            try {
                allVersionDataReceivedMutex.acquire();
            } catch (InterruptedException e) {
                LOGGER.error("Exception while waiting for all data of version " + loadingNumber + " to be received.", e);
            }
            
            LOGGER.info("All data of version " + loadingNumber + " received. Proceed to the loading of such version.");
            loadVersion("http://graph.version." + loadingNumber);
            
            // Clean up data files after loading
            File theDir = new File(datasetFolderName);
            for (File f : theDir.listFiles()) {
                f.delete();
            }
            
            // Calculate storage space usage
            long storageSpaceCost = 0;
            try {
                storageSpaceCost = spaceBefore - Files.getFileStore(Paths.get("/")).getUsableSpace();
                spaceBefore = Files.getFileStore(Paths.get("/")).getUsableSpace();
            } catch (IOException e1) {
                LOGGER.error("Error calculating storage space", e1);
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
        LOGGER.info("Closing Time-Agnostic RDF Query System Adapter...");
        
        // Add shutdown logic for your time-agnostic system here
        // Example:
        // stopTimeAgnosticSystem();
        
        // Always close the super class after yours!
        super.close();
        LOGGER.info("Time-Agnostic RDF Query System Adapter closed successfully.");
    }
    
    /**
     * Stop the time-agnostic RDF query system.
     * Implement this method based on how your system should be stopped.
     */
    private void stopTimeAgnosticSystem() {
        try {
            // Example implementation - replace with your system's shutdown logic
            LOGGER.info("Stopping Time-Agnostic RDF Query System...");
            
            // Option 1: Run as a system command
            // ProcessBuilder pb = new ProcessBuilder("your-system-stop-command");
            // Process process = pb.start();
            // process.waitFor();
            
            // Option 2: Use API to shutdown
            // YourSystemClient client = new YourSystemClient(systemEndpoint);
            // client.shutdown();
            
            LOGGER.info("Time-Agnostic RDF Query System stopped successfully.");
        } catch (Exception e) {
            LOGGER.error("Failed to stop Time-Agnostic RDF Query System", e);
        }
    }
} 