/*
 * Copyright (c)2016 General Networks Corporation 
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
 * 
 * http://www.apache.org/licenses/LICENSE-2.0 
 * 
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License. 
 * 
 * The use of the Apache License does not indicate that this project is 
 * affiliated with the Apache Software Foundation. 
 */

package org.gnc.marklogic.dataextraction.marklogic;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.DatabaseClientFactory;
import com.marklogic.client.DatabaseClientFactory.Authentication;
import com.marklogic.client.document.DocumentPage;
import com.marklogic.client.document.XMLDocumentManager;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.query.QueryManager;
import com.marklogic.client.query.RawCombinedQueryDefinition;
import com.marklogic.client.query.RawQueryByExampleDefinition;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.gnc.marklogic.dataextraction.DataExtraction;
import org.gnc.marklogic.dataextraction.ExtractHelper;
import org.gnc.marklogic.dataextraction.LogFormatter;
import static org.gnc.marklogic.dataextraction.ExtractHelper.extractFileToList;
import static org.gnc.marklogic.dataextraction.ExtractHelper.getColumnHeadersStr;
import static org.gnc.marklogic.dataextraction.ExtractHelper.log;

/**
 *
 * @author Robert Kennedy, rkennedy@gennet.com
 *
 */
public class Marklogic_DataExtraction extends DataExtraction {

    // global variables
    private int MARKLOGIC_PORT;
    private String MARKLOGIC_HOST;
    private String MARKLOGIC_USERNAME;
    private String MARKLOGIC_PASSWORD;
    private int MARKLOGIC_PAGE_SIZE;
    private String MARKLOGIC_OUTPUT_FILE_PATH;
    private String MARKLOGIC_ELEMENTS_FILE;
    private String MARKLOGIC_FILTERS_FILE;
    private String MARKLOGIC_NAME;
    private String MARKLOGIC_ROOT_NODE;
    private String DELIMITER;
    private String MARKLOGIC_LOG_DIR;
    private DatabaseClient mlClient;

    private Logger LOGGER;  //Remove after - was Static

    private boolean started = false;
    private boolean exportComplete = false;
    private final Properties props;

    public Marklogic_DataExtraction(Logger LOGGER, Properties props) {
        this.LOGGER = LOGGER;
        this.props = props;
    }

    @Override
    public void run() {
        try {
            processMigration();
        } catch (InterruptedException | IOException ex) {
            ExtractHelper.log(Level.INFO, ex.getMessage(), true);
            Logger.getLogger(Marklogic_DataExtraction.class.getName()).log(Level.SEVERE, null, ex);
        }
        System.exit(0);
    }

    private void processMigration() throws InterruptedException, IOException {

        // LOAD CONSTANTS
        try {
            loadConstants(props);
        } catch (NullPointerException nex) {
            log(Level.SEVERE, "UNABLE TO LOAD CONSTANTS FROM SETTINGS FILE. PLEASE CHECK SETTINGS ARE LABELED CORRECTLY AND MANDATORY SETTINGS ARE FILLED IN", true);
            System.exit(1);
        }

        // START LOGGING
        LOGGER = Logger.getLogger("MAIN_LOG");
        File logDir = new File(MARKLOGIC_LOG_DIR);

        String logFilePath;
        if (MARKLOGIC_LOG_DIR.toLowerCase().endsWith(".txt")) {
            logFilePath = MARKLOGIC_LOG_DIR;
        } else {
            logFilePath = MARKLOGIC_LOG_DIR + "Log_DataMigration_" + new SimpleDateFormat("YYYY-MM-dd_HH-mm-ss").format(new Date()) + ".txt";
        }
        
        
        // IF LOG DIR DOES NOT EXIST -> CREATE IT
        if (!logDir.exists()) {
            logDir.mkdir();
        }
        FileHandler fh = new FileHandler(logFilePath);
        fh.setFormatter(new LogFormatter());
        LOGGER.addHandler(fh);
        LOGGER.setLevel(Level.INFO);
        LOGGER.setUseParentHandlers(false);
        log(Level.INFO, "STARTING NEW MARKLOGIC DATA EXTRACTION", true);

        // CREATE THREADPOOL
        int numThreads = Runtime.getRuntime().availableProcessors();
        //int numThreads = 1;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        // GET DB CONNECTION
        mlClient = DatabaseClientFactory.newClient(MARKLOGIC_HOST, MARKLOGIC_PORT, MARKLOGIC_NAME, MARKLOGIC_USERNAME, MARKLOGIC_PASSWORD, Authentication.DIGEST);
        XMLDocumentManager docMgr = mlClient.newXMLDocumentManager();
        docMgr.setPageLength(MARKLOGIC_PAGE_SIZE);

        log(Level.INFO, "Connected to ML on port " + MARKLOGIC_PORT, true);

        // READ IN ELEMENTS
        ArrayList<String> elements = extractFileToList(MARKLOGIC_ELEMENTS_FILE);
        log(Level.INFO, "Elements to extract: " + elements, true);

        // CREATE OUTPUT FILE
        PrintStream out = new PrintStream(MARKLOGIC_OUTPUT_FILE_PATH, "UTF-8");

        // PRINT OUT HEADERS
        String headers = getColumnHeadersStr(elements, DELIMITER);

        out.println(headers);

        // GET QUERY DEF
        RawCombinedQueryDefinition combinedquerydef = generateQuery(mlClient);

        // INIT VARIABLES
        int start = 1;
        int docsProcessed = 0;
        long totalDocs = 0;
        DocumentPage documentPage = null;
        Marklogic_ProcessPage processPage;

        // START SEARCHING
        do {
            documentPage = docMgr.search(combinedquerydef, start);

            // GET TOTAL COUNT ON FIRST PASS
            if (start == 1) {
                totalDocs = documentPage.getTotalSize();
                log(Level.INFO, "Found: " + totalDocs + " total documents", true);
            }

            processPage = new Marklogic_ProcessPage(documentPage, elements, out, DELIMITER, docMgr, MARKLOGIC_ROOT_NODE);
            Runnable worker = processPage;
            executor.execute(worker);

            // PRINT UPDATE OF PROGRESS
            //docsProcessed++;
            docsProcessed += documentPage.getPageSize();
            //if (docsProcessed % PAGE_SIZE == 0) {
            if (docsProcessed < totalDocs) {
                log(Level.INFO, "Processed: " + docsProcessed + " of: " + totalDocs, true);
            }
            //}

            start += documentPage.getPageSize();

        } while (documentPage.hasNextPage());

        executor.shutdown();
        // wait until all threads are finish
        while (!executor.isTerminated()) {
        }

        mlClient.release();

        log(Level.INFO, "Extraction complete", true);
    }

    // GENERATE QUERY
    private RawCombinedQueryDefinition generateQuery(DatabaseClient mlClient) {
        QueryManager queryMgr = mlClient.newQueryManager();

        String filters = ExtractHelper.extractFileToString(MARKLOGIC_FILTERS_FILE);
        String rawXMLQuery
                = "<q:qbe xmlns:q='http://marklogic.com/appservices/querybyexample'>"
                + "<q:query>"
                + filters
                + "</q:query>"
                + "</q:qbe>";

        if (filters.length() > 0) {
            log(Level.INFO, "Using filters " + filters, true);
        }

        StringHandle rawHandle = new StringHandle(rawXMLQuery);
        RawQueryByExampleDefinition querydef = queryMgr.newRawQueryByExampleDefinition(rawHandle);

        // CONVERT TO COMBINED QUERY
        StringHandle combinedQueryHandle = queryMgr.convert(querydef, new StringHandle());

        return queryMgr.newRawCombinedQueryDefinition(combinedQueryHandle);
    }

    /*
     * read constants in from settings file
     */
    private void loadConstants(Properties props) throws IOException {
        MARKLOGIC_PASSWORD = props.getProperty("MARKLOGIC_PASSWORD");
        MARKLOGIC_USERNAME = props.getProperty("MARKLOGIC_USERNAME");
        MARKLOGIC_HOST = props.getProperty("MARKLOGIC_HOST");
        try {
            MARKLOGIC_PORT = Integer.parseInt(props.getProperty("MARKLOGIC_PORT"));
        } catch (NumberFormatException nex) {
            MARKLOGIC_PORT = 8000;
        }

        MARKLOGIC_NAME = props.getProperty("MARKLOGIC_DB_NAME");
        DELIMITER = props.getProperty("DELIMITER", "|");
        MARKLOGIC_ROOT_NODE = props.getProperty("MARKLOGIC_ROOT_NODE");

        try {
            MARKLOGIC_PAGE_SIZE = Integer.parseInt(props.getProperty("MARKLOGIC_PAGE_SIZE"));
        } catch (NumberFormatException nex) {
            MARKLOGIC_PAGE_SIZE = 10000;
        }

        MARKLOGIC_OUTPUT_FILE_PATH = props.getProperty("MARKLOGIC_OUTPUT_PATH").trim();
        MARKLOGIC_FILTERS_FILE = props.getProperty("MARKLOGIC_FILTERS_FILE_PATH").trim();
        MARKLOGIC_LOG_DIR = props.getProperty("MARKLOGIC_LOGS_DIR").trim();
        MARKLOGIC_ELEMENTS_FILE = props.getProperty("MARKLOGIC_ELEMENTS_FILE_PATH").trim();
    }

    /**
     * @return the exportComplete
     */
    public boolean isExportComplete() {
        return exportComplete;
    }

    /**
     * @param exportComplete the exportComplete to set
     */
    public void setExportComplete(boolean exportComplete) {
        this.exportComplete = exportComplete;
    }

    /**
     * @return the started
     */
    public boolean isStarted() {
        return started;
    }

    /**
     * @param started the started to set
     */
    public void setStarted(boolean started) {
        this.started = started;
    }

    @Override
    public void exitImmediately(String MSG) throws InterruptedException {
        try {
            mlClient.release();
        } catch (Exception ex) {
        }
        ExtractHelper.log(Level.SEVERE, MSG, false);
        System.exit(-1);
    }

}
