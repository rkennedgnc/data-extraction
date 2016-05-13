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

package org.gnc.marklogic.dataextraction.relational;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.gnc.marklogic.dataextraction.DataExtraction;

import org.gnc.marklogic.dataextraction.ExtractHelper;
import org.gnc.marklogic.dataextraction.LogFormatter;

import static org.gnc.marklogic.dataextraction.ExtractHelper.calcTimeTakenInMs;
import static org.gnc.marklogic.dataextraction.ExtractHelper.cleanData;
import static org.gnc.marklogic.dataextraction.ExtractHelper.cleanDataText;
import static org.gnc.marklogic.dataextraction.ExtractHelper.cleanQueryText;
import static org.gnc.marklogic.dataextraction.ExtractHelper.formatJDBCString;
import static org.gnc.marklogic.dataextraction.ExtractHelper.getColumnHeadersStr;
import static org.gnc.marklogic.dataextraction.ExtractHelper.getEstimatedCompleteDate;
import static org.gnc.marklogic.dataextraction.ExtractHelper.getLineCount;
import static org.gnc.marklogic.dataextraction.ExtractHelper.getQueryFromFile;
import static org.gnc.marklogic.dataextraction.ExtractHelper.isValidFilePath;
import static org.gnc.marklogic.dataextraction.ExtractHelper.log;
import static org.gnc.marklogic.dataextraction.ExtractHelper.validateEntry;

import org.gnc.marklogic.dataextraction.database.DAO;
import org.gnc.marklogic.dataextraction.database.DAOFactory;

/**
 *
 * @author Robert Kennedy, rkennedy@gennet.com
 *
 */
public class Relational_DataExtraction extends DataExtraction {

    // global variables
    private String SID = "";
    private String SERVICE_NAME = "";
    private String PORT = "";
    private String HOST = "";
    private String USERNAME = "";
    private String PASSWORD = "";
    private int CHUNK_SIZE = 100000000;
    private int FETCH_SIZE = 100000;
    private int OUTPUT_SIZE = 500;
    private DAO myDao = null;
    private final String DB_TYPE;
    private String LOGS_DIR;
    private String QUERY_FILE_LOC;
    private String OUTPUT_DIR;
    private String DELIMITER;
    private String DATE_FORMAT;
    private boolean NO_HEADER = false;
    private boolean MSSQL_WINDOWS_AUTH = true;
    private String MSSQL_DATABASE_NAME = "";

    private Logger LOGGER;

    private boolean started = false;
    private boolean exportComplete = false;
    private Properties props;

    public Relational_DataExtraction(Logger LOGGER, Properties props, String DB_TYPE) {
        this.LOGGER = LOGGER;
        this.props = props;
        this.DB_TYPE = DB_TYPE;
    }

    @Override
    public void run() {
        try {
            processMigration();
        } catch (InterruptedException ex) {
            ExtractHelper.log(Level.INFO, ex.getMessage(), true);
            System.exit(0);
        }
        System.exit(0);
    }

    private void processMigration() throws InterruptedException {
        try {

            // LOAD CONSTANTS
            try {
                if (DB_TYPE.equals("ORACLE")) {
                    oracleLoadConstants(props);
                } else if (DB_TYPE.equals("MSSQL")) {
                    mssqlLoadConstants(props);
                }
            } catch (NullPointerException nex) {
                log(Level.SEVERE, "UNABLE TO LOAD CONSTANTS FROM SETTINGS FILE. PLEASE CHECK SETTINGS ARE LABELED CORRECTLY AND MANDATORY SETTINGS ARE FILLED IN", true);
                System.exit(1);
            }

            // START LOGGING
            LOGGER = Logger.getLogger("MAIN_LOG");
            String logFilePath = LOGS_DIR + "Log_DataMigration_" + new SimpleDateFormat("YYYY-MM-dd_HH-mm-ss").format(new Date()) + ".txt";
            File logDir = new File(LOGS_DIR);

            // IF LOG DIR DOES NOT EXIST -> CREATE IT
            if (!logDir.exists()) {
                logDir.mkdir();
            }
            FileHandler fh = new FileHandler(logFilePath);
            fh.setFormatter(new LogFormatter());
            LOGGER.addHandler(fh);
            LOGGER.setLevel(Level.INFO);
            LOGGER.setUseParentHandlers(false);
            log(Level.INFO, "STARTING NEW " + DB_TYPE + " DATA EXTRACTION", true);

            // INIT VARIABLES
            String entry, fileName, query, secondComp;
            String countQuery = null;
            //ResultSet rs = null;
            long rowCount, currLineCount, totalLineCount;
            String[] data;
            BufferedReader in;

            // GET DB CONNECTION
            DAOFactory daoFac = DAOFactory.getInstance();

            if (DB_TYPE.equals("ORACLE")) {
                myDao = daoFac.getDAO(DB_TYPE);
                String formattedJDBCStr = formatJDBCString(SID, SERVICE_NAME, HOST, PORT);
                myDao.openConnection(formattedJDBCStr, USERNAME, PASSWORD);
            } else if (DB_TYPE.equals("MSSQL")) {
                myDao = daoFac.getDAO(DB_TYPE);
                // Determines if Windows Authentication or Server Authentication will be used to connect
                if (MSSQL_WINDOWS_AUTH) {
                    String url = "jdbc:sqlserver://";
                    String selectMethod = "cursor";
                    String connStr = url + HOST + ":" + PORT + ";integratedSecurity=" + MSSQL_WINDOWS_AUTH + ";databaseName=" + MSSQL_DATABASE_NAME + ";selectMethod=" + selectMethod + ";";
                    myDao.openConnection(connStr);
                } else {
                    String connStr = "jdbc:sqlserver://" + HOST + ";integratedSecurity=false;port=" + PORT;
                    myDao.openConnection(connStr, USERNAME, PASSWORD);
                }
            } else {
                ExtractHelper.log(Level.SEVERE, "Invalid DB TYPE Specified. Exiting", false);
                System.exit(-1);
            }

            Connection conn = myDao.getConnection();
            PreparedStatement pstmt;

            // LOOP THROUGH ENTRIES IN QUERY FILE
            totalLineCount = getLineCount(QUERY_FILE_LOC);
            in = new BufferedReader(new FileReader(QUERY_FILE_LOC));
            currLineCount = 0;
            while (((entry = in.readLine()) != null)) {
                try {
                    currLineCount++;
                    log(Level.INFO, "Processing Query " + currLineCount + "/" + totalLineCount + "", true);
                    // CHECK FOR VALID INPUT
                    boolean isValidEntry = validateEntry(entry);
                    if (isValidEntry) {

                        // GET DATA FROM ENTRY LINE
                        data = entry.split("###");
                        fileName = data[0];
                        secondComp = data[1];
                        if (data.length == 3) {
                            countQuery = cleanQueryText(data[2]);
                        }

                        // IF SECOND COMPONENT IS A TXT FILE URI -> READ QUERY FROM FILE
                        if (isValidFilePath(secondComp)) {
                            query = getQueryFromFile(secondComp);
                            log(Level.INFO, "Query Taken From File  " + query, false);
                        } else {
                            query = cleanQueryText(secondComp);
                        }

                        // GET ROW COUNT FROM TABLE
                        rowCount = getRowCount(countQuery);

                        // EXECUTE QUERY
                        pstmt = conn.prepareStatement(query);
                        pstmt.setFetchSize(FETCH_SIZE);

                        log(Level.INFO, "Running query: " + query, true);
                        if (rowCount != 0) {
                            log(Level.INFO, "Row count is: " + rowCount, false);
                        }

                        // WRITE OUT DATA TO DISK
                        this.outputToFile(pstmt, fileName, rowCount, CHUNK_SIZE);
                        pstmt.close();
                    }
                } catch (SQLException | IOException ex) {
                    log(Level.SEVERE, "Skipping line " + entry + " in " + QUERY_FILE_LOC + " due to exception: " + ex.getMessage() + "", true);
                }
            }

            // CLOSE CONNECTIONS
            try {
                in.close();
            } catch (IOException | NullPointerException ex) {
            } finally {
                myDao.closeConnection();
            }
            log(Level.INFO, "DATA EXPORT COMPLETE", true);
            setExportComplete(true);
        } catch (IOException | SecurityException | SQLException | NullPointerException ex) {
            log(Level.SEVERE, "PROCESS FAILED: " + ex + " " + ex.getMessage(), true);
        }
    }
    /*
     * Outputs a a resultset of data (Table) to file of given filename
     */

    private void outputToFile(PreparedStatement pstmt, String fileName, long rowCount, int chunkSize) throws IOException, SQLException, InterruptedException {

        long queryStartTime = new Date().getTime();
        ResultSet rs = pstmt.executeQuery();

        // GET COL HEADERS
        ResultSetMetaData rsmd = rs.getMetaData();
        int colCount = rsmd.getColumnCount();

        String columnHeaders = getColumnHeadersStr(rsmd, DELIMITER);

        boolean newFile = true;

        // CREATE INITIAL FILE
        File outputDir = new File(OUTPUT_DIR);

        // IF OUTPUT DIR DOES NOT EXIST -> CREATE IT
        if (!outputDir.exists()) {
            outputDir.mkdir();
        }

        PrintStream out = new PrintStream(OUTPUT_DIR + fileName + ".1.dsv");
        if (!NO_HEADER) {
            out.print(columnHeaders);
        } else {
            newFile = true;
        }
        // INIT VARIABLES
        long rowsProcessed = 0;
        long chunkCount = 1;

        String dataToWrite = "", columnType, row;
        Object dataObj;
        long clobLength = 0;
        String clobStr = "";
        Blob blob = null;
        DateFormat outputDateFormat = new SimpleDateFormat(DATE_FORMAT);
        java.sql.Date formattedDate;

        // LOOP THROUGH QUERY RESULTS, WRITING ROWS TO DISK
        while (rs.next()) {

            // CREATE NEW OUTPUT FILE EVERYTIME CHUNK SIZE IS REACHED
            if (rowsProcessed > 0 && rowsProcessed % chunkSize == 0) {

                // TIME ESTIMATE
                if (rowCount != 0) {
                    System.out.println("Current estimated completion time: " + getEstimatedCompleteDate(queryStartTime, rowsProcessed, rowCount));
                } else {
                    System.out.println("Estimated completion time unavailable for this query");
                }
                // WRITE OUT REMAINING ROWS AND CLOSE CHUNK FILE
                out.print(dataToWrite);
                dataToWrite = "";
                try {
                    out.close();
                } catch (Exception ex) {
                }

                log(Level.INFO, "Chunk file " + fileName + "." + chunkCount + " complete.", false);
                chunkCount++;

                out = new PrintStream(OUTPUT_DIR + fileName + "." + chunkCount + ".dsv");
                if (!NO_HEADER) {
                    out.print(columnHeaders);
                } else {
                    newFile = true;
                }
                // LOG PERCENT COMPLETE AND EST TIME LEFT
                if (rowCount != 0) {
                    System.out.println("Current estimated completion time: " + getEstimatedCompleteDate(queryStartTime, rowsProcessed, rowCount));
                }
            }
            row = "";

            // LOOP THROUGH COLUMNS IN ROW
            for (int i = 1; i <= colCount; i++) {

                dataObj = rs.getObject(i);

                if (DB_TYPE.equals("MSSQL")) {
                    if (dataObj != null && !dataObj.equals("null") && !dataObj.equals("")) {
                        columnType = rsmd.getColumnTypeName(i);
                        //  System.out.println("Data Types" + columnType);
                        if (columnType.toUpperCase().contains("CHAR")) {
                            dataObj = cleanData(dataObj.toString(), DELIMITER);
                            row += "\"" + dataObj + "\"";
                        } else if (columnType.toUpperCase().contains("TEXT")) {
                            dataObj = cleanDataText(dataObj.toString(), DELIMITER);
                            row += "\"" + dataObj + "\"";
                        } else if (columnType.toUpperCase().equals("DATETIME")) {
                            formattedDate = new java.sql.Date(rs.getTimestamp(i).getTime());
                            row += outputDateFormat.format(formattedDate);
                        } else {
                            row += dataObj;
                        }
                    }

                } else if (DB_TYPE.equals("ORACLE")) {
                    if (dataObj != null && !dataObj.equals("null") && !dataObj.equals("")) {
                        columnType = rsmd.getColumnTypeName(i);

                        if (columnType.toUpperCase().contains("CHAR")) {
                            dataObj = cleanData(dataObj.toString(), DELIMITER);
                            row += "\"" + dataObj + "\"";

                        } else if (columnType.toUpperCase().equals("TIMESTAMP") || columnType.equals("DATE")) {
                            formattedDate = new java.sql.Date(rs.getTimestamp(i).getTime());
                            row += outputDateFormat.format(formattedDate);

                        } else if (columnType.toUpperCase().equals("CLOB")) {
                            java.sql.Clob myClob = rs.getClob(rsmd.getColumnLabel(i));
                            clobLength = myClob.length();
                            clobStr = myClob.getSubString(1, (int) clobLength);
                            clobStr = cleanData(clobStr, DELIMITER);
                            row += "\"" + clobStr + "\"";

                        } else if (columnType.toUpperCase().equals("BLOB")) {
                            blob = rs.getBlob(rsmd.getColumnLabel(i));
                            row += "\"" + Arrays.toString(blob.getBytes(1, (int) blob.length())) + "\"";
                        } else {
                            row += dataObj;
                        }
                    }
                }

                // APPEND PIPE AFTER EACH COLUMN EXCEPT AT END OF ROW
                if (i < colCount) {
                    row += DELIMITER;
                }

            }

            // SUPPORT NO HEADERS  
            try {
                if (NO_HEADER) {
                    if (newFile) {
                        dataToWrite += row;
                        // will reset newFile to false - used to exclude "\r\n"
                        newFile = false;
                    } else {
                        dataToWrite += "\r\n" + row;
                    }
                } else {
                    // normal pipe-delmited processing with Header
                    dataToWrite += "\r\n" + row;
                }
            } catch (Exception ex) {
                System.out.println(ex.toString());
                log(Level.INFO, "Error " + ex.toString(), false);
            }

            if (rowsProcessed % OUTPUT_SIZE == 0) {
                out.print(dataToWrite);
                dataToWrite = "";
            }

            rowsProcessed++;
        }

        // WRITE ANY REMAINING OUTPUT
        if (!dataToWrite.trim().equals("")) {
            out.print(dataToWrite);
            log(Level.INFO, "Chunk file " + fileName + "." + chunkCount + " complete (final partial file).", false);
        }
        try {
            out.close();
        } catch (Exception ex) {

        }
        log(Level.INFO, rowsProcessed + " rows written to disk", false);

        // LOG TOTAL TIME TAKEN
        long queryEndTime = new Date().getTime();
        long timeTaken = calcTimeTakenInMs(queryEndTime, queryStartTime);
        String detail = ExtractHelper.convertMilisecondsToDetail(timeTaken);
        log(Level.INFO, "Data Export Completed in " + detail + " ", true);

        try {
            rs.close();
        } catch (Exception ex) {
        }
    }

    @Override
    public void exitImmediately(String MSG) throws InterruptedException {
        try {
            myDao.closeConnection();
        } catch (Exception ex) {
        }
        ExtractHelper.log(Level.SEVERE, MSG, false);
        System.exit(0);
    }


    /*
     * read constants in from settings file
     */
    private void oracleLoadConstants(Properties props) throws IOException {
        HOST = props.getProperty("ORACLE_HOST");
        PORT = props.getProperty("ORACLE_PORT");
        SID = props.getProperty("ORACLE_SID");
        SERVICE_NAME = props.getProperty("ORACLE_SERVICE_NAME");
        PASSWORD = props.getProperty("ORACLE_PASSWORD");
        USERNAME = props.getProperty("ORACLE_USERNAME");
        FETCH_SIZE = Integer.parseInt(props.getProperty("ORACLE_FETCH_SIZE"));
        OUTPUT_SIZE = Integer.parseInt(props.getProperty("ORACLE_OUTPUT_SIZE"));
        CHUNK_SIZE = Integer.parseInt(props.getProperty("ORACLE_CHUNK_SIZE"));
        QUERY_FILE_LOC = props.getProperty("ORACLE_QUERY_FILE_PATH");
        LOGS_DIR = props.getProperty("ORACLE_LOGS_DIR", "");
        OUTPUT_DIR = props.getProperty("ORACLE_OUTPUT_DIR", "");
        DELIMITER = props.getProperty("DELIMITER", "|");
        NO_HEADER = Boolean.parseBoolean(props.getProperty("ORACLE_NO_HEADER")); // Javier 2/11/2016 
        DATE_FORMAT = props.getProperty("ORACLE_DATE_FORMAT", "YYYY-MM-dd'T'HH:mm:ss");
    }

    /*
     * read constants in from settings file
     */
    private void mssqlLoadConstants(Properties props) throws IOException {
        HOST = props.getProperty("MSSQL_HOST");
        PORT = props.getProperty("MSSQL_PORT");
        SID = props.getProperty("MSSQL_SID");
        SERVICE_NAME = props.getProperty("MSSQL_SERVICE_NAME");
        PASSWORD = props.getProperty("MSSQL_PASSWORD");
        USERNAME = props.getProperty("MSSQL_USERNAME");
        FETCH_SIZE = Integer.parseInt(props.getProperty("MSSQL_FETCH_SIZE"));
        OUTPUT_SIZE = Integer.parseInt(props.getProperty("MSSQL_OUTPUT_SIZE"));
        CHUNK_SIZE = Integer.parseInt(props.getProperty("MSSQL_CHUNK_SIZE"));
        QUERY_FILE_LOC = props.getProperty("MSSQL_QUERY_FILE_PATH");
        LOGS_DIR = props.getProperty("MSSQL_LOGS_DIR", "");
        OUTPUT_DIR = props.getProperty("MSSQL_OUTPUT_DIR", "");
        DELIMITER = props.getProperty("DELIMITER", "|");
        NO_HEADER = Boolean.parseBoolean(props.getProperty("MSSQL_NO_HEADER"));
        DATE_FORMAT = props.getProperty("MSSQL_DATE_FORMAT", "YYYY-MM-dd'T'HH:mm:ss");
        MSSQL_WINDOWS_AUTH = Boolean.parseBoolean(props.getProperty("MSSQL_WINDOWS_AUTH"));
        MSSQL_DATABASE_NAME = props.getProperty("MSSQL_DATABASE_NAME");
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

    // get count given a query
    private long getRowCount(String countQuery) {
        long count = 0;
        if (countQuery != null && !countQuery.trim().equals("")) {
            try {
                Statement st = myDao.getConnection().createStatement();
                ResultSet rs = st.executeQuery(countQuery);
                if (rs.next()) {
                    count = rs.getInt(1);
                }
                st.close();
                rs.close();
            } catch (Exception ex) {
            }
        }

        return count;
    }

}
