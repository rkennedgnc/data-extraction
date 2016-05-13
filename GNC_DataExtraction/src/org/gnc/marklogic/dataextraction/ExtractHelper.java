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

package org.gnc.marklogic.dataextraction;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.activation.UnsupportedDataTypeException;

/**
 *
 * @author Robert Kennedy, rkennedy@gennet.com
 */
public class ExtractHelper {

    public static String cleanQueryText(String query) {
        if (query.endsWith(";")) {
            query = query.replace(";", "");
        }
        // replace any areas of whitespace with single whitespace
        query = query.replaceAll("[\\s&&[^\\n]]+", " ");
        //remove /* comments
        Pattern commentPattern = Pattern.compile("/\\*.*?\\*/", Pattern.DOTALL);
        query = commentPattern.matcher(query).replaceAll("");
        return query;
    }

    public static double calcPercentCompleted(long numProcessed, long total) {
        double percentCompleted = ((double) numProcessed * 100) / (double) total;
        return percentCompleted;
    }

    public static long estCompletedTimeInMs(long startTimeInMs, long numProcessed, long total) {
        long now = new Date().getTime();
        long timeTaken = calcTimeTakenInMs(now, startTimeInMs);
        double percentCompleted = calcPercentCompleted(numProcessed, total);
        long msLeft = (long) (((timeTaken / percentCompleted) * 100) - timeTaken);
        return (now + msLeft);
    }

    public static Date getEstimatedCompleteDate(long startTimeInMs, long numProcessed, long total) {
        Date estCompleteDate = new Date(estCompletedTimeInMs(startTimeInMs, numProcessed, total));
        return estCompleteDate;
    }

    public static long calcTimeTakenInMs(long currentTimeInMs, long startTimeInMs) {
        return currentTimeInMs - startTimeInMs;
    }

    public static String convertMilisecondsToDetail(long ms) {
        String detail = String.format("%d days, %d hours, %d min and %d sec(s)",
                TimeUnit.MILLISECONDS.toDays(ms),
                TimeUnit.MILLISECONDS.toHours(ms) - TimeUnit.DAYS.toHours(TimeUnit.MILLISECONDS.toDays(ms)),
                TimeUnit.MILLISECONDS.toMinutes(ms) - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(ms)),
                TimeUnit.MILLISECONDS.toSeconds(ms) - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(ms))
        );
        // unpretty hack for runs that take less than 1 second - need to replace
        if (detail.equals("0 days, 0 hours, 0 min and 0 sec(s)")) {
            detail = "0 days, 0 hours, 0 min and 1 sec";
        }
        return detail;
    }

    public static String getColumnHeadersStr(ResultSetMetaData rsmd, String delimiter) throws SQLException {
        String columnHeaders = "";

        int cols = rsmd.getColumnCount();
        for (int i = 1; i <= cols; i++) {
            columnHeaders += rsmd.getColumnName(i);
            // add | after each heading except for the last heading
            if (i != cols) {
                columnHeaders += delimiter;
            }
        }
        return columnHeaders;
    }

    // MARKLOGIC VERSION
    public static String getColumnHeadersStr(ArrayList<String> elements, String delimiter) {
        String columnHeaders = "";

        int i = 1;
        for (String element : elements) {

            String elementName = checkForMappedElementName(element);

            columnHeaders += elementName;
            // add | after each heading except for the last heading
            if (i != elements.size()) {
                columnHeaders += delimiter;
            }
            i++;
        }
        return columnHeaders;
    }

    public static String extractFileToString(String fileName) {
        try {
            Scanner s = new Scanner(new File(fileName));
            String fileContentsStr = "";
            while (s.hasNextLine()) {
                fileContentsStr += s.nextLine();
            }
            s.close();
            return fileContentsStr;
        } catch (FileNotFoundException ex) {
            Logger.getLogger(ExtractHelper.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }
    }

    public static ArrayList<String> extractFileToList(String fileName) {
        try {
            Scanner s = new Scanner(new File(fileName));
            ArrayList<String> list = new ArrayList<String>();
            while (s.hasNextLine()) {
                String nextLine = s.nextLine();
                if (!nextLine.startsWith("#")) {
                    list.add(nextLine);
                }
            }
            s.close();
            return list;
        } catch (FileNotFoundException ex) {
            Logger.getLogger(ExtractHelper.class.getName()).log(Level.SEVERE, null, ex);
            return null;
        }
    }

    public static long getLineCount(String filename) {
        long lineCount = 0;
        try {
            LineNumberReader lnr = new LineNumberReader(new FileReader(new File(filename)));
            lnr.skip(Long.MAX_VALUE);
            lineCount = lnr.getLineNumber() + 1;
            // Finally, the LineNumberReader object should be closed to prevent resource leak
            lnr.close();
        } catch (IOException ex) {
            Logger.getLogger(ExtractHelper.class.getName()).log(Level.SEVERE, null, ex);
        }
        return lineCount;
    }

    public static String formatJDBCString(String sid, String serviceName, String host, String port) throws UnsupportedDataTypeException, IOException {
        String jdbc = "";
        if (sid != null && !sid.equals("")) {
            jdbc = "jdbc:oracle:thin:@" + host + ":" + port + ":" + sid;
        } else if (serviceName != null && !serviceName.equals("")) {
            //jdbc:oracle:thin:@//<host>:<port>/<service_name>
            jdbc = "jdbc:oracle:thin:@//" + host + ":" + port + "/" + serviceName + "";
        }
        return jdbc;
    }

    public static boolean isValidSelectStatement(String query) {
        query = query.toUpperCase();
        if (!query.startsWith("--") && !query.contains("UPDATE") && !query.contains("DELETE") && !query.contains("CREATE")) {
            return true;
        } else {
            return false;
        }
    }

    public static boolean isComment(String entry) {
        if (entry.startsWith("//")) {
            return true;
        } else {
            return false;
        }
    }

    public static String getQueryFromFile(String secondComp) throws FileNotFoundException, IOException {
        BufferedReader qIn = new BufferedReader(new FileReader(secondComp));
        String query = "";
        String qEntry;
        while ((qEntry = qIn.readLine()) != null) {
            //ignore -- comments
            if (qEntry.contains("--")) {
                qEntry = qEntry.substring(0, qEntry.indexOf("--"));
            }
            query += " ";
            query += qEntry;
        }
        query = ExtractHelper.cleanQueryText(query);
        qIn.close(); // Javier Added
        return query;
    }

    public static boolean validateEntry(String entry) {
        boolean result = false;
        try {
            String[] components = entry.split("###");
            String query = components[1];

            if (isComment(entry)) {
                log(Level.INFO, "Skipping entry in Query file as it is a comment: " + entry, true);
                result = false;
            } else if (components.length < 2) {
                log(Level.INFO, "Skipping entry in Query file as it is not a valid format: " + entry, true);
                result = false;
            } else if (!isValidSelectStatement(query) && !isValidFilePath(query)) {
                log(Level.INFO, "Skipping entry in Query file as no valid query or file location of query found: " + entry, true);
                result = false;
            } else {
                result = true;
            }
        } catch (Exception ex) {
            log(Level.SEVERE, "Skipping entry in Query file due to exception: " + entry, true);
        }
        return result;
    }

    public static void log(Level level, String message, boolean logToConsole) {
        if (logToConsole) {
            System.out.println(message);
        }
        Logger.getLogger("MAIN_LOG").log(level, message);
    }

    public static String stripInvalidXMLChars(String data) {
        String xml10pattern = "[^"
                + "\u0009\r\n"
                + "\u0020-\uD7FF"
                + "\uE000-\uFFFD"
                + "\ud800\udc00-\udbff\udfff"
                + "]";
        Pattern p = Pattern.compile(xml10pattern);
        Matcher m = p.matcher(data);
        while (m.find()) {
            data = data.replaceAll(xml10pattern, "");
        }
        return data;
    }

    public static String escapeDoubleQuotes(String data) {
        if (data.contains("\"")) {
            data = data.replaceAll("\"", "\"\"");
        }
        return data;
    }

    public static String removeDoubleQuotes(String data) {
        if (data.contains("\"")) {
            data = data.replaceAll("\"", "");
        }
        return data;
    }

    public static String removeSingleQuotes(String data) {
        if (data.contains("\'")) {
            data = data.replaceAll("\'", "");
        }
        return data;
    }

    public static String replaceDelimiter(String data, String delimiter) {
        if (data.contains(delimiter)) {
            data = data.replaceAll(delimiter, " ");
        }
        return data;
    }

    public static String cleanData(String strData, String DELIMITER) {
        strData = cleanDataText(strData, DELIMITER);
        strData = ExtractHelper.replaceAmpersand(strData);
        strData = ExtractHelper.replaceLessThan(strData);
        strData = ExtractHelper.replaceMoreThan(strData);
        return strData;
    }

    public static String cleanDataText(String strData, String DELIMITER) {  // Method was created for Chevron for ntext field.
        strData = stripInvalidXMLChars(strData);
        strData = escapeDoubleQuotes(strData);
        strData = replaceDelimiter(strData, DELIMITER);
        strData = ExtractHelper.replaceCR_LF((strData));
        return strData;
    }

    public static String replaceAmpersand(String data) {
        if (data.contains("&")) {
            data = data.replaceAll("\\&", "-");
        }
        return data;
    }

    public static String replaceLessThan(String data) {
        if (data.contains("<")) {
            data = data.replaceAll("\\<", "-");
        }
        return data;
    }

    public static String replaceMoreThan(String data) {
        if (data.contains(">")) {
            data = data.replaceAll("\\>", "-");
        }
        return data;
    }

    public static String replaceCR_LF(String data) {
        data = data.replaceAll("(\\.[\\t\\n\\r]+)", ". ");
        data = data.replaceAll("\\. [\\t\\n\\r]+", ". ");
        data = data.replaceAll("[\\t\\n\\r]+", ". ");
        return data;
    }

    public static void main(String[] args) {
        String test = "test. \n\n\r\r\n\n\n hi";
        System.out.println(replaceCR_LF(test));
    }

    public static boolean isValidFilePath(String path) {
        return path.toLowerCase().contains(".txt");
    }

    public static double estTimeLeft(double timeTaken, double percentCompleted) {
        return Math.ceil((percentCompleted / 100) * timeTaken);
    }

    // if user puts in Date==DateTime, we will use DateTime as the column name
    // no == in column name, means we use the specified entry with no changes
    private static String checkForMappedElementName(String element) {
        String elementMappedName = "";
        if (element.contains("==")) {
            String[] elementNameArray = element.split("==");
            if (elementNameArray.length == 2) {
                elementMappedName = element.split("==")[1];
            } else {
                throw new IllegalArgumentException("Column list is invalid, too many instances of ==. Only one instance should be in each column/element entry.");
            }
        } else {
            elementMappedName = element; // unmapped
        }
        return elementMappedName;
    }
}
