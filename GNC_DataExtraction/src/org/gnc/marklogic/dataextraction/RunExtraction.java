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

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;
import org.gnc.marklogic.dataextraction.relational.Relational_DataExtraction;
import org.gnc.marklogic.dataextraction.marklogic.Marklogic_DataExtraction;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.jasypt.encryption.pbe.*;  
import org.jasypt.properties.*;  

/**
*
* @author Robert Kennedy, rkennedy@gennet.com
*/
public class RunExtraction {

    public static void main(String[] args) {
        try {

            Logger LOGGER = Logger.getLogger("MAIN_LOG");

            String DB_TYPE = "";

            // SETTINGS FILE ENCRYPTION SUPPORT (OPTIONAL)
            StandardPBEStringEncryptor encryptor = new StandardPBEStringEncryptor();
            encryptor.setPassword("mySecretPasssphrase"); // Passphrase needed for encryptio/decryption

            // GET PROPERTIES FILE
            Properties props = new EncryptableProperties(encryptor);

            if (args.length < 1) {
                ExtractHelper.log(Level.SEVERE, "Zero arguments specified - Settings File must be passed as parameter. Exiting.", false);
                System.exit(-1);
            } else {
                FileInputStream fis;
                fis = new FileInputStream(args[0]);
                props.load(fis); //Will load property file

                DB_TYPE = props.getProperty("DB_TYPE");
                try {
                    fis.close();
                } catch (IOException ex) {
                }
            }

            // INITIALIZE EXTRACTION
            DataExtraction extractionRun = null;

            if (DB_TYPE.equals("ORACLE")) {
                extractionRun = new Relational_DataExtraction(LOGGER, props, "ORACLE");
            } else if (DB_TYPE.equals("MSSQL")) {
                extractionRun = new Relational_DataExtraction(LOGGER, props, "MSSQL");
            } else if (DB_TYPE.equals("MARKLOGIC")) {
                extractionRun = new Marklogic_DataExtraction(LOGGER, props);
            } else {
                ExtractHelper.log(Level.SEVERE, "Invalid DB TYPE Specified. Exiting", false);
                System.exit(-1);
            }

            System.out.println("X = EXIT GRACEFULLY");

            Scanner scanner = new Scanner(System.in);
            extractionRun.start();

            while (true) {
                String uInput = scanner.nextLine();

                // ONLY X SUPPORTED NOW
                switch (uInput.toLowerCase()) {
                    case "x":
                        ExtractHelper.log(Level.INFO, "USER INPUT: EXIT IMMEDIATELY", true);
                        scanner.close();
                        extractionRun.exitImmediately("IMMEDIATE EXIT WAS CALLED BY USER. EXITING GRACEFULLY");
                        break;
                    default:
                        System.out.println("USER INPUT: COMMAND NOT VALID");
                        break;
                }
            }

        } catch (IOException | InterruptedException ex) {
            ExtractHelper.log(Level.SEVERE, ex + " " + ex.getMessage(), false);
            ex.printStackTrace();
        }

    }
}
