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

import com.marklogic.client.document.DocumentPage;
import com.marklogic.client.document.DocumentRecord;
import com.marklogic.client.document.XMLDocumentManager;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 *
 * @author Robert Kennedy, rkennedy@gennet.com
 */
public class Marklogic_ProcessPage implements Runnable {

    private final DocumentPage documents;
    private final ArrayList<String> elements;
    private final PrintStream out;
    private final String DELIMITER;
    private final XMLDocumentManager docMgr;
    private final String ROOT_NODE;

    public Marklogic_ProcessPage(DocumentPage documents, ArrayList<String> elements, PrintStream out, String DELIMITER, XMLDocumentManager docMgr, String ROOT_NODE) {
        this.documents = documents;
        this.elements = elements;
        this.out = out;
        this.DELIMITER = DELIMITER;
        this.docMgr = docMgr;
        this.ROOT_NODE = ROOT_NODE;
    }

    public void run() {
        processPage();
    }

    private void processPage() {
        // PROCESS PAGE OF DOCUMENTS
        Marklogic_ProcessDoc processDoc;

        int numThreads = Runtime.getRuntime().availableProcessors();
        //int numThreads = 1;
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        for (DocumentRecord document : documents) {
            // CALL THREAD TO PROCESS DOC
            processDoc = new Marklogic_ProcessDoc(document, elements, out, DELIMITER, docMgr, ROOT_NODE);
            Runnable worker = processDoc;
            executor.execute(worker);
        }

        executor.shutdown();
        // wait until all threads are finish
        while (!executor.isTerminated()) {
        }
    }

}
