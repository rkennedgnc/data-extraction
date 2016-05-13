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

import com.marklogic.client.document.DocumentRecord;
import com.marklogic.client.document.XMLDocumentManager;
import com.marklogic.client.io.DOMHandle;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamSource;

import org.gnc.marklogic.dataextraction.ExtractHelper;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

/**
 *
 * @author Robert Kennedy, rkennedy@gennet.com
 */
public class Marklogic_ProcessDoc implements Runnable {

    private DocumentRecord document;
    private ArrayList<String> elements;
    private PrintStream out;
    private final String DELIMITER;
    private String ROOT_NODE;
    private XMLDocumentManager docMgr;


    public Marklogic_ProcessDoc(DocumentRecord document, ArrayList<String> elements, PrintStream out, String DELIMITER, XMLDocumentManager docMgr, String ROOT_NODE) {
        this.document = document;
        this.elements = elements;
        this.out = out;
        this.DELIMITER = DELIMITER;
        this.docMgr = docMgr;
        this.ROOT_NODE = ROOT_NODE;

    }

    public void run() {
        try {
            processDoc();
        } catch (ParserConfigurationException | TransformerException | SAXException | IOException | InterruptedException ex) {
            Logger.getLogger(Marklogic_ProcessDoc.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void processDoc() throws ParserConfigurationException, TransformerConfigurationException, TransformerException, SAXException, IOException, InterruptedException {

        // read marklogic document into dom document
        DOMHandle handle = new DOMHandle();
        docMgr.read(document.getUri(), handle);
        Document mlDoc = handle.get();

        // if root node is null or empty, avoid extra processing and just skip to printing out relevant elements from doc
        if (ROOT_NODE != null && !ROOT_NODE.equals("")) {
            NodeList nList = mlDoc.getElementsByTagName(ROOT_NODE);

            // load transform for later use
            TransformerFactory factory = TransformerFactory.newInstance();
            Transformer transformer = factory.newTransformer(new StreamSource(new File("transforms\\marklogic\\TRANSFORM.xslt")));

            // for each element with root name -> create new temp doc exploding from specified root (in memory only)
            for (int j = 0; j < nList.getLength(); j++) {
                Document tempDoc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();

                // add specified root element to temp doc
                Node rootNode = nList.item(j);
                Node tempNode = tempDoc.createElement("ROOT");
                tempNode.appendChild(tempDoc.importNode(rootNode, true));

                // add uri to temp doc
                Element uriElement = tempDoc.createElement("DOC_URI");
                uriElement.setTextContent(document.getUri());
                tempNode.appendChild(uriElement);

                // add parents of root element and parent's siblings to temp doc
                tempNode = addParentsAndParentsSiblings(tempNode, rootNode, tempDoc);

                // transform attributes to elements
                tempDoc = transformDoc(tempNode, transformer);
                mlDoc = tempDoc;
            }
        }

        printOutElements(mlDoc);

    }

    private void printOutElements(Document mlDoc) {

        int columnCount = 1;
        String output = "";

        // loop through elements specified in elements_input_file and fetch each one from our temp doc where available
        for (String elementName : elements) {
            // get element value
            String elementValue = getElementValue(mlDoc, elementName);

            // add elementvalue to output
            output = output.concat(elementValue);

            // add delimiter to outputrow unless last column in row
            if (columnCount != elements.size()) {
                output = output.concat(DELIMITER);
            }
            columnCount++;
        }

        // write outputrow to file
        if (!output.trim().equals("")) {
            out.println(output);
        }
    }


    // this method has been built to work for specific solution and likely needs further fine tuning to be fully generic
    private Node addParentsAndParentsSiblings(Node tempNode, Node rootNode, Document tempDoc) {
        Node parent = rootNode.getParentNode();
        while (parent instanceof Element) {
            tempNode.appendChild(tempDoc.importNode(parent, false));
            //add parent's children except where child has same name as root node
            Node child = parent.getLastChild();
            while (child != null && !child.getNodeName().equals("")) {
                if (!child.getNodeName().equals(ROOT_NODE)) {
                    tempNode.appendChild(tempDoc.importNode(child, true));
                }
                child = child.getPreviousSibling();
            }

            // add parent's siblings except where sibling has same name
            Node sibling = parent.getPreviousSibling();
            while (sibling != null && !sibling.getNodeName().equals("") && !sibling.getNodeName().equals(parent.getNodeName())) {
                tempNode.appendChild(tempDoc.importNode(sibling, true));
                sibling = sibling.getPreviousSibling();
            }

            parent = parent.getParentNode();
        }
        return tempNode;
    }

    private Document transformDoc(Node rootNode, Transformer transformer) throws TransformerConfigurationException, TransformerException {
        DOMSource domInput = new DOMSource(rootNode);
        DOMResult domOutput = new DOMResult();
        transformer.transform(domInput, domOutput);
        Document domDoc = (Document) domOutput.getNode();
        return domDoc;
    }

    private String getElementValue(Document domDoc, String elementName) {
        NodeList nodeList = domDoc.getElementsByTagName(elementName);
        String elementValue = "";
        try {
            if (nodeList.getLength() > 1) {
                for (int k = 0; k < nodeList.getLength(); k++) {
                    // MERGE ELEMENTS TOGETHER WITH SAME NAME
                    elementValue += elementName
                            + (k + 1) + ": "
                            + nodeList.item(k).getChildNodes().item(0).getNodeValue()
                            + " ";
                }
            } else {
                elementValue += nodeList.item(0).getChildNodes().item(0).getNodeValue();
            }
        } catch (NullPointerException nex) {
            elementValue = "";
        }

        // REMOVE CARRIAGE RETURNS/LINE FEEDS
        elementValue = ExtractHelper.replaceCR_LF(elementValue);

        // REMOVE DELIMITER IF POSSIBLE (DEFAULT PIPE)
        try {
            elementValue = elementValue.replaceAll("\\" + DELIMITER, "");
        } catch (Exception ex) {
        }

        return elementValue;
    }
}
