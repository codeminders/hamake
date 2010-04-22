package com.codeminders.hamake.syntax;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.codeminders.hamake.Context;
import com.codeminders.hamake.Hamake;
import com.codeminders.hamake.InvalidContextVariableException;
import com.codeminders.hamake.PigNotFoundException;
import com.codeminders.hamake.Utils;

public abstract class BaseSyntaxParser {
	
	protected static boolean isPigAvailable = Utils.isPigAvailable(); 

	public static Hamake parse(Context context, String filename, String wdir, boolean verbose)
			throws Exception {
		InputStream is = new FileInputStream(filename);
		try {
			return parse(context, is, wdir, verbose);
		} finally {
			try {
				is.close();
			} catch (Exception ex) { /* Don't care */
			}
		}
	}

	public static Hamake parse(Context context, InputStream is, String wdir, boolean verbose)
			throws IOException, ParserConfigurationException, SAXException,
			InvalidMakefileException, PigNotFoundException, InvalidContextVariableException{
		Document doc = loadMakefile(is);
		BaseSyntaxParser syntaxParser = new SyntaxParser(doc, wdir, context, verbose);
		if(!syntaxParser.isCorrectParser()){
			throw new InvalidMakefileException("Unknown hamakefile syntax");
		}
		return syntaxParser.parseSyntax();
	}	
	
	protected abstract Hamake parseSyntax() throws IOException, ParserConfigurationException, SAXException,
	InvalidMakefileException, PigNotFoundException, InvalidContextVariableException;
	
	protected abstract boolean isCorrectParser();
	
	protected static Document loadMakefile(InputStream is) throws IOException,
			ParserConfigurationException, SAXException {
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder = factory.newDocumentBuilder();
		return builder.parse(is);
	}	
	
	protected static String getPath(Node n) {
		StringBuilder ret = new StringBuilder();
		while (n != null && n.getNodeType() != Node.DOCUMENT_NODE) {
			ret.insert(0, n.getNodeName()).insert(0, '/');
			n = n.getParentNode();
		}
		return ret.toString();
	}

	protected Element getMandatory(Element root, String name)
			throws InvalidMakefileException {
		NodeList c = root.getElementsByTagName(name);
		if (c.getLength() != 1)
			throw new InvalidMakefileException("Missing or ambiguous '" + name
					+ "' section");
		return (Element) c.item(0);
	}

	protected Element getOptional(Element root, String name)
			throws InvalidMakefileException {
		NodeList c = root.getElementsByTagName(name);
		if (c.getLength() != 1)
			throw new InvalidMakefileException("Missing or ambiguous '" + name
					+ "' section");
		return (Element) c.item(0);
	}
	
	protected Element getOneSubElement(Element root, String... elementNames) throws InvalidMakefileException{
		for(String elementName : elementNames){
			NodeList list = root.getElementsByTagName(elementName);
			if(list.getLength() > 0){
		        if(list.getLength() == 1){
		        	return (Element) list.item(0);
		        }
		        else{
		        	throw new InvalidMakefileException("Multiple elements '" + elementName + "' in " + getPath(root) +
	                " are not permitted");
		        }
			}
		}
		return null;
	}
	
	protected String getRequiredAttribute(Element root, String name) throws InvalidMakefileException {
		if (root.hasAttribute(name)) {
			return root.getAttribute(name);
		}
		throw new InvalidMakefileException("Missing '" + name
				+ "' attribute in '" + getPath(root) + "' element");
	}

	protected String getOptionalAttribute(Element root, String name) {
		if (root.hasAttribute(name)) {
			return root.getAttribute(name);
		}
		return null;
	}

}
