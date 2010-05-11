package com.codeminders.hamake.syntax;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.io.output.ByteArrayOutputStream;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.codeminders.hamake.Hamake;
import com.codeminders.hamake.InvalidContextStateException;
import com.codeminders.hamake.PigNotFoundException;
import com.codeminders.hamake.Utils;
import com.codeminders.hamake.context.Context;

public abstract class BaseSyntaxParser {
	
	protected static boolean isPigAvailable = Utils.isPigAvailable(); 

	public static Hamake parse(Context context, String filename)
			throws Exception {
		InputStream is = new FileInputStream(filename);
		try {
			return parse(context, is);
		} finally {
			try {
				is.close();
			} catch (Exception ex) { /* Don't care */
			}
		}
	}

	public static Hamake parse(Context context, InputStream is)
			throws IOException, ParserConfigurationException, SAXException,
			InvalidMakefileException, PigNotFoundException, InvalidContextStateException{
		BaseSyntaxParser syntaxParser = new SyntaxParser(context);
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		bos.write(is);
		if(syntaxParser.validate(new ByteArrayInputStream(bos.toByteArray()))){
			Document doc = loadMakefile(new ByteArrayInputStream(bos.toByteArray()));
			return syntaxParser.parseSyntax(doc);
		}
		else{
			throw new InvalidMakefileException("Hamake-file validation error");
		}
	}	
	
	protected abstract Hamake parseSyntax(Document dom) throws IOException, ParserConfigurationException, SAXException,
	InvalidMakefileException, PigNotFoundException, InvalidContextStateException;
	
	protected abstract boolean validate(InputStream is) throws SAXException, IOException;
	
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
		return getOptionalAttribute(root, name, null);
	}
	
	protected String getOptionalAttribute(Element root, String name, String defaultValue) {
		if (root.hasAttribute(name)) {
			return root.getAttribute(name);
		}
		return defaultValue;
	}

}
