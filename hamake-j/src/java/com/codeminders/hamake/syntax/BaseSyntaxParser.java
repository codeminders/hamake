package com.codeminders.hamake.syntax;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.lang.StringUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.codeminders.hamake.Context;
import com.codeminders.hamake.Hamake;
import com.codeminders.hamake.PigNotFoundException;
import com.codeminders.hamake.Utils;

public abstract class BaseSyntaxParser {
	
	protected static boolean isPigAvailable = Utils.isPigAvailable(); 

	public static Hamake parse(Context context, String filename, String wdir, boolean verbose)
			throws IOException, ParserConfigurationException, SAXException,
			InvalidMakefileException, PigNotFoundException {
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
			InvalidMakefileException, PigNotFoundException{
		Document doc = loadMakefile(is);
		BaseSyntaxParser syntaxParser = new SyntaxParser(doc, wdir, context, verbose);
		if(!syntaxParser.isCorrectParser()){
			throw new InvalidMakefileException("Unknown hamakefile syntax");
		}
		return syntaxParser.parseSyntax();
	}	
	
	protected abstract Hamake parseSyntax() throws IOException, ParserConfigurationException, SAXException,
	InvalidMakefileException, PigNotFoundException;
	
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
	
	protected String getRequiredAttribute(Element root, String name)
			throws InvalidMakefileException {
		return getRequiredAttribute(root, name, null, null);
	}

	protected String getRequiredAttribute(Element root, String name,
			Pattern variablePattern, Context context) throws InvalidMakefileException {
		if (root.hasAttribute(name)) {
			String ret = root.getAttribute(name);
			if (context != null) {
				ret = substitute(ret, variablePattern, context);
			}
			return ret;
		}
		throw new InvalidMakefileException("Missing '" + name
				+ "' attribute in '" + getPath(root) + "' element");
	}

	protected String getOptionalAttribute(Element root, String name) {
		return getOptionalAttribute(root, name, null, null);
	}

	protected String getOptionalAttribute(Element root, String name,
			Pattern variablePattern, Context context) {
		if (root.hasAttribute(name)) {
			String ret = root.getAttribute(name);
			if (context != null) {
				ret = substitute(ret, variablePattern, context);
			}
			return ret;
		}
		return null;
	}

	protected String substitute(String s,
			Pattern variablePlaceholder, Context context) {
		int pos = 0;
		Matcher m = variablePlaceholder.matcher(s);
		while (m.find(pos)) {
			int start = m.start();
			int end = m.end();
			StringBuilder buf = new StringBuilder();
			if (start > 0)
				buf.append(s, 0, start);
			String subst = context.getString(m.group(1));
			if (subst != null)
				buf.append(subst);
			else
				subst = StringUtils.EMPTY;
			if (end < s.length() - 1)
				buf.append(s, end, s.length());
			s = buf.toString();
			pos = start + subst.length();
			if (pos > s.length())
				break;
			m = variablePlaceholder.matcher(s);
		}
		return s;
	}

}
