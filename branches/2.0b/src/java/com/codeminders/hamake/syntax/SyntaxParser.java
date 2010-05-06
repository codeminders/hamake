package com.codeminders.hamake.syntax;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import com.codeminders.hamake.Context;
import com.codeminders.hamake.Hamake;
import com.codeminders.hamake.InvalidContextStateException;
import com.codeminders.hamake.PigNotFoundException;
import com.codeminders.hamake.Utils;
import com.codeminders.hamake.data.DataFunction;
import com.codeminders.hamake.data.FileDataFunction;
import com.codeminders.hamake.data.FilesetDataFunction;
import com.codeminders.hamake.data.SetDataFunction;
import com.codeminders.hamake.dtr.DataTransformationRule;
import com.codeminders.hamake.dtr.Fold;
import com.codeminders.hamake.dtr.Foreach;
import com.codeminders.hamake.params.CommaConcatFunction;
import com.codeminders.hamake.params.ConcatFunction;
import com.codeminders.hamake.params.HamakeParameter;
import com.codeminders.hamake.params.IdentityProcessingFunction;
import com.codeminders.hamake.params.JobConfParam;
import com.codeminders.hamake.params.Literal;
import com.codeminders.hamake.params.AppendConcatFunction;
import com.codeminders.hamake.params.Parameter;
import com.codeminders.hamake.params.ParameterItem;
import com.codeminders.hamake.params.ProcessingFunction;
import com.codeminders.hamake.params.Reference;
import com.codeminders.hamake.params.SpaceConcatFunction;
import com.codeminders.hamake.task.*;

public class SyntaxParser extends BaseSyntaxParser {
	
	public static final Log LOG = LogFactory.getLog(SyntaxParser.class);
	public static boolean validationSucceeded = true;
	
	public class ForgivingErrorHandler implements ErrorHandler {

		@Override
		public void error(SAXParseException e) throws SAXException {
			LOG.error(e.getMessage() + " at line " + e.getLineNumber() + ", column " + e.getColumnNumber());
			validationSucceeded = false;
		}

		@Override
		public void fatalError(SAXParseException e) throws SAXException {
			LOG.error(e.getMessage() + " at line " + e.getLineNumber() + ", column " + e.getColumnNumber());
			validationSucceeded = false;
		}

		@Override
		public void warning(SAXParseException e) throws SAXException {
			LOG.warn(e.getMessage() + " at line " + e.getLineNumber() + ", column " + e.getColumnNumber());
		}

	}
	
	private Random random = new Random();
	private Context rootContext;
	private static final String SCHEMA_NAME = "hamakefile-" + Hamake.HAMAKE_VERSION + ".xsd";
	
	
	protected SyntaxParser(Context rootContext){
		this.rootContext = rootContext;
	}
	
	@Override
	protected boolean validate(InputStream is) throws SAXException, IOException{	
		ForgivingErrorHandler errorHandler = new ForgivingErrorHandler();
		SchemaFactory factory = SchemaFactory.newInstance("http://www.w3.org/2001/XMLSchema");
		Schema schema = factory.newSchema(new StreamSource(SyntaxParser.class.getResourceAsStream(SCHEMA_NAME)));
		Validator validator = schema.newValidator();
		validator.setErrorHandler(errorHandler);
        validator.validate(new StreamSource(is));
        return validationSucceeded;
	}
	
	@Override
	protected Hamake parseSyntax(Document dom)
			throws IOException, ParserConfigurationException, SAXException,
			InvalidMakefileException, PigNotFoundException, InvalidContextStateException {

		Hamake ret = new Hamake(rootContext);
		parseProperties(dom);
		parseRootDataFunctions(dom);
		parseDTRs(ret, dom.getDocumentElement());
		ret.setDefaultTarget(dom.getDocumentElement().getAttribute("default"));
		ret.setProjectName(getOptionalAttribute(dom.getDocumentElement(), "name", "project" + Math.abs(random.nextInt() % 1000)));
		return ret;
	}
	
	protected void parseProperties(Document dom)
			throws InvalidMakefileException, InvalidContextStateException {
		NodeList c = dom.getElementsByTagName("property");
		for (int i = 0, sz = c.getLength(); i < sz; i++) {
			Element e = (Element) c.item(i);
			rootContext.set(getRequiredAttribute(e, "name"), getRequiredAttribute(e, "value"));
		}
	}
	
	protected void parseRootDataFunctions(Document dom)
			throws InvalidMakefileException, IOException,
			InvalidContextStateException {
		NodeList children = dom.getDocumentElement().getChildNodes();
		for (int i = 0; i < children.getLength(); i++) {
			if (children.item(i).getNodeType() == Node.ELEMENT_NODE) {
				Element child = (Element) children.item(i);
				if (child.getNodeName().equals("file")) {
					DataFunction func = parseFile(child, Long.MAX_VALUE);
					if(!StringUtils.isEmpty(func.getId()))rootContext.setForbidden(func.getId(), func);
				} else if (child.getNodeName().equals("fileset")) {
					DataFunction func = parseFileset(child, Long.MAX_VALUE);
					if(!StringUtils.isEmpty(func.getId()))rootContext.setForbidden(func.getId(), func);
				} else if (child.getNodeName().equals("set")) {
					DataFunction func = parseSet(child, Long.MAX_VALUE);
					if(!StringUtils.isEmpty(func.getId()))rootContext.setForbidden(func.getId(), func);
				}
			}
		}
	}

	protected void parseDTRs(Hamake hamake, Element config) throws InvalidMakefileException, IOException,
			PigNotFoundException, InvalidContextStateException {
		NodeList tx = config.getElementsByTagName("foreach");
		for (int i = 0, sz = tx.getLength(); i < sz; i++) {
			Element t = (Element) tx.item(i);
			String dattr = getOptionalAttribute(t, "disabled");
			if ("yes".equalsIgnoreCase(dattr) || "true".equalsIgnoreCase(dattr)) {
				if (rootContext.getBoolean(Context.HAMAKE_PROPERTY_VERBOSE))
					System.out.println("Ignoring disabled task "
							+ getOptionalAttribute(t, "name"));
				continue;
			}
			hamake.addTask(parseForeachDTR(t));
		}
		tx = config.getElementsByTagName("fold");
		for (int i = 0, sz = tx.getLength(); i < sz; i++) {
			Element t = (Element) tx.item(i);
			String dattr = getOptionalAttribute(t, "disabled");
			if ("yes".equalsIgnoreCase(dattr) || "true".equalsIgnoreCase(dattr)) {
				if (rootContext.getBoolean(Context.HAMAKE_PROPERTY_VERBOSE))
					System.out.println("Ignoring disabled task "
							+ getOptionalAttribute(t, "name"));
				continue;
			}
			hamake.addTask(parseFoldDTR(t));
		}
	}

	protected DataTransformationRule parseForeachDTR(Element root)
			throws InvalidMakefileException, IOException, PigNotFoundException, InvalidContextStateException {
		String name = getOptionalAttribute(root, "name", "foreach" + Math.abs(random.nextInt() % 1000));

		Element input = getOneSubElement(root, "input");
		if(input == null){
			throw new InvalidMakefileException(getPath(root) + " should have one input sub-element");
		}
		DataFunction inputFunc = parseDTRData(input, Arrays.asList("fileset", "include"), 1, 1).get(0);
		Element output = getOneSubElement(root, "output");
		if(output == null){
			throw new InvalidMakefileException(getPath(root) + " should have one output sub-element");
		}
		List<DataFunction> outputFuncs = parseDTRData(output, Arrays.asList("fileset", "file", "set", "include"), 1, Integer.MAX_VALUE);
		Element dependencies = getOneSubElement(root, "dependencies");
		List<DataFunction> deps = null;
		deps = parseDTRData(dependencies, Arrays.asList("fileset", "file", "set", "include"), 0, Integer.MAX_VALUE);

		Task task = parseTask(root, (String)rootContext.get(Context.HAMAKE_PROPERTY_WORKING_FOLDER));

		Foreach foreach = new Foreach(rootContext, inputFunc, outputFuncs, deps);
		foreach.setName(name);
		foreach.setTask(task);
		return foreach;
	}

	protected DataTransformationRule parseFoldDTR(Element root) throws InvalidMakefileException, IOException, PigNotFoundException, InvalidContextStateException {
        String name = getOptionalAttribute(root, "name", "fold" + Math.abs(random.nextInt() % 1000));

        Element input = getOneSubElement(root, "input");
        if(input == null){
			throw new InvalidMakefileException(getPath(root) + " should have one input sub-element");
		}
        List<DataFunction> inputFuncs = parseDTRData(input, Arrays.asList("fileset", "file", "set", "include"), 0, Integer.MAX_VALUE);
        Element output = getOneSubElement(root, "output");
        if(output == null){
			throw new InvalidMakefileException(getPath(root) + " should have one output sub-element");
		}
        List<DataFunction> outputFuncs = parseDTRData(output, Arrays.asList("fileset", "file", "set", "include"), 1, Integer.MAX_VALUE);
        Element dependencies = getOneSubElement(root, "dependencies");
        List<DataFunction> dependenciesFunc = null;
        dependenciesFunc = parseDTRData(dependencies, Arrays.asList("fileset", "file", "set", "include"), 0, Integer.MAX_VALUE);

        Task task = parseTask(root, (String)rootContext.get(Context.HAMAKE_PROPERTY_WORKING_FOLDER));

        Fold res = new Fold(rootContext, inputFuncs, outputFuncs, dependenciesFunc);
        res.setName(name);
        res.setTask(task);
        return res;
    }
	
	protected List<DataFunction> parseDTRData(Element root, List<String> allowedSubElements, int minElementsAmount, int maxElementsAmount) throws InvalidMakefileException, IOException, InvalidContextStateException {
		if(root == null) return Collections.emptyList();
		List<DataFunction> functions = new ArrayList<DataFunction>();
		String expiration = getOptionalAttribute(root, "expiration");
		long validityPeriod = parseValidityPeriod(expiration);
		NodeList children = root.getChildNodes();		
		for(int i = 0; i < children.getLength(); i ++){
			if (children.item(i).getNodeType() == Node.ELEMENT_NODE) {
				Element child = (Element)children.item(i);
				for(String allowedSubElement : allowedSubElements){
					if(child.getNodeName().equals(allowedSubElement)){
						functions.add(parseDataFunction(child, allowedSubElement, validityPeriod));
					}
				}
			}
		}
		if(functions.size() < minElementsAmount){
			throw new InvalidMakefileException(getPath(root) + " should have at least one of " + StringUtils.join(allowedSubElements, ","));
		}
		if(functions.size() > maxElementsAmount){
			throw new InvalidMakefileException(getPath(root) + " should have no more then " + maxElementsAmount + " sub-elements");
		}
		return functions;
	}

	private DataFunction parseDataFunction(Element element, String elementName,	long validityPeriod) throws InvalidMakefileException, IOException, InvalidContextStateException {
		if(elementName.equals("fileset")){
			return parseFileset(element, validityPeriod);
		}
		else if(elementName.equals("file")){
			return parseFile(element, validityPeriod);
		}
		else if(elementName.equals("set")){
			return parseSet(element, validityPeriod);
		}
		else if(elementName.equals("include")){
			return parseInclude(element, validityPeriod);
		}
		throw new InvalidMakefileException(getPath(element) + ": unknown element found");
	}

	protected DataFunction parseFileset(Element root, long validityPeriod)
			throws InvalidMakefileException, IOException, InvalidContextStateException{
		String id = getOptionalAttribute(root, "id", UUID.randomUUID().toString());
		String path = getRequiredAttribute(root, "path");
		String mask = getOptionalAttribute(root, "mask", "*");
		String generationValue = getOptionalAttribute(root, "generation", "0");
		int generation = Integer.parseInt(generationValue);
		FilesetDataFunction fileset = new FilesetDataFunction(id, generation, validityPeriod, (String)rootContext.get(Context.HAMAKE_PROPERTY_WORKING_FOLDER), path, mask);
		if(!StringUtils.isEmpty(id)) rootContext.set(id, fileset);
		return fileset;
	}

	protected DataFunction parseFile(Element root, long validityPeriod)
			throws InvalidMakefileException, IOException, InvalidContextStateException {

		String id = getOptionalAttribute(root, "id", UUID.randomUUID().toString());
		String path = getRequiredAttribute(root, "path");
		String generationValue = getOptionalAttribute(root, "generation", "0");
		int	generation = Integer.parseInt(generationValue);
		FileDataFunction file = new FileDataFunction(id, generation, validityPeriod, (String)rootContext.get(Context.HAMAKE_PROPERTY_WORKING_FOLDER), path);
		if(!StringUtils.isEmpty(id)) rootContext.set(id, file);
		return file;
	}

	protected DataFunction parseSet(Element root, long validityPeriod) throws InvalidMakefileException, InvalidContextStateException, IOException{
		String id = getOptionalAttribute(root, "id", UUID.randomUUID().toString());
		SetDataFunction set = new SetDataFunction(id);
		NodeList children = root.getChildNodes();		
		for(int i = 0; i < children.getLength(); i ++){
			if (children.item(i).getNodeType() == Node.ELEMENT_NODE) {
				Element child = (Element)children.item(i);
				if(child.getNodeName().equals("set")){
					set.addDataFunction(parseSet(child, validityPeriod));
				}
				else if(child.getNodeName().equals("file")){
					set.addDataFunction(parseFile(child, validityPeriod));
				}
				else if(child.getNodeName().equals("fileset")){
					set.addDataFunction(parseFileset(child, validityPeriod));
				}
				else if(child.getNodeName().equals("include")){
					set.addDataFunction(parseInclude(child, validityPeriod));
				}
				else{
					throw new InvalidMakefileException(getPath(child) + ": unknown element found");
				}
			}
		}
		return set;
	}
	
	protected DataFunction parseInclude(Element root, long validityPeriod) throws InvalidMakefileException{		
		String reference = getRequiredAttribute(root, "idref");
		Object obj = rootContext.get(reference);
		if(obj == null || !(obj instanceof DataFunction)){
			throw new InvalidMakefileException("Unknown data function reference found: " + getPath(root));
		}
		return (DataFunction)obj;
	}

	protected Task parseTask(Element root, String wdir)
			throws InvalidMakefileException, IOException, PigNotFoundException {
		Element task = getOneSubElement(root, "mapreduce", "pig", "exec");
		if (task == null)
			throw new InvalidMakefileException(
					"Element '"
							+ root.getNodeName()
							+ "' should have one of mapreduce, pig or exec sub-elements");
		if (task.getNodeName().equals("mapreduce")) {
			return parseMapReduceTask(task);
		} else if (task.getNodeName().equals("pig")) {
			return parsePigTask(task);
		} else if (task.getNodeName().equals("exec")) {
			return parseExecTask(task);
		} else {
			throw new InvalidMakefileException("Unknown task type found in '"
					+ root.getNodeName() + "' element");
		}
	}

	protected Task parseMapReduceTask(Element root)
			throws InvalidMakefileException {
		MapReduce res = new MapReduce();
		res.setJar(Utils.resolvePath(Utils.replaceVariables(rootContext, getRequiredAttribute(root, "jar")), (String)rootContext.get(Context.HAMAKE_PROPERTY_WORKING_FOLDER)).toString());
		res.setMain(getRequiredAttribute(root, "main"));
		res.setParameters(parseParametersList(root));
		return res;
	}

	protected Task parsePigTask(Element root)
			throws InvalidMakefileException, IOException, PigNotFoundException {

        String jar = getOptionalAttribute(root, "jar");

        if (jar == null && Utils.isAmazonEMRPigAvailable())
            jar = Utils.AmazonEMRPigJarURI.toString();

        if (jar == null && !isPigAvailable)
            throw new PigNotFoundException("Pig isn't found in classpath. Please, make sure Pig classes are available in classpath.");

        Path scriptPath = Utils.resolvePath(Utils.replaceVariables(rootContext, getRequiredAttribute(root, "script")), (String)rootContext.get(Context.HAMAKE_PROPERTY_WORKING_FOLDER));
        List<Parameter> params = parseParametersList(root);

        return (jar == null)?
               new Pig(scriptPath, params):
               new PigJar(jar, scriptPath, params);		
	}

	protected Task parseExecTask(Element root)
			throws InvalidMakefileException, IOException {
		Exec res = new Exec();
		res.setBinary(Utils.resolvePath(Utils.replaceVariables(rootContext, getRequiredAttribute(root, "binary")), (String)rootContext.get(Context.HAMAKE_PROPERTY_WORKING_FOLDER)));
		res.setParameters(parseParametersList(root));
		return res;
	}

	protected List<Parameter> parseParametersList(Element root) throws InvalidMakefileException {
		List<Parameter> parameters = new ArrayList<Parameter>();
		NodeList children = root.getChildNodes();
		int counter = 0;
		for (int i = 0; i < children.getLength(); i++) {
			if (children.item(i).getNodeType() == Node.ELEMENT_NODE) {
				Element element = (Element)children.item(i);
				if ("parameter".equals(element.getNodeName())) {
					parameters.add(parseParameter(element, counter));
				}
				else if ("jobconf".equals(element.getNodeName())) {
					parameters.add(new JobConfParam(getRequiredAttribute(element, "name"), getRequiredAttribute(element, "value")));
				}
			}
		}
		return parameters;
	}
	
	protected HamakeParameter parseParameter(Element root, int index) throws InvalidMakefileException {
        String concatFuncIdentificator = getOptionalAttribute(root, "concat_function");
        ConcatFunction concatFunction = null;
        if(!StringUtils.isEmpty(concatFuncIdentificator)){
        	if("space".equals(concatFuncIdentificator)) concatFunction = new SpaceConcatFunction();
        	else if("comma".equals(concatFuncIdentificator)) concatFunction = new CommaConcatFunction();
        	else if("append".equals(concatFuncIdentificator)) concatFunction = new AppendConcatFunction();
        	else throw new InvalidMakefileException("'concat_function' attribute of " + getPath(root) + " contains unknown function");
        }
        else{
        	concatFunction = new AppendConcatFunction();
        }
        String processingFuncIdentificator = getOptionalAttribute(root, "processing_function");
        String parameterName = getOptionalAttribute(root, "name", "param" + Math.abs(random.nextInt() % 10000));
        ProcessingFunction processingFunc = null;
        if(!StringUtils.isEmpty(processingFuncIdentificator)){
        	if("identity".equals(processingFuncIdentificator)) concatFunction = new SpaceConcatFunction();
        	else throw new InvalidMakefileException("'processing_function' attribute of " + getPath(root) + "contains unknown function");
        }
        else{
        	processingFunc = new IdentityProcessingFunction();
        }
        List<ParameterItem> values = new ArrayList<ParameterItem>();
        NodeList children = root.getChildNodes();
		for (int i = 0; i < children.getLength(); i++) {
			if (children.item(i).getNodeType() == Node.ELEMENT_NODE) {
				Element element = (Element)children.item(i);
				if ("reference".equals(element.getNodeName())) {
					values.add(new Reference(getRequiredAttribute(element, "idref")));
				}
				else if ("literal".equals(element.getNodeName())) {
					values.add(new Literal(getRequiredAttribute(element, "value")));
				}
				else{
					throw new InvalidMakefileException("unknown sub-element found whithin " + getPath(root));
				}
			}
		}
        return new HamakeParameter(parameterName, values, concatFunction, processingFunc);
    }
	
	protected long parseValidityPeriod(String expiration) throws InvalidMakefileException{
		long res = Long.MAX_VALUE;
		long multiplier = 1;
		if(!StringUtils.isEmpty(expiration)){
			if(StringUtils.endsWithIgnoreCase(expiration, "s")){
				expiration = expiration.substring(0, expiration.length() - 1);
				multiplier = 1;
			}
			else if(StringUtils.endsWithIgnoreCase(expiration, "m")){
				expiration = expiration.substring(0, expiration.length() - 1);
				multiplier = 60;
			}
			else if(StringUtils.endsWithIgnoreCase(expiration, "h")){
				expiration = expiration.substring(0, expiration.length() - 1);
				multiplier = 60*60;
			}
			else if(StringUtils.endsWithIgnoreCase(expiration, "d")){
				expiration = expiration.substring(0, expiration.length() - 1);
				multiplier = 60*60*24;
			}
			else if(StringUtils.endsWithIgnoreCase(expiration, "w")){
				expiration = expiration.substring(0, expiration.length() - 1);
				multiplier = 60*60*24*7;
			}
			if(StringUtils.isNumeric(expiration)){
				res = Long.parseLong(expiration) * multiplier;
			}
			else{
				throw new InvalidMakefileException("Can not parse expiration date " + expiration);
			}
		}
		return res;
	}

}
