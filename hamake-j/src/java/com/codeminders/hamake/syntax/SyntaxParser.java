package com.codeminders.hamake.syntax;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.lang.StringUtils;
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
import com.codeminders.hamake.params.Parameter;
import com.codeminders.hamake.params.ProcessingFunction;
import com.codeminders.hamake.params.Reference;
import com.codeminders.hamake.params.SpaceConcatFunction;
import com.codeminders.hamake.task.Exec;
import com.codeminders.hamake.task.MapReduce;
import com.codeminders.hamake.task.Pig;
import com.codeminders.hamake.task.Task;

public class SyntaxParser extends BaseSyntaxParser {
	
	Document dom;
	String wdir;
	boolean verbose;
	Context context;
	
	protected SyntaxParser(Document dom, String wdir, Context context, boolean verbose){
		this.dom = dom;
		this.wdir = wdir;
		this.verbose = verbose;
		this.context = context;
	}

	@Override
	protected Hamake parseSyntax()
			throws IOException, ParserConfigurationException, SAXException,
			InvalidMakefileException, PigNotFoundException, InvalidContextVariableException {

		parseProperties(dom);

		Hamake ret = new Hamake();
		parseRootDataFunctions(dom);
		parseDTRs(ret, dom.getDocumentElement(), verbose);
		String defaultTask = dom.getDocumentElement().getAttribute("default");
		ret.setProjectName(dom.getDocumentElement().getAttribute("name"));
		if (!StringUtils.isEmpty(defaultTask)) {
			ret.setDefaultTarget(defaultTask);
		}
		return ret;
	}
	
	@Override
	protected boolean isCorrectParser(){		
		return (dom.getElementsByTagName("foreach").getLength() > 0) || (dom.getElementsByTagName("foreach").getLength() > 0);
	}

	protected Element parseConfig(Document dom) throws InvalidMakefileException {
		return getMandatory(dom.getDocumentElement(), "config");
	}

	protected void parseProperties(Document dom)
			throws InvalidMakefileException, InvalidContextVariableException {
		NodeList c = dom.getElementsByTagName("property");
		context.set(Hamake.SYS_PROPERTY_WORKING_FOLDER, wdir);

		for (int i = 0, sz = c.getLength(); i < sz; i++) {
			Element e = (Element) c.item(i);
			context.set(getRequiredAttribute(e, "name"), getRequiredAttribute(e, "value"));
		}
	}
	
	protected void parseRootDataFunctions(Document dom)
			throws InvalidMakefileException, IOException,
			InvalidContextVariableException {
		NodeList children = dom.getDocumentElement().getChildNodes();
		for (int i = 0; i < children.getLength(); i++) {
			if (children.item(i).getNodeType() == Node.ELEMENT_NODE) {
				Element child = (Element) children.item(i);
				if (child.getNodeName().equals("file")) {
					DataFunction func = parseFile(child, Long.MAX_VALUE);
					if(!StringUtils.isEmpty(func.getId()))context.setHamake(func.getId(), func);
				} else if (child.getNodeName().equals("fileset")) {
					DataFunction func = parseFileset(child, Long.MAX_VALUE);
					if(!StringUtils.isEmpty(func.getId()))context.setHamake(func.getId(), func);
				} else if (child.getNodeName().equals("set")) {
					DataFunction func = parseSet(child, Long.MAX_VALUE);
					if(!StringUtils.isEmpty(func.getId()))context.setHamake(func.getId(), func);
				}
			}
		}
	}

	protected void parseDTRs(Hamake hamake, Element config, boolean verbose) throws InvalidMakefileException, IOException,
			PigNotFoundException, InvalidContextVariableException {
		NodeList tx = config.getElementsByTagName("foreach");
		for (int i = 0, sz = tx.getLength(); i < sz; i++) {
			Element t = (Element) tx.item(i);
			String dattr = getOptionalAttribute(t, "disabled");
			if ("yes".equalsIgnoreCase(dattr) || "true".equalsIgnoreCase(dattr)) {
				if (verbose)
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
				if (verbose)
					System.out.println("Ignoring disabled task "
							+ getOptionalAttribute(t, "name"));
				continue;
			}
			hamake.addTask(parseFoldDTR(t));
		}
	}

	protected DataTransformationRule parseForeachDTR(Element root)
			throws InvalidMakefileException, IOException, PigNotFoundException, InvalidContextVariableException {
		String name = getRequiredAttribute(root, "name");

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

		Task task = parseTask(root, wdir);

		Foreach foreach = new Foreach(context, inputFunc, outputFuncs, deps);
		foreach.setName(name);
		foreach.setTask(task);
		return foreach;
	}

	protected DataTransformationRule parseFoldDTR(Element root) throws InvalidMakefileException, IOException, PigNotFoundException, InvalidContextVariableException {
        String name = getRequiredAttribute(root, "name");

        Element input = getOneSubElement(root, "input");
        if(input == null){
			throw new InvalidMakefileException(getPath(root) + " should have one input sub-element");
		}
        List<DataFunction> inputFuncs = parseDTRData(input, Arrays.asList("fileset", "file", "set", "include"), 1, Integer.MAX_VALUE);
        Element output = getOneSubElement(root, "output");
        if(output == null){
			throw new InvalidMakefileException(getPath(root) + " should have one output sub-element");
		}
        List<DataFunction> outputFuncs = parseDTRData(output, Arrays.asList("fileset", "file", "set", "include"), 1, Integer.MAX_VALUE);
        Element dependencies = getOneSubElement(root, "dependencies");
        List<DataFunction> dependenciesFunc = null;
        dependenciesFunc = parseDTRData(dependencies, Arrays.asList("fileset", "file", "set", "include"), 0, Integer.MAX_VALUE);

        Task task = parseTask(root, wdir);

        Fold res = new Fold(context, inputFuncs, outputFuncs, dependenciesFunc);
        res.setName(name);
        res.setTask(task);
        return res;
    }
	
	protected List<DataFunction> parseDTRData(Element root, List<String> allowedSubElements, int minElementsAmount, int maxElementsAmount) throws InvalidMakefileException, IOException, InvalidContextVariableException {
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

	private DataFunction parseDataFunction(Element element, String elementName,	long validityPeriod) throws InvalidMakefileException, IOException, InvalidContextVariableException {
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
			throws InvalidMakefileException, IOException, InvalidContextVariableException{
		String id = getOptionalAttribute(root, "id");
		String path = getRequiredAttribute(root, "path");
		String mask = getOptionalAttribute(root, "mask");
		String generation = getOptionalAttribute(root, "generation");
		int gen;
		if (generation == null)
			gen = 0;
		else
			gen = Integer.parseInt(generation);
		FilesetDataFunction fileset = new FilesetDataFunction(id, gen, validityPeriod, wdir, path, mask);
		if(!StringUtils.isEmpty(id)) context.set(id, fileset);
		return fileset;
	}

	protected DataFunction parseFile(Element root, long validityPeriod)
			throws InvalidMakefileException, IOException, InvalidContextVariableException {

		String id = getOptionalAttribute(root, "id");
		String path = getRequiredAttribute(root, "path");
		String gen_s = getOptionalAttribute(root, "generation");
		int generation;
		if (gen_s == null)
			generation = 0;
		else
			generation = Integer.parseInt(gen_s);
		FileDataFunction file = new FileDataFunction(id, generation, validityPeriod, wdir, path);
		if(!StringUtils.isEmpty(id)) context.set(id, file);
		return file;
	}

	protected DataFunction parseSet(Element root, long validityPeriod) throws InvalidMakefileException, InvalidContextVariableException, IOException{
		String id = getOptionalAttribute(root, "id");
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
		String reference = getOptionalAttribute(root, "idref");
		Object obj = context.getHamake(reference);
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
		res.setJar(Utils.resolvePath(Utils.replaceVariables(context, getRequiredAttribute(root, "jar")), wdir).toString());
		res.setMain(getRequiredAttribute(root, "main"));
		res.setParameters(parseParametersList(root));
		return res;
	}

	protected Task parsePigTask(Element root)
			throws InvalidMakefileException, IOException, PigNotFoundException {
		if (!isPigAvailable)
			throw new PigNotFoundException(
					"Pig isn't found in classpath. Please, make sure Pig classes are available in classpath.");

		Pig res = new Pig();
		res.setScript(Utils.resolvePath(Utils.replaceVariables(context, getRequiredAttribute(root, "script")), wdir));
		res.setParameters(parseParametersList(root));
		return res;
	}

	protected Task parseExecTask(Element root)
			throws InvalidMakefileException, IOException {
		Exec res = new Exec();
		res.setBinary(Utils.resolvePath(Utils.replaceVariables(context, getRequiredAttribute(root, "binary")), wdir));
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
        	else throw new InvalidMakefileException("'concat_function' attribute of " + getPath(root) + "contains unknown function");
        }
        else{
        	concatFunction = new CommaConcatFunction();
        }
        String processingFuncIdentificator = getOptionalAttribute(root, "processing_function");
        ProcessingFunction processingFunc = null;
        if(!StringUtils.isEmpty(processingFuncIdentificator)){
        	if("identity".equals(processingFuncIdentificator)) concatFunction = new SpaceConcatFunction();
        	else throw new InvalidMakefileException("'processing_function' attribute of " + getPath(root) + "contains unknown function");
        }
        else{
        	processingFunc = new IdentityProcessingFunction();
        }
        List<Object> values = new ArrayList<Object>();
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
        return new HamakeParameter(values, concatFunction, processingFunc);
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
