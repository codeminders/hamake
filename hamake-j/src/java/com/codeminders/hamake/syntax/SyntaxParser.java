package com.codeminders.hamake.syntax;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

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
import com.codeminders.hamake.params.ProcessingFunction;
import com.codeminders.hamake.params.Reference;
import com.codeminders.hamake.params.SpaceConcatFunction;
import com.codeminders.hamake.task.Exec;
import com.codeminders.hamake.task.MapReduce;
import com.codeminders.hamake.task.Pig;
import com.codeminders.hamake.task.Task;

public class SyntaxParser extends BaseSyntaxParser {
	
	public static final Pattern VARIABLE_PATTERN = Pattern.compile("\\$\\{([^\\}]+)\\}");

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

		parseProperties(dom, context);

		Hamake ret = new Hamake();
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

	protected void parseProperties(Document dom, Context context)
			throws InvalidMakefileException, InvalidContextVariableException {
		NodeList c = dom.getElementsByTagName("property");
		context.set(Hamake.SYS_PROPERTY_WORKING_FOLDER, wdir);

		for (int i = 0, sz = c.getLength(); i < sz; i++) {
			Element e = (Element) c.item(i);
			context.set(getRequiredAttribute(e, "name", VARIABLE_PATTERN, context), getRequiredAttribute(
					e, "value", VARIABLE_PATTERN, context));
		}
	}

	protected void parseDTRs(Hamake hamake, Element config, boolean verbose) throws InvalidMakefileException, IOException,
			PigNotFoundException, InvalidContextVariableException {
		NodeList tx = config.getElementsByTagName("foreach");
		for (int i = 0, sz = tx.getLength(); i < sz; i++) {
			Element t = (Element) tx.item(i);
			String dattr = getOptionalAttribute(t, "disabled", VARIABLE_PATTERN, context);
			if ("yes".equalsIgnoreCase(dattr) || "true".equalsIgnoreCase(dattr)) {
				if (verbose)
					System.out.println("Ignoring disabled task "
							+ getOptionalAttribute(t, "name", VARIABLE_PATTERN, context));
				continue;
			}
			hamake.addTask(parseForeachDTR(t));
		}
		tx = config.getElementsByTagName("fold");
		for (int i = 0, sz = tx.getLength(); i < sz; i++) {
			Element t = (Element) tx.item(i);
			String dattr = getOptionalAttribute(t, "disabled", VARIABLE_PATTERN, context);
			if ("yes".equalsIgnoreCase(dattr) || "true".equalsIgnoreCase(dattr)) {
				if (verbose)
					System.out.println("Ignoring disabled task "
							+ getOptionalAttribute(t, "name", VARIABLE_PATTERN, context));
				continue;
			}
			hamake.addTask(parseFoldDTR(t));
		}
	}

	protected DataTransformationRule parseForeachDTR(Element root)
			throws InvalidMakefileException, IOException, PigNotFoundException, InvalidContextVariableException {
		String name = getRequiredAttribute(root, "name", VARIABLE_PATTERN, context);

		Element input = getOneSubElement(root, "input");
		DataFunction inputFunc = parseDTRData(input, Arrays.asList("fileset"), 1).get(0);
		Element output = getOneSubElement(root, "output");
		List<DataFunction> outputFuncs = parseDTRData(output, Arrays.asList("fileset", "file", "set", "include"), Integer.MAX_VALUE);
		Element dependencies = getOneSubElement(root, "dependencies");
		List<DataFunction> deps = parseDTRData(dependencies, Arrays.asList("fileset", "file", "set", "include"), Integer.MAX_VALUE);

		Task task = parseTask(root, wdir);

		Foreach foreach = new Foreach(context, inputFunc, outputFuncs, deps);
		foreach.setName(name);
		foreach.setTask(task);
		return foreach;
	}

	protected DataTransformationRule parseFoldDTR(Element root) throws InvalidMakefileException, IOException, PigNotFoundException, InvalidContextVariableException {
        String name = getRequiredAttribute(root, "name", VARIABLE_PATTERN, context);

        Element input = getOneSubElement(root, "input");
        List<DataFunction> inputFuncs = parseDTRData(input, Arrays.asList("fileset", "file", "set", "include"), Integer.MAX_VALUE);
        Element output = getOneSubElement(root, "output");
        List<DataFunction> outputFuncs = parseDTRData(output, Arrays.asList("fileset", "file", "set", "include"), Integer.MAX_VALUE);
        Element dependencies = getOneSubElement(root, "dependencies");
        List<DataFunction> dependenciesFunc = parseDTRData(dependencies, Arrays.asList("fileset", "file", "set", "include"), Integer.MAX_VALUE);

        Task task = parseTask(root, wdir);

        Fold res = new Fold(context, inputFuncs, outputFuncs, dependenciesFunc);
        res.setName(name);
        res.setTask(task);
        return res;
    }
	
	protected List<DataFunction> parseDTRData(Element root, List<String> allowedSubElements, int subElementsAmount) throws InvalidMakefileException, IOException, InvalidContextVariableException {
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
		if(functions.size() < 1){
			throw new InvalidMakefileException(getPath(root) + " should have at least one of " + StringUtils.join(allowedSubElements, ","));
		}
		if(functions.size() > subElementsAmount){
			throw new InvalidMakefileException(getPath(root) + " should have no more then " + subElementsAmount + " sub-elements");
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
		String id = getOptionalAttribute(root, "id", VARIABLE_PATTERN, context);
		String path = getRequiredAttribute(root, "path", VARIABLE_PATTERN, context);
		String mask = getOptionalAttribute(root, "mask", VARIABLE_PATTERN, context);
		String generation = getOptionalAttribute(root, "generation", VARIABLE_PATTERN, context);
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

		String id = getOptionalAttribute(root, "id", VARIABLE_PATTERN, context);
		String path = getRequiredAttribute(root, "path", VARIABLE_PATTERN, context);
		String gen_s = getOptionalAttribute(root, "generation", VARIABLE_PATTERN, context);
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
		String id = getOptionalAttribute(root, "id", VARIABLE_PATTERN, context);
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
		Object obj = context.get(reference);
		if(obj != null || !(obj instanceof DataFunction)){
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
		res.setJar(Utils.resolvePath(wdir,
				getRequiredAttribute(root, "jar", VARIABLE_PATTERN, context)).toString());
		res.setMain(getRequiredAttribute(root, "main", VARIABLE_PATTERN, context));
		res.setParameters(parseParametersList(root));
		return res;
	}

	protected Task parsePigTask(Element root)
			throws InvalidMakefileException, IOException, PigNotFoundException {
		if (!isPigAvailable)
			throw new PigNotFoundException(
					"Pig isn't found in classpath. Please, make sure Pig classes are available in classpath.");

		Pig res = new Pig();
		res.setScript(Utils.resolvePath(wdir, getRequiredAttribute(root, "script",
				VARIABLE_PATTERN, context)));
		res.setParameters(parseParametersList(root));
		return res;
	}

	protected Task parseExecTask(Element root)
			throws InvalidMakefileException, IOException {
		Exec res = new Exec();
		res.setBinary(Utils.resolvePath(wdir, getRequiredAttribute(root, "binary",
				VARIABLE_PATTERN, context)));
		res.setParameters(parseParametersList(root));
		return res;
	}

	protected List<HamakeParameter> parseParametersList(Element root) throws InvalidMakefileException {
		List<HamakeParameter> parameters = new ArrayList<HamakeParameter>();
		NodeList children = root.getChildNodes();
		int counter = 0;
		for (int i = 0; i < children.getLength(); i++) {
			if (children.item(i).getNodeType() == Node.ELEMENT_NODE) {
				Element element = (Element)children.item(i);
				if ("parameter".equals(element.getNodeName())) {
					parameters.add(parseParameter(element, counter));
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
					values.add(new Reference(getOptionalAttribute(root, "idref")));
				}
				else if ("literal".equals(element.getNodeName())) {
					values.add(new Literal(getOptionalAttribute(root, "value")));
				}
				else{
					throw new InvalidMakefileException("unknown sub-element found whithin " + getPath(root));
				}
			}
		}
        return new HamakeParameter(values, concatFunction, processingFunc);
    }
	
	protected JobConfParam parseJobConfParameter(Element root, Map<String, String> properties) throws InvalidMakefileException {
        return new JobConfParam(getRequiredAttribute(root, "name", VARIABLE_PATTERN, context),
                getRequiredAttribute(root, "value", VARIABLE_PATTERN, context));
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
