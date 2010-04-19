package com.codeminders.hamake.syntax;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.commons.lang.StringUtils;
import org.mortbay.util.StringUtil;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.codeminders.hamake.Command;
import com.codeminders.hamake.Context;
import com.codeminders.hamake.Hamake;
import com.codeminders.hamake.HamakePath;
import com.codeminders.hamake.PigNotFoundException;
import com.codeminders.hamake.dtr.DataTransformationRule;
import com.codeminders.hamake.dtr.Fold;
import com.codeminders.hamake.dtr.Foreach;
import com.codeminders.hamake.dtr.GroupOutputFunction;
import com.codeminders.hamake.dtr.IdentityOutputFunction;
import com.codeminders.hamake.dtr.IncludeOutputFunction;
import com.codeminders.hamake.dtr.OutputFunction;
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

	Map<String, String> properties;
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
			InvalidMakefileException, PigNotFoundException {

		properties = parseProperties(dom, wdir);

		Hamake ret = new Hamake();
		parseDTRs(ret, dom.getDocumentElement(), verbose, wdir);
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

	protected Map<String, String> parseProperties(Document dom, String wdir)
			throws InvalidMakefileException {
		NodeList c = dom.getElementsByTagName("property");
		Map<String, String> ret = new HashMap<String, String>();
		ret.put("workdir", wdir);

		for (int i = 0, sz = c.getLength(); i < sz; i++) {
			Element e = (Element) c.item(i);
			ret.put(getRequiredAttribute(e, "name", ret, VARIABLE_PATTERN), getRequiredAttribute(
					e, "value", ret, VARIABLE_PATTERN));
		}
		return ret;
	}

	protected void parseDTRs(Hamake hamake, Element config, boolean verbose,
			String wdir) throws InvalidMakefileException, IOException,
			PigNotFoundException {
		NodeList tx = config.getElementsByTagName("foreach");
		for (int i = 0, sz = tx.getLength(); i < sz; i++) {
			Element t = (Element) tx.item(i);
			String dattr = getOptionalAttribute(t, "disabled", properties, VARIABLE_PATTERN);
			if ("yes".equalsIgnoreCase(dattr) || "true".equalsIgnoreCase(dattr)) {
				if (verbose)
					System.out.println("Ignoring disabled task "
							+ getOptionalAttribute(t, "name", properties, VARIABLE_PATTERN));
				continue;
			}
			hamake.addTask(parseForeachDTR(t, wdir));
		}
		tx = config.getElementsByTagName("fold");
		for (int i = 0, sz = tx.getLength(); i < sz; i++) {
			Element t = (Element) tx.item(i);
			String dattr = getOptionalAttribute(t, "disabled", properties, VARIABLE_PATTERN);
			if ("yes".equalsIgnoreCase(dattr) || "true".equalsIgnoreCase(dattr)) {
				if (verbose)
					System.out.println("Ignoring disabled task "
							+ getOptionalAttribute(t, "name", properties, VARIABLE_PATTERN));
				continue;
			}
			hamake.addTask(parseFoldDTR(t, wdir));
		}
	}

	protected DataTransformationRule parseForeachDTR(Element root, String wdir)
			throws InvalidMakefileException, IOException, PigNotFoundException {
		String name = getRequiredAttribute(root, "name", properties, VARIABLE_PATTERN);

		Collection<HamakePath> inputs = parseForeachInput(root, "input", wdir);
		HamakePath input;
		if (inputs.size() == 0)
			input = null;
		else if (inputs.size() == 1)
			input = inputs.iterator().next();
		else
			throw new InvalidMakefileException(
					"Multiple 'input' elements in MAP task '%s' are not permitted"
							+ name);

		OutputFunction outputFunc = parseOutputFunction(root, wdir);
		List<HamakePath> deps = parseDependencies(root, "dependencies", wdir);

		Task task = parseTask(root, wdir);

		Foreach res = new Foreach(context, Arrays.asList(input), outputFunc, deps);
		res.setName(name);
		res.setTask(task);
		return res;
	}

	protected DataTransformationRule parseFoldDTR(Element root, String wdir) throws InvalidMakefileException, IOException, PigNotFoundException {
        String name = getRequiredAttribute(root, "name", properties, VARIABLE_PATTERN);

        Element input = getOneSubElement(root, "input");
        List<HamakePath> inputs = parseFoldData(input, wdir, 0);        
        Element output = getOneSubElement(root, "output");
        String expiration = getOptionalAttribute(output, "expiration");
        long validityPeriod = parseValidityPeriod(expiration);
        List<HamakePath> outputs = parseFoldData(output, wdir, validityPeriod);
        List<HamakePath> deps = parseDependencies(root, "dependencies", wdir);

        Task task = parseTask(root, wdir);

        Fold res = new Fold(context, inputs, outputs, deps);
        res.setName(name);
        res.setTask(task);
        return res;
    }
	
	protected List<HamakePath> parseFoldData(Element root, String wdir, long validityPeriod) throws InvalidMakefileException, IOException {
		List<HamakePath> ret = new ArrayList<HamakePath>();
		NodeList files = root.getElementsByTagName("file");
		for (int i = 0, sz = files.getLength(); i < sz; i++) {
			ret.add(parseFile((Element) files.item(i), wdir, validityPeriod));
		}
		NodeList filesets = root.getElementsByTagName("fileset");			
		for (int i = 0, sz = filesets.getLength(); i < sz; i++) {
			ret.add(parseFileset((Element) filesets.item(i), wdir));
		}
		if(filesets.getLength() < 1 && files.getLength() < 1){
			throw new InvalidMakefileException(getPath(root) + " should have at least one file or fileset element");
		}
		return ret;
	}
	
	protected List<HamakePath> parseForeachInput(Element root, String name,
			String wdir) throws InvalidMakefileException, IOException {
		NodeList path = getOneSubElement(root, name).getElementsByTagName(
				"fileset");
		if(path == null || path.getLength() != 1){
			throw new InvalidMakefileException(getPath(root) + " should have at least one fileset element");
		}
		List<HamakePath> ret = new ArrayList<HamakePath>();
		for (int i = 0, sz = path.getLength(); i < sz; i++) {
			ret.add(parseFileset((Element) path.item(i), wdir));
		}
		return ret;
	}

	@SuppressWarnings( { "unchecked" })
	protected List<HamakePath> parseDependencies(Element root, String name,
			String wdir) throws InvalidMakefileException, IOException {
		Element dependency = getOneSubElement(root, name);
		if (dependency != null) {
			NodeList files = dependency.getElementsByTagName("file");
			List<HamakePath> ret = new ArrayList<HamakePath>();
			for (int i = 0, sz = files.getLength(); i < sz; i++) {
				ret.add(parseFile((Element) files.item(i), wdir, Long.MAX_VALUE));
			}
			return ret;
		}
		return Collections.EMPTY_LIST;
	}

	protected HamakePath parseFileset(Element root, String wdir)
			throws InvalidMakefileException, IOException {

		String id = getOptionalAttribute(root, "id", properties, VARIABLE_PATTERN);
		String path = getRequiredAttribute(root, "path", properties, VARIABLE_PATTERN);
		String mask = getOptionalAttribute(root, "mask", properties, VARIABLE_PATTERN);
		String generation = getOptionalAttribute(root, "generation", properties, VARIABLE_PATTERN);
		int gen;
		if (generation == null)
			gen = 0;
		else
			gen = Integer.parseInt(generation);
		HamakePath fileset = new HamakePath(id, wdir, path, mask, gen);
		if(!StringUtils.isEmpty(id)) context.set(id, fileset);
		return fileset;
	}

	protected HamakePath parseFile(Element root, String wdir, long validityPeriod)
			throws InvalidMakefileException, IOException {

		String id = getOptionalAttribute(root, "id", properties, VARIABLE_PATTERN);
		String path = getRequiredAttribute(root, "path", properties, VARIABLE_PATTERN);
		String var_s = getOptionalAttribute(root, "variant", properties, VARIABLE_PATTERN);
		String mask = getOptionalAttribute(root, "mask", properties, VARIABLE_PATTERN);
		String gen_s = getOptionalAttribute(root, "generation", properties, VARIABLE_PATTERN);
		int generation;
		if (gen_s == null)
			generation = 0;
		else
			generation = Integer.parseInt(gen_s);
		HamakePath.Variant variant = HamakePath.Variant.parseString(var_s);
		if(variant == HamakePath.Variant.MASK && StringUtils.isEmpty(mask)){
			throw new InvalidMakefileException("You can not specify 'variant' attribute without 'mask' in'" + getPath(root));
		}
		HamakePath file = new HamakePath(id, wdir, path, null, mask, generation, variant, validityPeriod);
		if(!StringUtils.isEmpty(id)){
			context.set(id, file);
		}
		return file;
	}

	protected OutputFunction parseOutputFunction(Element root, String wdir)
			throws InvalidMakefileException, IOException {

		Element output = getOneSubElement(root, "output");
		String expiration = getOptionalAttribute(output, "expiration");
		long validityPeriod = parseValidityPeriod(expiration);
		NodeList children = output.getChildNodes();		
		for(int i = 0; i < children.getLength(); i ++){
			if (children.item(i).getNodeType() == Node.ELEMENT_NODE) {
				Element child = (Element)children.item(i);
				if(child.getNodeName().equals("set")){
					return parseSet(child, wdir, validityPeriod);
				}
				else if(child.getNodeName().equals("file")){
					return parseFileOutputFunction(child, wdir, validityPeriod);
				}
				else if(child.getNodeName().equals("include")){
					return parseIncludeOutputFunction(child, wdir, validityPeriod);
				}
			}
		}
		throw new InvalidMakefileException(getPath(root) + " should have at least one of set, file or include elements");
	}
	
	protected OutputFunction parseSet(Element root, String wdir, long validityPeriod) throws InvalidMakefileException{
		GroupOutputFunction outputFunction = new GroupOutputFunction();
		NodeList children = root.getChildNodes();		
		for(int i = 0; i < children.getLength(); i ++){
			if (children.item(i).getNodeType() == Node.ELEMENT_NODE) {
				Element child = (Element)children.item(i);
				if(child.getNodeName().equals("set")){
					outputFunction.addOutputFunction(parseSet(child, wdir, validityPeriod));
				}
				else if(child.getNodeName().equals("file")){
					outputFunction.addOutputFunction(parseFileOutputFunction(child, wdir, validityPeriod));
				}
				else if(child.getNodeName().equals("include")){
					outputFunction.addOutputFunction(parseIncludeOutputFunction(child, wdir, validityPeriod));
				}
			}
		}
		return new GroupOutputFunction();
	}
	
	protected OutputFunction parseFileOutputFunction(Element root, String wdir, long validityPeriod) throws InvalidMakefileException{
		String id = getOptionalAttribute(root, "id");	
		String gen_s = getOptionalAttribute(root, "generation");
		int gen;
        if (StringUtils.isEmpty(gen_s) || !StringUtils.isNumeric(gen_s))
            gen = 0;
        else
            gen = Integer.parseInt(gen_s);
		String functionString = getRequiredAttribute(root, "path");
		IdentityOutputFunction func = new IdentityOutputFunction(functionString, validityPeriod);
		if(!StringUtils.isEmpty(id)){
			context.set(id, func);
		}
		return func;
	}
	
	protected OutputFunction parseIncludeOutputFunction(Element root, String wdir, long validityPeriod){
		String reference = getOptionalAttribute(root, "idref");
		return new IncludeOutputFunction(reference);
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
			return parseMapReduceTask(task, properties, wdir);
		} else if (task.getNodeName().equals("pig")) {
			return parsePigTask(task, properties, wdir);
		} else if (task.getNodeName().equals("exec")) {
			return parseExecTask(task, properties, wdir);
		} else {
			throw new InvalidMakefileException("Unknown task type found in '"
					+ root.getNodeName() + "' element");
		}
	}

	protected Task parseMapReduceTask(Element root,
			Map<String, String> properties, String wdir)
			throws InvalidMakefileException {
		MapReduce res = new MapReduce();
		res.setJar(HamakePath.resolve(wdir,
				getRequiredAttribute(root, "jar", properties, VARIABLE_PATTERN)).toString());
		res.setMain(getRequiredAttribute(root, "main", properties, VARIABLE_PATTERN));
		res.setParameters(parseParametersList(root, properties));
		return res;
	}

	protected Task parsePigTask(Element root,
			Map<String, String> properties, String wdir)
			throws InvalidMakefileException, IOException, PigNotFoundException {
		if (!isPigAvailable)
			throw new PigNotFoundException(
					"Pig isn't found in classpath. Please, make sure Pig classes are available in classpath.");

		Pig res = new Pig();
		res.setScript(new HamakePath(wdir, getRequiredAttribute(root, "script",
				properties, VARIABLE_PATTERN)));
		res.setParameters(parseParametersList(root, properties));
		return res;
	}

	protected Task parseExecTask(Element root,
			Map<String, String> properties, String wdir)
			throws InvalidMakefileException, IOException {
		Exec res = new Exec();
		res.setBinary(new HamakePath(wdir, getRequiredAttribute(root, "binary",
				properties, VARIABLE_PATTERN)));
		res.setParameters(parseParametersList(root, properties));
		return res;
	}

	protected List<HamakeParameter> parseParametersList(Element root,
			Map<String, String> properties) throws InvalidMakefileException {
		List<HamakeParameter> parameters = new ArrayList<HamakeParameter>();
		NodeList children = root.getChildNodes();
		int counter = 0;
		for (int i = 0; i < children.getLength(); i++) {
			if (children.item(i).getNodeType() == Node.ELEMENT_NODE) {
				Element element = (Element)children.item(i);
				if ("parameter".equals(element.getNodeName())) {
					parameters.add(parseParameter(element, properties, counter));
				}
			}
		}
		return parameters;
	}
	
	protected HamakeParameter parseParameter(Element root, Map<String, String> properties, int index) throws InvalidMakefileException {
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
		int counter = 0;
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
        return new JobConfParam(getRequiredAttribute(root, "name", properties, VARIABLE_PATTERN),
                getRequiredAttribute(root, "value", properties, VARIABLE_PATTERN));
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
