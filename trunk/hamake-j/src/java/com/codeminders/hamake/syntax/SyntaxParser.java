package com.codeminders.hamake.syntax;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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

import com.codeminders.hamake.Command;
import com.codeminders.hamake.Hamake;
import com.codeminders.hamake.HamakePath;
import com.codeminders.hamake.Param;
import com.codeminders.hamake.HamakeParameter;
import com.codeminders.hamake.PigNotFoundException;
import com.codeminders.hamake.Task;
import com.codeminders.hamake.commands.ExecCommand;
import com.codeminders.hamake.commands.HadoopCommand;
import com.codeminders.hamake.commands.PigCommand;
import com.codeminders.hamake.params.JobConfParam;
import com.codeminders.hamake.params.PathParam;
import com.codeminders.hamake.tasks.MapTask;
import com.codeminders.hamake.tasks.ReduceTask;

public class SyntaxParser extends BaseSyntaxParser {
	
	protected static final Pattern VARIABLE_PATTERN = Pattern.compile("\\$\\{([^\\}]+)\\}");

	Map<String, String> properties;
	Document dom;
	String wdir;
	boolean verbose;
	
	protected SyntaxParser(Document dom, String wdir, boolean verbose){
		this.dom = dom;
		this.wdir = wdir;
		this.verbose = verbose;
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

	protected Task parseForeachDTR(Element root, String wdir)
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

		List<HamakePath> outputs = parseForeachOutput(root, "output", wdir);
		List<HamakePath> deps = parseDependencies(root, "dependencies", wdir);

		Command command = parseTask(root, wdir);

		// Sanity checks
		if (input != null) {
			boolean itype = input.hasFilename();
			for (HamakePath p : outputs)
				if (p.hasFilename() != itype)
					throw new InvalidMakefileException(
							"Input/Output type/file mismatch for MAP task "
									+ name);
		}

		MapTask res = new MapTask();
		res.setName(name);
		res.setOutputs(outputs);
		res.setDeps(deps);
		res.setCommand(command);
		res.setXinput(input);
		return res;
	}

	protected Task parseFoldDTR(Element root, String wdir) throws InvalidMakefileException, IOException, PigNotFoundException {
        String name = getRequiredAttribute(root, "name", properties, VARIABLE_PATTERN);

        List<HamakePath> inputs = parseFoldData(root, "input", wdir);
        List<HamakePath> outputs = parseFoldData(root, "output", wdir);
        List<HamakePath> deps = parseDependencies(root, "dependencies", wdir);

        Command command = parseTask(root, wdir);

        ReduceTask res = new ReduceTask();
        res.setName(name);
        res.setOutputs(outputs);
        res.setCommand(command);
        res.setInputs(inputs);
        res.setDeps(deps);
        return res;
    }
	
	protected List<HamakePath> parseFoldData(Element root, String name,
			String wdir) throws InvalidMakefileException, IOException {
		List<HamakePath> ret = new ArrayList<HamakePath>();
		Element output = getOneSubElement(root, name);
		if(output != null){
			String expiration = getOptionalAttribute(output, "expiration");
			long validityPeriod = parseValidityPeriod(expiration);
			NodeList files = output.getElementsByTagName(
			"file");
			for (int i = 0, sz = files.getLength(); i < sz; i++) {
				ret.add(parseFile((Element) files.item(i), wdir, validityPeriod));
			}
			NodeList filesets = output.getElementsByTagName(
			"fileset");			
			for (int i = 0, sz = filesets.getLength(); i < sz; i++) {
				ret.add(parseFileset((Element) filesets.item(i), wdir));
			}
		}
		return ret;
	}
	
	protected List<HamakePath> parseForeachInput(Element root, String name,
			String wdir) throws InvalidMakefileException, IOException {
		NodeList path = getOneSubElement(root, name).getElementsByTagName(
				"fileset");
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
		return new HamakePath(id, wdir, path, mask, gen);
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
		return new HamakePath(id, wdir, path, null, mask, generation, variant, validityPeriod);
	}

	protected List<HamakePath> parseForeachOutput(Element root, String name,
			String wdir) throws InvalidMakefileException, IOException {
		List<HamakePath> paths = new ArrayList<HamakePath>();				
		Element output = getOneSubElement(root, name);
		String expiration = getOptionalAttribute(output, "expiration");
		long validityPeriod = parseValidityPeriod(expiration);
		Element composition = getOneSubElement(output, "composition");
		if(composition == null){
			Element identity = getOneSubElement(output, "identity");
			if(identity == null){
				throw new InvalidMakefileException("Element '" + getPath(output)
						+ "' should have one composition or identity sub-elements");
			}
			paths.add(parseIdentity(identity, wdir, validityPeriod));
		}
		else{
			NodeList identities = composition.getElementsByTagName("identity");
			if(identities.getLength() > 0){
				for(int i = 0, sz = identities.getLength(); i < sz; i++) {
					paths.add(parseIdentity((Element) identities.item(i), wdir, validityPeriod));
				}
			}
			else{
				throw new InvalidMakefileException("Element '" + getPath(composition)
						+ "' should have at least one identity sub-element");
			}
		}
		return paths;
	}

	protected HamakePath parseIdentity(Element root, String wdir, long validityPeriod)
			throws InvalidMakefileException, IOException {

		String path = getRequiredAttribute(root, "path", properties, VARIABLE_PATTERN);
		String gen_s = getOptionalAttribute(root, "generation");
		int gen;
        if (StringUtils.isEmpty(gen_s) || !StringUtils.isNumeric(gen_s))
            gen = 0;
        else
            gen = Integer.parseInt(gen_s);
		return new HamakePath(path, validityPeriod, gen);
	}

	protected Command parseTask(Element root, String wdir)
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

	protected Command parseMapReduceTask(Element root,
			Map<String, String> properties, String wdir)
			throws InvalidMakefileException {
		HadoopCommand res = new HadoopCommand();
		res.setJar(HamakePath.resolve(wdir,
				getRequiredAttribute(root, "jar", properties, VARIABLE_PATTERN)).toString());
		res.setMain(getRequiredAttribute(root, "main", properties, VARIABLE_PATTERN));
		res.setParameters(parseParametersList(root, properties));
		return res;
	}

	protected Command parsePigTask(Element root,
			Map<String, String> properties, String wdir)
			throws InvalidMakefileException, IOException, PigNotFoundException {
		if (!isPigAvailable)
			throw new PigNotFoundException(
					"Pig isn't found in classpath. Please, make sure Pig classes are available in classpath.");

		PigCommand res = new PigCommand();
		res.setScript(new HamakePath(wdir, getRequiredAttribute(root, "script",
				properties, VARIABLE_PATTERN)));
		res.setParameters(parseParametersList(root, properties));
		return res;
	}

	protected Command parseExecTask(Element root,
			Map<String, String> properties, String wdir)
			throws InvalidMakefileException, IOException {
		ExecCommand res = new ExecCommand();
		res.setBinary(new HamakePath(wdir, getRequiredAttribute(root, "binary",
				properties, VARIABLE_PATTERN)));
		res.setParameters(parseParametersList(root, properties));
		return res;
	}

	protected List<Param> parseParametersList(Element root,
			Map<String, String> properties) throws InvalidMakefileException {
		List<Param> ret = new ArrayList<Param>();
		NodeList children = root.getChildNodes();
		int counter = 0;
		for (int i = 0, sz = children.getLength(); i < sz; i++) {
			Node n = children.item(i);
			Param param;
			if (n.getNodeType() == Node.ELEMENT_NODE) {
				String name = n.getNodeName();
				counter++;
				Element e = (Element) n;
				if ("parameter".equals(name)) {
					param = parseParameter(e, properties, counter);
				} else if ("jobconf".equals(name)) {
					param = parseJobConfParameter(e, properties);
				} else {
					throw new InvalidMakefileException("Unknown sub-element '"
							+ name + "' under " + getPath(root));
				}
				ret.add(param);
			}
		}
		return ret;
	}
	
	protected Param parseParameter(Element root, Map<String, String> properties, int index) throws InvalidMakefileException {
        String value = getRequiredAttribute(root, "value");
        return new HamakeParameter(value);
    }
	
	protected Param parseJobConfParameter(Element root, Map<String, String> properties) throws InvalidMakefileException {
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
