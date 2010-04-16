package com.codeminders.hamake.syntax;

import com.codeminders.hamake.Command;
import com.codeminders.hamake.Hamake;
import com.codeminders.hamake.HamakePath;
import com.codeminders.hamake.PigNotFoundException;
import com.codeminders.hamake.Task;
import com.codeminders.hamake.commands.ExecCommand;
import com.codeminders.hamake.commands.HadoopCommand;
import com.codeminders.hamake.commands.PigCommand;
import com.codeminders.hamake.params.ConstParam;
import com.codeminders.hamake.params.JobConfParam;
import com.codeminders.hamake.params.Param;
import com.codeminders.hamake.params.PathParam;
import com.codeminders.hamake.params.PigParam;
import com.codeminders.hamake.tasks.MapTask;
import com.codeminders.hamake.tasks.ReduceTask;

import org.apache.commons.lang.StringUtils;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

public class PhytonSyntaxParser extends BaseSyntaxParser{
	
	public static final Pattern VARIABLE_PATTERN = Pattern.compile("%\\(([^\\)]+)\\)[sdiefc]");
	
	Document dom;
	String wdir;
	boolean verbose;
	
	protected PhytonSyntaxParser(Document dom, String wdir, boolean verbose){
		this.dom = dom;
		this.wdir = wdir;
		this.verbose = verbose;
	}

	@Override
    protected Hamake parseSyntax() throws IOException,
            ParserConfigurationException,
            SAXException,
            InvalidMakefileException,
            PigNotFoundException {
        
        Element config = parseConfig(dom);
        Map<String, String> properties = parseProperties(config, wdir);

        Hamake ret = new Hamake();               
        parseTasks(ret, dom.getDocumentElement(), properties, verbose, wdir);
        String defaultTask = dom.getDocumentElement().getAttribute("default");
        ret.setProjectName(dom.getDocumentElement().getAttribute("name"));
        if(!StringUtils.isEmpty(defaultTask)){        	
        	ret.setDefaultTarget(defaultTask);
        }
        return ret;
    }    
    
    @Override
    protected boolean isCorrectParser(){
		return (dom.getElementsByTagName("map").getLength() > 0) || (dom.getElementsByTagName("reduce").getLength() > 0);
	}

    protected Element parseConfig(Document dom) throws InvalidMakefileException {
        return getMandatory(dom.getDocumentElement(), "config");
    }        

    protected Map<String, String> parseProperties(Element root, String wdir) throws InvalidMakefileException {
        NodeList c = root.getElementsByTagName("property");
        Map<String, String> ret = new HashMap<String, String>() {
            String get(String key) {
                if ("timestamp".equals(key))
                    return String.valueOf(System.currentTimeMillis() / 1000L);
                return super.get(key);
            }
        };
        ret.put("workdir", wdir);
        
        for (int i = 0, sz = c.getLength(); i < sz; i++) {
            Element e = (Element) c.item(i);
            ret.put(getRequiredAttribute(e, "name", ret, VARIABLE_PATTERN),
                    getRequiredAttribute(e, "value", ret, VARIABLE_PATTERN));
        }                  
        return ret;
    }

    protected void parseTasks(Hamake hamake,
                              Element config,
                              Map<String, String> properties,
                              boolean verbose,
                              String wdir) throws InvalidMakefileException, IOException, PigNotFoundException {
        NodeList tx = config.getElementsByTagName("map");
        for (int i = 0, sz = tx.getLength(); i < sz; i++) {
            Element t = (Element) tx.item(i);
            String dattr = getOptionalAttribute(t, "disabled", properties, VARIABLE_PATTERN);
            if ("yes".equalsIgnoreCase(dattr) || "true".equalsIgnoreCase(dattr)) {
                if (verbose)
                    System.out.println("Ignoring disabled task " + getOptionalAttribute(t, "name", properties, VARIABLE_PATTERN));
                continue;
            }
            hamake.addTask(parseMapTask(t, properties, wdir));
        }
        tx = config.getElementsByTagName("reduce");
        for (int i = 0, sz = tx.getLength(); i < sz; i++) {
            Element t = (Element) tx.item(i);
            String dattr = getOptionalAttribute(t, "disabled", properties, VARIABLE_PATTERN);
            if ("yes".equalsIgnoreCase(dattr) || "true".equalsIgnoreCase(dattr)) {
                if (verbose)
                    System.out.println("Ignoring disabled task " + getOptionalAttribute(t, "name", properties, VARIABLE_PATTERN));
                continue;
            }
            hamake.addTask(parseReduceTask(t, properties, wdir));
        }
    }

    protected Task parseMapTask(Element root, Map<String, String> properties, String wdir) throws InvalidMakefileException, IOException, PigNotFoundException {
        String name = getRequiredAttribute(root, "name", properties, VARIABLE_PATTERN);

        Collection<HamakePath> inputs = parsePathList(root, "input", properties, wdir);
        HamakePath input;
        if (inputs.size() == 0)
            input = null;
        else if (inputs.size() == 1)
            input = inputs.iterator().next();
        else
            throw new InvalidMakefileException("Multiple 'input' elements in MAP task '%s' are not permitted" + name);

        List<HamakePath> outputs = parsePathList(root, "output", properties, wdir);
        List<HamakePath> deps = parsePathList(root, "dependencies", properties, wdir);

        Command command = parseCommand(root, properties, wdir);

        // Sanity checks
        if (input != null) {
            boolean itype = input.hasFilename();
            for (HamakePath p : outputs)
                if (p.hasFilename() != itype)
                    throw new InvalidMakefileException("Input/Output type/file mismatch for MAP task " + name);
        }

        MapTask res = new MapTask();
        res.setName(name);
        res.setOutputs(outputs);
        res.setDeps(deps);
        res.setCommand(command);
        res.setXinput(input);
        parseTaskDeps(root, res, properties);
        return res;
    }

    protected Command parseCommand(Element root, Map<String, String> properties, String wdir) throws InvalidMakefileException, IOException, PigNotFoundException {
        NodeList list = root.getElementsByTagName("task");
        int size = list.getLength();
        if (size > 1)
            throw new InvalidMakefileException("Multiple elements 'task' in " + getPath(root) + " are not permitted");
        if (size == 1)
            return parseHadoopCommand((Element) list.item(0), properties, wdir);

        list = root.getElementsByTagName("pig");
        size = list.getLength();
        if (size > 1)
            throw new InvalidMakefileException("Multiple elements 'pig' in " + getPath(root) + " are not permitted");
        if (size == 1)
            return parsePigCommand((Element) list.item(0), properties, wdir);

        list = root.getElementsByTagName("exec");
        size = list.getLength();
        if (size > 1)
            throw new InvalidMakefileException("Multiple elements 'exec' in " + getPath(root) + " are not permitted");
        if (size == 1)
            return parseExecCommand((Element) list.item(0), properties, wdir);

        throw new InvalidMakefileException("No commands are encountered in " + getPath(root));
    }

    protected Command parseHadoopCommand(Element root, Map<String, String> properties, String wdir) throws InvalidMakefileException {
        HadoopCommand res = new HadoopCommand();
        res.setJar(HamakePath.resolve(wdir, getRequiredAttribute(root, "jar", properties, VARIABLE_PATTERN)).toString());
        res.setMain(getRequiredAttribute(root, "main", properties, VARIABLE_PATTERN));
        res.setParameters(parseParameters(root, properties));
        return res;
    }
    
    protected Command parsePigCommand(Element root, Map<String, String> properties, String wdir) throws InvalidMakefileException, IOException, PigNotFoundException {
        if (!isPigAvailable)
            throw new PigNotFoundException("Pig isn't found in classpath. Please, make sure Pig classes are available in classpath.");

        PigCommand res = new PigCommand();
        res.setScript(new HamakePath(wdir, getRequiredAttribute(root, "script", properties, VARIABLE_PATTERN)));
        res.setParameters(parseParameters(root, properties));
        return res;
    }

    protected Command parseExecCommand(Element root, Map<String, String> properties, String wdir) throws InvalidMakefileException, IOException {
        ExecCommand res = new ExecCommand();
        res.setBinary(new HamakePath(wdir, getRequiredAttribute(root, "binary", properties, VARIABLE_PATTERN)));
        res.setParameters(parseParameters(root, properties));
        return res;
    }

    protected Task parseReduceTask(Element root, Map<String, String> properties, String wdir) throws InvalidMakefileException, IOException, PigNotFoundException {
        String name = getRequiredAttribute(root, "name", properties, VARIABLE_PATTERN);

        List<HamakePath> inputs = parsePathList(root, "input", properties, wdir);
        List<HamakePath> outputs = parsePathList(root, "output", properties, wdir);

        Command command = parseCommand(root, properties, wdir);

        ReduceTask res = new ReduceTask();
        res.setName(name);
        res.setOutputs(outputs);
        res.setCommand(command);
        res.setInputs(inputs);
        parseTaskDeps(root, res, properties);
        return res;
    }

    protected List<Param> parseParameters(Element root, Map<String, String> properties)
            throws InvalidMakefileException {
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
                if ("constparam".equals(name)) {
                    param = parseConstParam(e, properties);
                } else if ("pathparam".equals(name)) {
                    param = parsePathParam(e, properties, counter);
                } else if ("jobconfparam".equals(name)) {
                    param = parseJobConfParam(e, properties);
                } else if ("pigparam".equals(name)) {
                    param = parsePigParam(e, properties);
                } else {
                    throw new InvalidMakefileException("Unknown sub-element '" + name + "' under " + getPath(root));
                }
                ret.add(param);
            }
        }
        return ret;
    }

    protected Param parseConstParam(Element root, Map<String, String> properties) throws InvalidMakefileException {
        return new ConstParam(getRequiredAttribute(root, "value", properties, VARIABLE_PATTERN));
    }

    protected Param parsePathParam(Element root, Map<String, String> properties, int index) throws InvalidMakefileException {
        String ptype = getRequiredAttribute(root, "type", properties, VARIABLE_PATTERN);
        String number_s = getOptionalAttribute(root, "number", properties, VARIABLE_PATTERN);
        int number;
        if (number_s != null)
            number = Integer.parseInt(number_s);
        else
            number = -1;
        String mask_handling = getOptionalAttribute(root, "mask", properties, VARIABLE_PATTERN);
        PathParam.Mask mask;
        if (mask_handling == null)
            mask = PathParam.Mask.keep;
        else {
            try {
                mask = PathParam.Mask.valueOf(mask_handling);
            } catch (IllegalArgumentException ex) {
                throw new InvalidMakefileException("Unsupported value of 'mask' parameter in " + getPath(root));
            }
        }
        String name = getOptionalAttribute(root, "name", properties, VARIABLE_PATTERN);
        if (name == null)
            name = "path" + index;
        return new PathParam(name, ptype, number, mask);
    }

    protected Param parseJobConfParam(Element root, Map<String, String> properties) throws InvalidMakefileException {
        return new JobConfParam(getRequiredAttribute(root, "name", properties, VARIABLE_PATTERN),
                getRequiredAttribute(root, "value", properties, VARIABLE_PATTERN));
    }

    protected Param parsePigParam(Element root, Map<String, String> properties) throws InvalidMakefileException {
        return new PigParam(getRequiredAttribute(root, "name", properties, VARIABLE_PATTERN),
                getRequiredAttribute(root, "value", properties, VARIABLE_PATTERN));
    }

    protected void parseTaskDeps(Element root, Task res, Map<String, String> properties)
            throws InvalidMakefileException {
        NodeList list = root.getElementsByTagName("taskdep");
        int size = list.getLength();
        if (size == 0)
            return;
        if (size > 1)
            throw new InvalidMakefileException("Multiple elements 'taskdep' in " + res.getName() + " are not permitted");

        NodeList pretasks = ((Element) list.item(0)).getElementsByTagName("pretask");

        Collection<String> deps = new ArrayList<String>();
        for (int i = 0, sz = pretasks.getLength(); i < sz; i++) {
            String name = getRequiredAttribute((Element) pretasks.item(i), "name", properties, VARIABLE_PATTERN);
            if (!name.equals(res.getName()))
                deps.add(name);
        }
        res.setTaskDeps(deps);
    }

    @SuppressWarnings({"unchecked"})
    protected List<HamakePath> parsePathList(Element root, String name, Map<String, String> properties, String wdir)
            throws InvalidMakefileException, IOException {
        NodeList list = root.getElementsByTagName(name);
        int len = list.getLength();
        if (len == 0)
            return Collections.EMPTY_LIST;
        if (len != 1)
            throw new InvalidMakefileException("Multiple elements '" + name + "' in " + getPath(root) +
                    " are not permitted");
        NodeList path = ((Element) list.item(0)).getElementsByTagName("path");
        List<HamakePath> ret = new ArrayList<HamakePath>();
        for (int i = 0, sz = path.getLength(); i < sz; i++) {
            ret.add(parsePath((Element) path.item(i), properties, wdir));
        }
        return ret;
    }

    protected HamakePath parsePath(Element root, Map<String, String> properties, String wdir) throws InvalidMakefileException, IOException {

        String location = getRequiredAttribute(root, "location", properties, VARIABLE_PATTERN);
        String filename = getOptionalAttribute(root, "filename", properties, VARIABLE_PATTERN);
        String mask = getOptionalAttribute(root, "mask", properties, VARIABLE_PATTERN);
        String gen_s = getOptionalAttribute(root, "generation", properties, VARIABLE_PATTERN);
        int gen;
        if (gen_s == null)
            gen = 0;
        else
            gen = Integer.parseInt(gen_s);
        return new HamakePath(null, wdir, location, filename, mask, gen, null, Long.MAX_VALUE);
    }

}
