package com.codeminders.hamake;

import com.codeminders.hamake.commands.ExecCommand;
import com.codeminders.hamake.commands.HadoopCommand;
import com.codeminders.hamake.commands.PigCommand;
import com.codeminders.hamake.params.ConstParam;
import com.codeminders.hamake.params.JobConfParam;
import com.codeminders.hamake.params.PathParam;
import com.codeminders.hamake.params.PigParam;
import com.codeminders.hamake.tasks.BaseTask;
import com.codeminders.hamake.tasks.MapTask;
import com.codeminders.hamake.tasks.ReduceTask;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

public class MakefileParser {

    public Hamake parse(String filename, boolean verbose) throws IOException,
            ParserConfigurationException,
            SAXException,
            InvalidMakefileException {
        InputStream is = new FileInputStream(filename);
        try {
            return parse(is, verbose);
        } finally {
            try {
                is.close();
            } catch (Exception ex) { /* Don't care */ }
        }
    }

    public Hamake parse(InputStream is, boolean verbose) throws IOException,
            ParserConfigurationException,
            SAXException,
            InvalidMakefileException {
        Document dom = loadMakefile(is);

        Element config = parseConfig(dom);
        Map<String, String> properties = parseProperties(config);

        Hamake ret = new Hamake();

        Element dfs = Utils.getMandatory(config, "dfs");
        Element api = Utils.getMandatory(dfs, "thriftAPI");

        ret.setThriftHost(Utils.getRequiredAttribute(api, "host", properties));
        ret.setThriftPort(Integer.parseInt(Utils.getRequiredAttribute(api, "port", properties)));

        parseTasks(ret, dom.getDocumentElement(), properties, verbose);
        return ret;
    }

    protected Document loadMakefile(InputStream is)
            throws IOException, ParserConfigurationException, SAXException {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        DocumentBuilder builder = factory.newDocumentBuilder();
        return builder.parse(is);
    }

    protected Element parseConfig(Document dom) throws InvalidMakefileException {
        return Utils.getMandatory(dom.getDocumentElement(), "config");
    }

    protected Map<String, String> parseProperties(Element root) throws InvalidMakefileException {
        NodeList c = root.getElementsByTagName("property");
        Map<String, String> ret = new HashMap<String, String>() {
            String get(String key) {
                if ("timestamp".equals(key))
                    return String.valueOf(System.currentTimeMillis() / 1000L);
                return super.get(key);
            }
        };
        for (int i = 0, sz = c.getLength(); i < sz; i++) {
            Element e = (Element) c.item(i);
            ret.put(Utils.getRequiredAttribute(e, "name", ret),
                    Utils.getRequiredAttribute(e, "value", ret));
        }
        return ret;
    }

    protected void parseTasks(Hamake hamake,
                              Element config,
                              Map<String, String> properties,
                              boolean verbose) throws InvalidMakefileException {
        NodeList tx = config.getElementsByTagName("map");
        for (int i = 0, sz = tx.getLength(); i < sz; i++) {
            Element t = (Element) tx.item(i);
            String dattr = Utils.getOptionalAttribute(t, "disabled", properties);
            if ("yes".equalsIgnoreCase(dattr) || "true".equalsIgnoreCase(dattr)) {
                if (verbose)
                    System.out.println("Ignoring disabled task " + Utils.getOptionalAttribute(t, "name", properties));
                continue;
            }
            hamake.addTask(parseMapTask(t, properties));
        }
        tx = config.getElementsByTagName("reduce");
        for (int i = 0, sz = tx.getLength(); i < sz; i++) {
            Element t = (Element) tx.item(i);
            String dattr = Utils.getOptionalAttribute(t, "disabled", properties);
            if ("yes".equalsIgnoreCase(dattr) || "true".equalsIgnoreCase(dattr)) {
                if (verbose)
                    System.out.println("Ignoring disabled task " + Utils.getOptionalAttribute(t, "name", properties));
                continue;
            }
            hamake.addTask(parseReduceTask(t, properties));
        }
    }

    protected Task parseMapTask(Element root, Map<String, String> properties) throws InvalidMakefileException {
        String name = Utils.getRequiredAttribute(root, "name", properties);

        Collection<Path> inputs = parsePathList(root, "input", properties);
        Path input;
        if (inputs.size() == 0)
            input = null;
        else if (inputs.size() == 1)
            input = inputs.iterator().next();
        else
            throw new InvalidMakefileException("Multiple 'input' elements in MAP task '%s' are not permitted" + name);

        Collection<Path> outputs = parsePathList(root, "output", properties);
        Collection<Path> deps = parsePathList(root, "dependencies", properties);

        Command command = parseCommand(root, properties);

        // Sanity checks
        if (input != null) {
            boolean itype = input.hasFilename();
            for (Path p : outputs)
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

    protected Command parseCommand(Element root, Map<String, String> properties) throws InvalidMakefileException {
        NodeList list = root.getElementsByTagName("task");
        int size = list.getLength();
        if (size > 1)
            throw new InvalidMakefileException("Multiple elements 'task' in " + Utils.getPath(root) + " are not permitted");
        if (size == 1)
            return parseHadoopCommand((Element) list.item(0), properties);

        list = root.getElementsByTagName("pig");
        size = list.getLength();
        if (size > 1)
            throw new InvalidMakefileException("Multiple elements 'pig' in " + Utils.getPath(root) + " are not permitted");
        if (size == 1)
            return parsePigCommand((Element) list.item(0), properties);

        list = root.getElementsByTagName("exec");
        size = list.getLength();
        if (size > 1)
            throw new InvalidMakefileException("Multiple elements 'exec' in " + Utils.getPath(root) + " are not permitted");
        if (size == 1)
            return parseExecCommand((Element) list.item(0), properties);

        throw new InvalidMakefileException("No commands are encountered in " + Utils.getPath(root));
    }

    protected Command parseHadoopCommand(Element root, Map<String, String> properties) throws InvalidMakefileException {
        HadoopCommand res = new HadoopCommand();
        res.setJar(Utils.getRequiredAttribute(root, "jar", properties));
        res.setMain(Utils.getRequiredAttribute(root, "main", properties));
        res.setParameters(parseParametersList(root, properties));
        return res;
    }

    protected Command parsePigCommand(Element root, Map<String, String> properties) throws InvalidMakefileException {
        PigCommand res = new PigCommand();
        res.setScript(Utils.getRequiredAttribute(root, "script", properties));
        res.setParameters(parseParametersList(root, properties));
        return res;
    }

    protected Command parseExecCommand(Element root, Map<String, String> properties) throws InvalidMakefileException {
        ExecCommand res = new ExecCommand();
        res.setBinary(Utils.getRequiredAttribute(root, "binary", properties));
        res.setParameters(parseParametersList(root, properties));
        return res;
    }

    protected Task parseReduceTask(Element root, Map<String, String> properties) throws InvalidMakefileException {
        String name = Utils.getRequiredAttribute(root, "name", properties);

        Collection<Path> inputs = parsePathList(root, "input", properties);
        Collection<Path> outputs = parsePathList(root, "output", properties);
        Collection<Path> deps = parsePathList(root, "dependencies", properties);

        Command command = parseCommand(root, properties);

        ReduceTask res = new ReduceTask();
        res.setName(name);
        res.setOutputs(outputs);
        res.setDeps(deps);
        res.setCommand(command);
        res.setInputs(inputs);
        parseTaskDeps(root, res, properties);
        return res;
    }

    protected Collection<Param> parseParametersList(Element root, Map<String, String> properties)
            throws InvalidMakefileException {
        Collection<Param> ret = new ArrayList<Param>();
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
                    throw new InvalidMakefileException("Unknown sub-element '" + name + "' under " + Utils.getPath(root));
                }
                ret.add(param);
            }
        }
        return ret;
    }

    protected Param parseConstParam(Element root, Map<String, String> properties) throws InvalidMakefileException {
        return new ConstParam(Utils.getRequiredAttribute(root, "value", properties));
    }

    protected Param parsePathParam(Element root, Map<String, String> properties, int index) throws InvalidMakefileException {
        String ptype = Utils.getRequiredAttribute(root, "type", properties);
        String number_s = Utils.getOptionalAttribute(root, "number", properties);
        int number;
        if (number_s != null)
            number = Integer.parseInt(number_s);
        else
            number = -1;
        String mask_handling = Utils.getOptionalAttribute(root, "mask", properties);
        PathParam.Mask mask;
        if (mask_handling == null)
            mask = PathParam.Mask.keep;
        else {
            try {
                mask = PathParam.Mask.valueOf(mask_handling);
            } catch (IllegalArgumentException ex) {
                throw new InvalidMakefileException("Unsupported value of 'mask' parameter in " + Utils.getPath(root));
            }
        }
        String name = Utils.getOptionalAttribute(root, "name", properties);
        if (name == null)
            name = "path" + index;
        return new PathParam(name, ptype, number, mask);
    }

    protected Param parseJobConfParam(Element root, Map<String, String> properties) throws InvalidMakefileException {
        return new JobConfParam(Utils.getRequiredAttribute(root, "name", properties),
                Utils.getRequiredAttribute(root, "value", properties));
    }

    protected Param parsePigParam(Element root, Map<String, String> properties) throws InvalidMakefileException {
        return new PigParam(Utils.getRequiredAttribute(root, "name", properties),
                Utils.getRequiredAttribute(root, "value", properties));
    }

    protected void parseTaskDeps(Element root, BaseTask res, Map<String, String> properties)
            throws InvalidMakefileException {
        NodeList list = root.getElementsByTagName("taskdep");
        int size = list.getLength();
        if (size == 0)
            return;
        if (size > 1)
            throw new InvalidMakefileException("Multiple elements 'taskdep' in " + res.getName() + " are not permitted");

        NodeList pretasks = ((Element) list.item(0)).getElementsByTagName("taskdep");

        Collection<String> deps = new ArrayList<String>();
        for (int i = 0, sz = pretasks.getLength(); i < sz; i++) {
            String name = Utils.getRequiredAttribute((Element) pretasks.item(i), "name", properties);
            if (!name.equals(res.getName()))
                deps.add(name);
        }
        res.setTaskDeps(deps);
    }

    protected Collection<Path> parsePathList(Element root, String name, Map<String, String> properties)
            throws InvalidMakefileException {
        NodeList list = root.getElementsByTagName(name);
        int len = list.getLength();
        if (len == 0)
            return Collections.emptySet();
        if (len != 1)
            throw new InvalidMakefileException("Multiple elements '" + name + "' in " + Utils.getPath(root) +
                    " are not permitted");
        NodeList path = ((Element) list.item(0)).getElementsByTagName("path");
        Collection<Path> ret = new ArrayList<Path>();
        for (int i = 0, sz = path.getLength(); i < sz; i++) {
            ret.add(parsePath((Element) path.item(i), properties));
        }
        return ret;
    }

    protected Path parsePath(Element root, Map<String, String> properties) throws InvalidMakefileException {

        String location = Utils.getRequiredAttribute(root, "location", properties);
        String filename = Utils.getOptionalAttribute(root, "filename", properties);
        String mask = Utils.getOptionalAttribute(root, "mask", properties);
        String gen_s = Utils.getOptionalAttribute(root, "generation", properties);
        int gen;
        if (gen_s == null)
            gen = 0;
        else
            gen = Integer.parseInt(gen_s);
        return new Path(location, filename, mask, gen);
    }

}
