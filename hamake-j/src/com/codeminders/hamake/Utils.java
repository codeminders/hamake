package com.codeminders.hamake;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.DFSClient;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Utils {

    private static final Pattern VARIABLE_PLACEHOLDER = Pattern.compile("%\\(([^\\)]+)\\)[sdiefc]");

    public static String getRequiredAttribute(Element root, String name) throws InvalidMakefileException {
        return getRequiredAttribute(root, name, null);
    }

    public static String getRequiredAttribute(Element root, String name, Map<String, String> properties) throws InvalidMakefileException {
        if (root.hasAttribute(name)) {
            String ret = root.getAttribute(name);
            if (properties != null) {
                ret = substitute(ret, properties);
            }
            return ret;
        }
        throw new InvalidMakefileException("Missing '" + name + "' attribute in '" + getPath(root) + "' element");
    }

    public static String getOptionalAttribute(Element root, String name) {
        return getOptionalAttribute(root, name, null);
    }

    public static String getOptionalAttribute(Element root, String name, Map<String, String> properties) {
        if (root.hasAttribute(name)) {
            String ret = root.getAttribute(name);
            if (properties != null) {
                ret = substitute(ret, properties);
            }
            return ret;
        }
        return null;
    }

    public static String getPath(Node n) {
        StringBuilder ret = new StringBuilder();
        while (n != null && n.getNodeType() != Node.DOCUMENT_NODE) {
            ret.insert(0, n.getNodeName()).insert(0, '/');
            n = n.getParentNode();
        }
        return ret.toString();
    }

    public static Element getMandatory(Element root, String name)
            throws InvalidMakefileException {
        NodeList c = root.getElementsByTagName(name);
        if (c.getLength() != 1)
            throw new InvalidMakefileException("Missing or ambiguous '" + name + "' section");
        return (Element) c.item(0);
    }

    public static Element getOptional(Element root, String name)
            throws InvalidMakefileException {
        NodeList c = root.getElementsByTagName(name);
        if (c.getLength() != 1)
            throw new InvalidMakefileException("Missing or ambiguous '" + name + "' section");
        return (Element) c.item(0);
    }


    public static String substitute(String s, Map<String, String> properties) {
        int pos = 0;
        Matcher m = VARIABLE_PLACEHOLDER.matcher(s);
        while (m.find(pos)) {
            int start = m.start();
            int end = m.end();
            StringBuilder buf = new StringBuilder();
            if (start > 0)
                buf.append(s, 0, start);
            String subst = properties.get(m.group(1));
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
            m = VARIABLE_PLACEHOLDER.matcher(s);
        }
        return s;
    }

    public static Map<String, org.apache.hadoop.fs.Path> getFileList(DFSClient fsclient, String ipath) throws IOException {
        return getFileList(fsclient, ipath, false, null);
    }

    public static Map<String, org.apache.hadoop.fs.Path> getFileList(DFSClient fsclient, String ipath, String mask) throws IOException {
        return getFileList(fsclient, ipath, false, mask);
    }

    public static Map<String, org.apache.hadoop.fs.Path> getFileList(DFSClient fsclient, String ipath, boolean create) throws IOException {
        return getFileList(fsclient, ipath, create, null);
    }

    public static Map<String, org.apache.hadoop.fs.Path> getFileList(DFSClient fsclient, String ipath, boolean create, String mask)
        throws IOException {
        if (Config.getInstance().test_mode)
            System.err.println("Scanning " + ipath);

        boolean exists;

        synchronized (fsclient) {
                exists = fsclient.exists(ipath);
        }
        if (!exists) {
            if (create) {
                System.err.println("Creating " + ipath);
                synchronized (fsclient) {
                        fsclient.mkdirs(ipath);
                }
                return Collections.emptyMap();
            } else {
                System.err.println("Path " + ipath + " does not exist!");
                return null;
            }
        }

        FileStatus fs;
        synchronized (fsclient) {
            fs = fsclient.getFileInfo(ipath);
        }
        if (!fs.isDir()) {
            System.err.println("Path " + ipath + " must be dir!");
            return null;
        }

        FileStatus list[];
        synchronized (fsclient) {
            list = fsclient.listPaths(ipath);
        }

        Map<String, org.apache.hadoop.fs.Path> ret = new HashMap<String, org.apache.hadoop.fs.Path>();
        for (FileStatus stat : list) {
            String name = stat.getPath().getName();
            if (mask == null || FilenameUtils.wildcardMatch(name, mask)) {
                ret.put(name, stat.getPath());
            }
        }
        return ret;
    }

    public static DFSClient getFSClient(Map<String, Object> context) {
        return (DFSClient) context.get("fsclient");
    }

    public static String getenv(String name, String defaultValue) {
        String ret = System.getenv(name);
        return !StringUtils.isEmpty(ret) ? ret : defaultValue;
    }

    public static int execute(String command) {
        if (Config.getInstance().verbose)
            System.err.println("Executing " + command);
        try {
            if (Config.getInstance().dryrun)
                return 0;
            else {
                return Runtime.getRuntime().exec(command).waitFor();
            }
        } catch (IOException ex) {
            System.err.println(command + " execution failed, I/O error");
            if (Config.getInstance().test_mode) {
                ex.printStackTrace();
            }
        } catch (InterruptedException ex) {
            System.err.println(command + " execution is interrupted");
            if (Config.getInstance().test_mode) {
                ex.printStackTrace();
            }
        } catch (Exception ex) {
            System.err.println(command + " execution failed, internal error");
            if (Config.getInstance().test_mode) {
                ex.printStackTrace();
            }
        }
        return -1000;
    }

}
