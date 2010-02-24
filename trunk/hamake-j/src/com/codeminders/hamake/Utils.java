package com.codeminders.hamake;

import org.apache.commons.lang.StringUtils;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.io.IOException;

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

    public static Map<String, String> getFileList(Object fsclient, String ipath) {
        return getFileList(fsclient, ipath, false, null);
    }

    public static Map<String, String> getFileList(Object fsclient, String ipath, String mask) {
        return getFileList(fsclient, ipath, false, mask);
    }

    public static Map<String, String> getFileList(Object fsclient, String ipath, boolean create) {
        return getFileList(fsclient, ipath, create, null);
    }

    public static Map<String, String> getFileList(Object fsclient, String ipath, boolean create, String mask) {
        // TODO
        return new HashMap<String, String>();
    }
    // TODO
    /*
    def _getFileList(fsclient, ipath, create = False, mask=None):
        """ Utility method to get list of files from DFS
        """
        if hconfig.test_mode:
            print >> sys.stderr, "Scanning %s" % ipath.pathname

        with fsclient.mutex:
            iexists = fsclient.exists(ipath)
        if not iexists:
            if create:
                print >> sys.stderr, "Creating %s" % ipath.pathname
                with fsclient.mutex:
                    fsclient.mkdirs(ipath)
                return {}
            else:
                print >> sys.stderr, "path %s does not exists!" % ipath.pathname
                return None

        with fsclient.mutex:
            istat = fsclient.stat(ipath)
        if not istat.isdir:
            print >> sys.stderr, "path %s must be dir!" % ipath.pathname
            return None
        with fsclient.mutex:
            inputlist = fsclient.listStatus(ipath)
        res = {}
        for i in inputlist:
            pos = i.path.rfind('/')
            fname = i.path[pos+1:]
            if mask==None or fnmatch(fname, mask):
                res[fname] = i
        return res

    */

    // TODO: signature, what is the type of fslient..
    public static Object getFSClient(Map<String, Object> context) {
        return context.get("fsclient");
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
        }  catch (Exception ex) {
            System.err.println(command + " execution failed, internal error");
            if (Config.getInstance().test_mode) {
                ex.printStackTrace();
            }
        }
        return -1000;
    }

}
