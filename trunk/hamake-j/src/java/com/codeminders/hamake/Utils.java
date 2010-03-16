package com.codeminders.hamake;

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.*;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.*;
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

    public static String getRequiredAttribute(Element root, String name, Map<String, String> properties)
            throws InvalidMakefileException {
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

    public static Map<String, FileStatus> getFileList(FileSystem fs, org.apache.hadoop.fs.Path ipath)
            throws IOException {
        return getFileList(fs, ipath, false, null);
    }

    public static Map<String, FileStatus> getFileList(FileSystem fs, org.apache.hadoop.fs.Path ipath, String mask)
            throws IOException {
        return getFileList(fs, ipath, false, mask);
    }

    public static Map<String, FileStatus> getFileList(FileSystem fs, org.apache.hadoop.fs.Path ipath, boolean create)
            throws IOException {
        return getFileList(fs, ipath, create, null);
    }

    public static Map<String, FileStatus> getFileList(FileSystem fs,
                                                      org.apache.hadoop.fs.Path ipath,
                                                      boolean create,
                                                      String mask)
            throws IOException {
        if (Config.getInstance().test_mode)
            System.err.println("Scanning " + ipath);

        boolean exists;

        synchronized (fs) {
            exists = fs.exists(ipath);
        }
        if (!exists) {
            if (create) {
                System.err.println("Creating " + ipath);
                synchronized (fs) {
                    fs.mkdirs(ipath);
                }
                return Collections.emptyMap();
            } else {
                System.err.println("Path " + ipath + " does not exist!");
                return null;
            }
        }

        FileStatus stat;
        synchronized (fs) {
            stat = fs.getFileStatus(ipath);
        }
        if (!stat.isDir()) {
            System.err.println("Path " + ipath + " must be dir!");
            return null;
        }

        FileStatus list[];
        synchronized (fs) {
            list = fs.listStatus(ipath);
        }

        Map<String, FileStatus> ret = new HashMap<String, FileStatus>();
        for (FileStatus s : list) {
            org.apache.hadoop.fs.Path p = s.getPath();
            if (matches(p, mask))
                ret.put(p.getName(), s);
        }
        return ret;
    }

    public static FileSystem getFileSystem(Map<String, Object> context) {
        return (FileSystem) context.get("filesystem");
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

    public static File copyToTemporaryLocal(String path, FileSystem fs)
            throws IOException {
    	File srcFile = new File(path);
    	org.apache.hadoop.fs.Path srcPath = new org.apache.hadoop.fs.Path(path);
    	File dstFile = File.createTempFile("hamake", ".tmp");
    	dstFile.deleteOnExit();
        if (Config.getInstance().verbose) {
            System.err.println("Downloading " + path + " to " + dstFile.getAbsolutePath());
        }
        System.out.println();
    	if(srcFile.exists()){
    		InputStream src = null;
    		OutputStream dst = null;
    		try{
    			src = new FileInputStream(srcFile);
    			dst = new FileOutputStream(dstFile);
    			IOUtils.copy(src, dst);
    		} finally{
    			if(src != null)IOUtils.closeQuietly(src);
    			if(dst != null)IOUtils.closeQuietly(dst);
    		}
    	}
    	else if(fs.exists(srcPath)){       		
            fs.copyToLocalFile(srcPath, new org.apache.hadoop.fs.Path(dstFile.getAbsolutePath()));
    	}
        else
            throw new IOException("Path not found: " + path);
        
        return dstFile;
    }

    public static boolean matches(org.apache.hadoop.fs.Path p, String mask) {
        String name = p.getName();
        return mask == null || FilenameUtils.wildcardMatch(name, mask);
    }

    public static String getPath(org.apache.hadoop.fs.Path p) {
        StringBuilder buf = new StringBuilder(p.getName());
        p = p.getParent();
        while (p != null) {
            buf.insert(0, org.apache.hadoop.fs.Path.SEPARATOR_CHAR);
            buf.insert(0, p.getName());
            p = p.getParent();
        }
        return buf.toString();
    }
}
