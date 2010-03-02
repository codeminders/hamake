package com.codeminders.hamake.params;

import com.codeminders.hamake.Param;
import com.codeminders.hamake.Path;
import com.codeminders.hamake.Utils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.*;

public class PathParam implements Param {

    public enum Mask {

        keep,
        suppress,
        expand
    }

    public enum Type {
        input,
        output,
        dependency,
        inputfile,
        outputfile
    }

    private String name;
    private String type;
    private int number;
    private Mask maskHandling;

    public PathParam(String name, String type, int number) {
        this(name, type, number, Mask.keep);
    }

    public PathParam(String name, String type, int number, Mask maskHandling) {
        setName(name);
        setType(type);
        setNumber(number);
        setMaskHandling(maskHandling);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getNumber() {
        return number;
    }

    public void setNumber(int number) {
        this.number = number;
    }

    public Mask getMaskHandling() {
        return maskHandling;
    }

    public void setMaskHandling(Mask maskHandling) {
        this.maskHandling = maskHandling;
    }

    public Collection<String> get(Map<String, Collection> dict, FileSystem fs) throws IOException {

        Collection<String> ret;

        int number = getNumber();
        if (number == -1) {
            ret = new ArrayList<String>();
            // mitliple inputs, may all be expanded. flatten results
            Collection params = dict.get(getType());
            if (params != null) {
                for (Object o : params) {
                    ret.addAll(toStrArr(o, fs));
                }
            }
        } else {
            int counter = 0;
            Collection pcol = dict.get(getType());
            if (pcol == null)
                throw new IllegalArgumentException("Not found " + getType() + " parameters");
            Iterator it = pcol.iterator();
            Object params = null;
            while (counter <= number) {
                if (it.hasNext()) {
                    params = it.next();
                    counter++;
                } else {
                    throw new IllegalArgumentException("Not found item " + number + " in " + getType() + " parameters");
                }
            }
            ret = toStrArr(params, fs);
        }
        return ret;
    }

    protected Collection<String> toStrArr(Object i) throws IOException {
        return toStrArr(i, null);
    }

    protected Collection<String> toStrArr(Object i, FileSystem fs) throws IOException {
        if (i instanceof Path) {
            Path p = (Path) i;
            Mask m = getMaskHandling();
            if (m == Mask.keep) {
                return Collections.singleton(p.getPathNameWithMask(fs));
            } else if (m == Mask.suppress) {
                if (fs == null)
                    throw new IllegalArgumentException("Could not expand path, no filesystem");
                return Collections.singleton(Utils.getPath(p.getPathName(fs)));
            } else {
                if (fs == null)
                    throw new IllegalArgumentException("Could not expand path, no filesystem");
                if (p.hasFilename())
                    throw new IllegalArgumentException("Could not expand file " + p);
                Collection<String> ret = new ArrayList<String>();

                Map<String, FileStatus> list =
                        Utils.getFileList(fs, p.getPathName(fs), false, p.getMask());
                if (list != null) {
                    for (String key : list.keySet())
                        ret.add(Utils.getPath(p.getPathName(fs, key)));
                }
                return ret;
            }
        } else
            return Collections.singleton(i.toString());
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).
                append("name", name).
                append("ptype", type).
                append("number", number).
                append("mask", maskHandling).toString();
    }

}