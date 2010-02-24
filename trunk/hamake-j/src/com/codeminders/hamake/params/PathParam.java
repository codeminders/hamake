package com.codeminders.hamake.params;

import com.codeminders.hamake.Param;
import com.codeminders.hamake.Path;
import com.codeminders.hamake.Utils;
import org.apache.commons.lang.builder.ToStringBuilder;

import java.util.*;

public class PathParam implements Param {

    public enum Mask {

        keep,
        suppress,
        expand
    }

    private String name;
    private String ptype;
    private int number;
    private Mask maskHandling;

    public PathParam(String name, String ptype, int number) {
        this(name, ptype, number, Mask.keep);
    }

    public PathParam(String name, String ptype, int number, Mask maskHandling) {
        setName(name);
        setPtype(ptype);
        setNumber(number);
        setMaskHandling(maskHandling);
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPtype() {
        return ptype;
    }

    public void setPtype(String ptype) {
        this.ptype = ptype;
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

    public Collection<String> get(Map<String, List> dict, Object fsClient) {

        Collection<String> ret;

        if (getNumber() == -1) {
            ret = new ArrayList<String>();
            // mitliple inputs, may all be expanded. flatten results
            Collection params = dict.get(getPtype());
            if (params != null) {
                for (Object o : params) {
                    ret.addAll(toStrArr(o, fsClient));
                }
            }
        } else {
            ret = toStrArr(dict.get(ptype).get(getNumber()), fsClient);
        }
        return ret;
    }

    protected Collection<String> toStrArr(Object i) {
        return toStrArr(i, null);
    }

    protected Collection<String> toStrArr(Object i, Object fsClient) {
        if (i instanceof Path) {
            Path p = (Path) i;
            Mask m = getMaskHandling();
            if (m == Mask.keep) {
                return Collections.singleton(p.getPathNameWithMask());
            } else if (m == Mask.suppress) {
                return Collections.singleton(p.getPathName());
            } else {
                if (fsClient == null)
                    throw new IllegalArgumentException("Could not expand path, no fsclient");
                if (p.hasFilename())
                    throw new IllegalArgumentException("Could not expand file " + p);
                Collection<String> ret = new ArrayList<String>();

                for (String key : Utils.getFileList(fsClient, p.getPathName(), false, p.getMask()).keySet())
                    ret.add(p.getPathName(key));
                return ret;
            }
        } else
            return Collections.singleton(i.toString());
    }

    @Override
     public String toString() {
         return new ToStringBuilder(this).
                 append("name", name).
                 append("ptype", ptype).
                 append("number", number).
                 append("mask", maskHandling).toString();
     }

}