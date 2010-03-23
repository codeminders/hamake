package com.codeminders.hamake;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class HamakePath {

    private String wdir;
    private String loc;
    private String filename;
    private String mask;
    private int gen;
    private FileSystem fs;

    public HamakePath(String wdir, String loc, String filename, String mask, int gen) throws IOException {
        this.wdir = wdir;
    	Configuration conf = new Configuration();
        Path pathLoc = new Path(loc);

        if (!pathLoc.isAbsolute())
            pathLoc = new Path(wdir, loc);

    	fs = pathLoc.getFileSystem(conf);
        setLoc(pathLoc.toString());
        
        if (filename != null && mask != null)
            throw new IllegalArgumentException("Both filename and mask specified!");
        setFilename(filename);
        setMask(mask);
        setGen(gen);
    }

    public boolean intersects(HamakePath other) {
        return StringUtils.equals(this.getLoc(), other.getLoc()) &&
                getGen() >= other.getGen() &&
                (getFilename() == null ||
                        other.getFilename() == null ||
                        StringUtils.equals(this.getFilename(), other.getFilename()));
    }

    public HamakePath getPathWithNewName(String newFilename) throws IOException {
        String mask;
        if (newFilename != null)
            mask = null;
        else
            mask = getMask();
        return new HamakePath(wdir, getLoc(), newFilename, mask, getGen());
    }

    public Path getPathName() {
        return getPathName(null);
    }

    public Path getPathName(String newFilename) {
        if (newFilename == null)
            newFilename = getFilename();
        if (newFilename != null)
            return new Path(getLoc(), newFilename);
        else
            return new Path(getLoc());
    }

    public String getPathNameWithMask() {
        String p = getPathName().toString();
        String mask = getMask();
        if (mask != null)
            return p + Path.SEPARATOR_CHAR + mask;
        else
            return p;
    }

    public boolean hasFilename() {
        return getFilename() != null;
    }

    @Override
    public String toString() {
        StringBuilder ret = new StringBuilder(getLoc());
        ret.append('/');
        if (hasFilename())
            ret.append(getFilename());
        String mask = getMask();
        if (mask != null)
            ret.append(mask);
        int gen = getGen();
        if (gen > 0)
            ret.append('@').append(gen);
        return ret.toString();
    }

    public void removeIfExists(FileSystem fs) throws IOException {
        Path p = getPathName();

        if (hasFilename() || getMask() == null) {
            boolean exists;
            synchronized (fs) {
                exists = fs.exists(p);
            }
            if (exists) {
                if (Config.getInstance().verbose) {
                    System.err.println("Removing " + p.toUri());
                }
                if (!Config.getInstance().dryrun) {
                    synchronized (fs) {
                        fs.delete(p, true);
                    }
                }
            }
        } else {
            FileStatus status;
            synchronized (fs) {
                status = fs.getFileStatus(p);
            }

            if (status == null) {
                // lyolik: Does not exist
                return;
            }

            if (!status.isDir()) {
                throw new IOException("Path " + p.toUri() + " must be dir!");
            }
            FileStatus list[];
            synchronized (fs) {
                list = fs.listStatus(p);
            }

            for (FileStatus stat : list) {
                if (Utils.matches(stat.getPath(), getMask())) {
                    if (Config.getInstance().verbose) {
                        System.err.println("Removing " + stat.getPath());
                    }
                    if (!Config.getInstance().dryrun) {
                        synchronized (fs) {
                            fs.delete(stat.getPath(), true);
                        }
                    }
                }
            }
        }
    }
    
    public FileSystem getFileSystem() throws IOException {
        return fs;
    }

    public String getLoc() {    	
        return loc;
    }

    public void setLoc(String loc){
        this.loc = loc;
    }

    public String getFilename() {
        return filename;
    }    

    public void setFilename(String filename) {
        this.filename = filename;
    }

    public String getMask() {
        return mask;
    }

    public void setMask(String mask) {
        this.mask = mask;
    }

    public int getGen() {
        return gen;
    }

    public void setGen(int gen) {
        this.gen = gen;
    }
}
