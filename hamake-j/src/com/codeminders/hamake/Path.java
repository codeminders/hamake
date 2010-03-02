package com.codeminders.hamake;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

public class Path {

    private String loc;
    private String filename;
    private String mask;
    private int gen;

    public Path(String loc) {
        this(loc, null, null, 0);
    }

    public Path(String loc, String filename) {
        this(loc, filename, null, 0);
    }

    public Path(String loc, String filename, String mask) {
        this(loc, filename, mask, 0);
    }

    public Path(String loc, String filename, String mask, int gen) {
        setLoc(loc);
        if (filename != null && mask != null)
            throw new IllegalArgumentException("Both filename and mask specified!");
        setFilename(filename);
        setMask(mask);
        setGen(gen);
    }

    public boolean intersects(Path other) {
        return StringUtils.equals(this.getLoc(), other.getLoc()) &&
                getGen() >= other.getGen() &&
                (getFilename() == null ||
                        other.getFilename() == null ||
                        StringUtils.equals(this.getFilename(), other.getFilename()));
    }

    public Path getPathWithNewName(String newFilename) {
        String mask;
        if (newFilename != null)
            mask = null;
        else
            mask = getMask();
        return new Path(getLoc(), newFilename, mask, getGen());
    }

    public org.apache.hadoop.fs.Path getPathName(FileSystem fs) {
        return getPathName(fs, null);
    }

    public org.apache.hadoop.fs.Path getPathName(FileSystem fs, String newFilename) {
        if (newFilename == null)
            newFilename = getFilename();
        if (newFilename != null)
            return fs.makeQualified(new org.apache.hadoop.fs.Path(getLoc() + '/' + newFilename));
        else
            return fs.makeQualified(new org.apache.hadoop.fs.Path(getLoc()));
    }

    public String getPathNameWithMask(FileSystem fs) {
        String p = Utils.getPath(getPathName(fs));
        String mask = getMask();
        if (mask != null)
            return p + org.apache.hadoop.fs.Path.SEPARATOR_CHAR + mask;
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
        org.apache.hadoop.fs.Path p = getPathName(fs);

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

    public String getLoc() {
        return loc;
    }

    public void setLoc(String loc) {
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
