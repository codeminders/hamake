package com.codeminders.hamake;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class HamakePath {
	
	public enum Variant {
		LIST, PATH, MASK;
		
		public static Variant parseString(String variant){
			if(StringUtils.isEmpty(variant)) return null;
			if(variant.equalsIgnoreCase("list")) return LIST;
			else if(variant.equalsIgnoreCase("path")) return PATH;
			else if(variant.equalsIgnoreCase("mask")) return MASK;
			else return null;
		}
	}
	
	public static final Log LOG = LogFactory.getLog(HamakePath.class);

	private String ID;
    private String wdir;
    private String loc;
    private String filename;
    private String mask;
    private int gen;
    private FileSystem fs;
    private Variant variant;
    private long validityPeriod;
    
	public HamakePath(String loc) throws IOException {
    	this(null, null, loc, null, null, 0, null, Long.MAX_VALUE);
    }
	
	public HamakePath(String loc, long validityPeriod) throws IOException {
    	this(null, null, loc, null, null, 0, null, validityPeriod);
    }
	
	public HamakePath(String loc, long validityPeriod, int gen) throws IOException {
    	this(null, null, loc, null, null, gen, null, validityPeriod);
    }

    public HamakePath(String wdir, String loc) throws IOException {
    	this(null, wdir, loc, null, null, 0, null, Long.MAX_VALUE);
    }

    public HamakePath(String loc, int gen) throws IOException {
    	this(null, null, loc, null, null, gen, null, Long.MAX_VALUE);
    }

    public HamakePath(String ID, String wdir, String loc, String mask, int gen) throws IOException {
    	this(ID, wdir, loc, null, mask, gen, null, Long.MAX_VALUE);
    }
    
    public HamakePath(String ID, String wdir, String loc, String mask, int gen, Variant variant) throws IOException {
    	this(ID, wdir, loc, null, mask, gen, variant, Long.MAX_VALUE);
    }
    
    public HamakePath(String ID, String wdir, String loc, String filename, String mask, int gen, Variant variant, long validityPeriod) throws IOException {
        this.wdir = wdir;
    	Configuration conf = new Configuration();

        Path pathLoc = resolve(wdir, loc);

        fs = pathLoc.getFileSystem(conf);

        setLoc(pathLoc.toString());
        
        if (filename != null && mask != null)
            throw new IllegalArgumentException("Both filename and mask specified!");
        setFilename(filename);
        setMask(mask);
        setGen(gen);
        this.variant = variant;
        this.validityPeriod = validityPeriod;
        this.ID = ID;
    }
    
    public String getID() {
		return ID;
	}
    
    public long getValidityPeriod() {
		return validityPeriod;
	}

	public static Path resolve(String wdir, String loc)
    {
        Path pathLoc = new Path(loc);

        if (!pathLoc.isAbsolute() && !StringUtils.isEmpty(wdir))
            pathLoc = new Path(wdir, loc);

        return pathLoc;
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
    
    public Variant getVariant() {
		return variant;
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
                	LOG.warn("Removing " + p.toUri());
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
                    	LOG.warn("Removing " + stat.getPath());
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
