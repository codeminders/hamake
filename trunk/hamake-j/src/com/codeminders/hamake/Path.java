package com.codeminders.hamake;

import org.apache.commons.lang.StringUtils;

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

    public String getPathName() {
        return getPathName(null);
    }

    public String getPathName(String newFilename) {
        if (newFilename == null)
            newFilename = getFilename();
        if (newFilename != null)
            return getLoc() + '/' + newFilename;
        else
            return getLoc();
    }

    public String getPathNameWithMask() {
        return getPathNameWithMask(null);
    }

    public String getPathNameWithMask(String newFilename) {
        String p = getPathName(newFilename);
        String mask = getMask();
        if (mask != null)
            return p + '/' + mask;
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


    // TODO
    /*
class Path:

    def removeIfExists(self, fsclient):
        ipath = self.getHPathName()
        if self.filename!=None or self.mask==None:
            with fsclient.mutex:
                iexists = fsclient.exists(ipath)
            if iexists:
                if hconfig.verbose:
                    print >> sys.stderr, "Removing %s" % ipath.pathname
                if not hconfig.dryrun:
                    with fsclient.mutex:
                        fsclient.rm(ipath,True)
        else:
            with fsclient.mutex:
                istat = fsclient.stat(ipath)
            if not istat.isdir:
                raise Exception("path %s must be dir!" % ipath.pathname)
            with fsclient.mutex:
                inputlist = fsclient.listStatus(ipath)
            for i in inputlist:
                pos = i.path.rfind('/')
                fname = i.path[pos+1:]
                if fnmatch(fname, self.mask):
                    fpath = self.getHPathName(fname)
                    with fsclient.mutex:
                        fexists = fsclient.exists(fpath)
                    if fexists:
                        if hconfig.verbose:
                            print >> sys.stderr, "Removing %s" % fpath.pathname
                        if not hconfig.dryrun:
                            with fsclient.mutex:
                                fsclient.rm(fpath,True)

    @classmethod
    def fromDOM(cls, dom, props):
        loc = getRequiredAttribute(dom, 'location', props)
        filename = getOptionalAttribute(dom, 'filename', props)
        mask = getOptionalAttribute(dom, 'mask', props)
        gen_s = getOptionalAttribute(dom, 'generation', props)
        if gen_s == None:
            gen = 0
        else:
            gen = int(gen_s)
        return Path(loc, filename, mask, gen)

     */

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
