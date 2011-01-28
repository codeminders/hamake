package com.codeminders.hamake;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import net.sf.ehcache.CacheException;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;
import net.sf.ehcache.event.CacheEventListener;

public class EhCacheListener implements CacheEventListener{
	
	public static final Log LOG = LogFactory.getLog(EhCacheListener.class);

	@Override
	public void dispose() {}

	@Override
	public void notifyElementEvicted(Ehcache cache, Element element) {
	}

	@Override
	public void notifyElementExpired(Ehcache cache, Element element) {}

	@Override
	public void notifyElementPut(Ehcache cache, Element element)
			throws CacheException {
	}

	@Override
	public void notifyElementRemoved(Ehcache cache, Element element)
			throws CacheException {
		Object o = element.getValue();
		if(o instanceof File){
			File f = (File)o;
			try {
				removeFile(f);
			} catch (IOException e) {
				throw new CacheException("IOException on element remove from ehcache", e);
			}
		}
	}

	@Override
	public void notifyElementUpdated(Ehcache cache, Element element)
			throws CacheException {}

	@Override
	public void notifyRemoveAll(Ehcache cache) {}
	
	public static void removeAll(Ehcache cache) throws IOException{
		for(Object key : cache.getKeys()){
			cache.remove(key);
		}
	}
	
	public Object clone() throws CloneNotSupportedException{
		throw new CloneNotSupportedException();
	}
	
	private static void removeFile(File f) throws IOException{
		if(f.isDirectory()){
			FileUtils.deleteDirectory(f);
		}
		else if(f.isFile()){
			if (!f.delete()){
				throw new IOException("Could not remove file " + f.getAbsolutePath());
			}
		}
	}

}
