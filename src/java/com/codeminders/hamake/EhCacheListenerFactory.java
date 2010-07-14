package com.codeminders.hamake;

import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import net.sf.ehcache.event.CacheEventListener;
import net.sf.ehcache.event.CacheEventListenerFactory;

public class EhCacheListenerFactory extends CacheEventListenerFactory {

	public static final Log LOG = LogFactory.getLog(CacheEventListenerFactory.class);
	
	@Override
	public CacheEventListener createCacheEventListener(Properties properties) {
		return new EhCacheListener();
	}

}
