package com.codeminders.hamake;

import java.io.IOException;
import java.security.Permission;
import java.util.Collection;
import java.util.HashMap;

import junit.framework.Assert;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.codeminders.hamake.commands.HadoopCommand;

public class TestHadoopCommand {

	private static final String DummyJobThatCallsSystemExit_MAIN_CLASS = "com.codeminders.hamake.examples.DummyJobThatCallsSystemExit";
	
	private static boolean testSystemExitIsProhibitedFailed = false;
	
	private static class NoExitSecurityManager extends SecurityManager {
		@Override
		public void checkPermission(Permission perm) {
			// allow anything.
		}

		@Override
		public void checkPermission(Permission perm, Object context) {
			// allow anything.
		}

		@Override
		public void checkExit(int status) {
			super.checkExit(status);
			testSystemExitIsProhibitedFailed = true;
			throw new SecurityException("System.exit() has been called");
		}
	}

	private SecurityManager securityManager;

	@Before
	public void setUp() {
		securityManager = System.getSecurityManager();
		System.setSecurityManager(new NoExitSecurityManager());
	}

	@After
	public void tearDown() {
		System.setSecurityManager(securityManager);
	}

	@Test
	public void testSystemExitIsProhibited() throws IOException {
		HadoopCommand command1 = new HadoopCommand();
		command1.setJar(TestHelperUtils.getExamplesJar().getAbsolutePath());
		command1.setMain(DummyJobThatCallsSystemExit_MAIN_CLASS);
		command1.execute(new HashMap<String, Collection>(),
				new HashMap<String, Object>());			
		Assert.assertFalse("System.exit has been passed by HadoopCommand", testSystemExitIsProhibitedFailed);
	}	

}
