package com.codeminders.hamake;

import java.security.Permission;

public class ExitSecurityManager extends SecurityManager {

	boolean exitCalled = false;

	@Override
	public void checkPermission(Permission perm) {
	}

	@Override
	public void checkPermission(Permission perm, Object context) {
	}

	@Override
	public void checkExit(int status) {
		exitCalled = true;
		throw new SecurityException();
	}
	
}
