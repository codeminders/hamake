package com.codeminders.hamake;

public class OS {

	public static boolean isWindows(){
		 
		String os = System.getProperty("os.name").toLowerCase();
	    return (os.indexOf( "win" ) >= 0); 
 
	}
 
	public static boolean isMac(){
 
		String os = System.getProperty("os.name").toLowerCase();
	    return (os.indexOf( "mac" ) >= 0); 
 
	}
 
	public static boolean isUnix(){
		String os = System.getProperty("os.name").toLowerCase();
	    return (os.indexOf( "nix") >=0 || os.indexOf( "nux") >=0);
	}
	
	public static boolean isLinux(){
		String os = System.getProperty("os.name").toLowerCase();
	    return (os.indexOf( "linux") >=0 || os.indexOf( "linux") >=0);
	}

	
}
