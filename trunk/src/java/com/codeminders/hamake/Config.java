package com.codeminders.hamake;

public class Config {

    public String version = "1.0";
    public boolean verbose   = false;
    public boolean test_mode = false;
    public boolean dryrun    = false;
    public boolean nodeps    = false;

    private static Config instance;

    private Config()
    {
    }

    public static Config getInstance()
    {
        if (instance == null)
            instance = new Config();
        return instance;
    }
}
