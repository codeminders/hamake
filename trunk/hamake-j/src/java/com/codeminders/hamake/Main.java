package com.codeminders.hamake;

import org.apache.commons.cli.*;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.InputStream;

public class Main {

    enum ExitCodes {
        OK,
        BADOPT,
        INITERR,
        FAILED
    }

    public static final String DEFAULT_MAKEFILE_NAME = "hamakefile.xml";

    public static void main(String args[]) {

        Options options = new Options();
        options.addOption("V", "version", false, "display version and exit");
        options.addOption("v", "verbose", false, "verbose mode");
        options.addOption("n", "dry-run", false, "dry run mode");
        options.addOption("d", "nodeps", false, "no deps");
        options.addOption("t", "test", false, "TODO test");
        options.addOption("j", "jobs", true, "TODO jobs");
        options.addOption("f", "file", true, "TODO file");

        CommandLineParser parser = new PosixParser();

        CommandLine line = null;

        try {
            // parse the command line arguments
            line = parser.parse(options, args, false);
        }
        catch (ParseException ex) {
            new HelpFormatter().printHelp("hamake", options);
            System.exit(ExitCodes.INITERR.ordinal());
        }

        Config config = Config.getInstance();

        if (line.hasOption('V')) {
            System.out.println("HAMake/J version " + config.version);
            System.exit(0);
        }

        if (line.hasOption('v'))
            config.verbose = true;
        if (line.hasOption('n'))
            config.dryrun = true;
        if (line.hasOption('d'))
            config.nodeps = true;
        if (line.hasOption('t'))
            config.test_mode = true;

        int njobs = -1;
        String mname = DEFAULT_MAKEFILE_NAME;

        if (line.hasOption('j'))
            njobs = Integer.parseInt(line.getOptionValue('j'));
        if (line.hasOption('f'))
            mname = line.getOptionValue('f');                

        MakefileParser makefileParser = new MakefileParser();        

        Hamake make = null;        

        InputStream is = null;
        try {
            Configuration hadoopCfg = new Configuration();
            Path makefilePath = new Path(mname);
            FileSystem fs = makefilePath.getFileSystem(hadoopCfg);
            is = fs.open(makefilePath);
            make = makefileParser.parse(is, config.verbose);
            if(line.getArgs().length > 0){
            	for(String target : line.getArgs()){
            		make.addTarget(target);
            	}
            }
            make.setFileSystem(FileSystem.get(hadoopCfg));            
        } catch (IOException ex) {
            System.err.println("Cannot load makefile " + mname + ": " + ex.getMessage());
            if (config.test_mode)
                ex.printStackTrace();
            System.exit(ExitCodes.INITERR.ordinal());
        } catch (ParserConfigurationException ex) {
            System.err.println("Cannot initialize XML parser: " + ex.getMessage());
            if (config.test_mode)
                ex.printStackTrace();
            System.exit(ExitCodes.INITERR.ordinal());
        } catch (SAXException ex) {
            System.err.println("Invalid makefile content: " + ex.getMessage());
            if (config.test_mode)
                ex.printStackTrace();
            System.exit(ExitCodes.INITERR.ordinal());
        } catch (InvalidMakefileException ex) {
            System.err.println("Error in makefile: " + ex.getMessage());
            if (config.test_mode)
                ex.printStackTrace();
            System.exit(ExitCodes.INITERR.ordinal());
        } finally {
            IOUtils.closeQuietly(is);
        }

        make.setNumJobs(njobs);

        try {
            System.exit(make.run().ordinal());
        } catch (Exception ex) {
            System.err.println(ex.getMessage());
            if (config.test_mode)
                ex.printStackTrace();
            System.exit(ExitCodes.FAILED.ordinal());
        }

    }
}