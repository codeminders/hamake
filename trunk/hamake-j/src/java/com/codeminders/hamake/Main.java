package com.codeminders.hamake;

import org.apache.commons.cli.*;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.SystemUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.VersionInfo;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;
import java.io.InputStream;
import java.io.FileInputStream;

public class Main {
	
	public static final Log LOG = LogFactory.getLog(Main.class);

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
        options.addOption("d", "nodeps", false, "no deps mode");
        options.addOption("t", "test", false, "test mode");
        options.addOption("j", "jobs", true, "number of job threads to spawn");
        options.addOption("f", "file", true, "makefile location, hamakefile.xml on local filesystem if not specified");
        options.addOption("w", "workdir", true, "path to data, default is user home dir");

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
        boolean localFs = true;
        String wdir = null;

        if (line.hasOption('j'))
            njobs = Integer.parseInt(line.getOptionValue('j'));
        if (line.hasOption('f')) {
            mname = line.getOptionValue('f');
            localFs = false;
        }
        if (line.hasOption('w'))
        {
            wdir = line.getOptionValue('w');
            if (StringUtils.isEmpty(wdir))
                wdir = SystemUtils.getUserHome().getAbsolutePath();
        }

        MakefileParser makefileParser = new MakefileParser();

        Hamake make = null;        

        InputStream is = null;
        try {
            Configuration hadoopCfg = new Configuration();
            LOG.info("Using Hadoop " + VersionInfo.getVersion());

            if (localFs) {
                is = new FileInputStream(mname);
            }
            else {
                Path makefilePath = new Path(mname);
                FileSystem fs = makefilePath.getFileSystem(hadoopCfg);
                is = fs.open(makefilePath);
            }
            make = makefileParser.parse(is, wdir, config.verbose);
            if(line.getArgs().length > 0){
            	for(String target : line.getArgs()){
            		make.addTarget(target);
            	}
            }
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
        } catch (PigNotFoundException ex) {
            System.err.println("Cannot execute Pig task: " + ex.getMessage());
            if (config.test_mode)
                ex.printStackTrace();
            System.exit(ExitCodes.INITERR.ordinal());
        } finally {
            IOUtils.closeQuietly(is);
        }

        make.setNumJobs(njobs);        

        int status;
        try {
            status = make.run().ordinal();
        } catch (Exception ex) {
        	LOG.error(ex);
            if (config.test_mode)
                ex.printStackTrace();
            status = ExitCodes.FAILED.ordinal();
        }        
        System.exit(status);

    }    

}