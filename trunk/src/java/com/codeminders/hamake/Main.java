package com.codeminders.hamake;

import org.apache.commons.cli.*;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.VersionInfo;
import org.xml.sax.SAXException;

import com.codeminders.hamake.context.Context;
import com.codeminders.hamake.syntax.BaseSyntaxParser;
import com.codeminders.hamake.syntax.InvalidMakefileException;

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

        boolean verbose = false;
        boolean dryRun = false;
        boolean withDeps = true;

        if (line.hasOption('V')) {
            System.out.println("HAMake version " + Hamake.HAMAKE_VERSION);
            System.exit(0);
        }

        if (line.hasOption('v'))
        	verbose = true;
        if (line.hasOption('n'))
        	dryRun = true;
        if (line.hasOption('d'))
        	withDeps = true;

        int njobs = -1;
        String mname = DEFAULT_MAKEFILE_NAME;
        boolean localFs = true;
        String wdir = null;
        Configuration hadoopCfg = new Configuration();

        if (line.hasOption('j'))
            njobs = Integer.parseInt(line.getOptionValue('j'));
        if (line.hasOption('f')) {
            mname = line.getOptionValue('f');
            localFs = false;
        }

        if (line.hasOption('w'))
            wdir = line.getOptionValue('w');

        Hamake make = null;

        InputStream is = null;
        try {
            LOG.info("Using Hadoop " + VersionInfo.getVersion());

            if (StringUtils.isEmpty(wdir))
                wdir = FileSystem.get(hadoopCfg).getWorkingDirectory().toString();

            LOG.info("Working dir:  " + wdir);
            LOG.info("Reading hamake-file " + mname);
            if (localFs) {
                is = new FileInputStream(mname);
            }
            else {
                Path makefilePath = new Path(mname);
                FileSystem fs = makefilePath.getFileSystem(hadoopCfg);
                is = fs.open(makefilePath);
            }
            Context context = new Context(hadoopCfg, wdir, withDeps, verbose, dryRun);
            LOG.info("Parsing hamake-file " + mname);
            make = BaseSyntaxParser.parse(context, is);
            if(line.getArgs().length > 0){
            	for(String target : line.getArgs()){
            		make.addTarget(target);
            	}
            }
        } catch (IOException ex) {
        	LOG.error("Cannot load makefile " + mname, ex);
            System.exit(ExitCodes.INITERR.ordinal());
        } catch (ParserConfigurationException ex) {
        	LOG.error("Cannot initialize XML parser: ", ex);
            System.exit(ExitCodes.INITERR.ordinal());
        } catch (SAXException ex) {
        	LOG.error("Invalid makefile content: ", ex);
            System.exit(ExitCodes.INITERR.ordinal());
        } catch (InvalidMakefileException ex) {
        	LOG.error("Error in makefile: ", ex);
            System.exit(ExitCodes.INITERR.ordinal());
        } catch (PigNotFoundException ex) {
        	LOG.error("Cannot execute Pig task: ", ex);
            System.exit(ExitCodes.INITERR.ordinal());
        } catch(InvalidContextStateException e){
        	LOG.error("Cannot execute Pig task: ", e);
        	System.exit(ExitCodes.INITERR.ordinal());
        }catch(Exception e){
        	LOG.error("Error occured", e);
        	System.exit(ExitCodes.INITERR.ordinal());
        } finally {
            IOUtils.closeQuietly(is);
        }
        if("local".equals(hadoopCfg.get("mapred.job.tracker", "local"))) make.setNumJobs(1);
        else make.setNumJobs(njobs);        

        int status;
        try {
            status = make.run().ordinal();
        } catch (Exception ex) {
        	LOG.error(ex);
            status = ExitCodes.FAILED.ordinal();
        }        
        System.exit(status);

    }    

}