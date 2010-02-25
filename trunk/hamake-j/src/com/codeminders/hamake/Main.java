package com.codeminders.hamake;

import org.apache.commons.cli.*;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.IOException;

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
            line = parser.parse(options, args, true);
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

        try {
            make = makefileParser.parse(mname, config.verbose);
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
        }

        System.out.println(new ToStringBuilder(make).append("tasks", make.getTasks()));

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