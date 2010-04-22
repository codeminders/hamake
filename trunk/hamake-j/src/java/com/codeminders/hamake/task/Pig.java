package com.codeminders.hamake.task;

import com.codeminders.hamake.Config;
import com.codeminders.hamake.Context;
import com.codeminders.hamake.Hamake;
import com.codeminders.hamake.params.HamakeParameter;
import com.codeminders.hamake.params.Parameter;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pig.ExecType;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.impl.PigContext;
import org.apache.pig.tools.grunt.Grunt;
import org.apache.pig.tools.parameters.ParameterSubstitutionPreprocessor;
import org.apache.pig.tools.parameters.ParseException;

import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Properties;

public class Pig extends Task {
	
	public static final Log LOG = LogFactory.getLog(Pig.class);

    private Path script;

    public Pig() {
    }

    public int execute(Context context) {
        FileSystem fs;
        Configuration conf = context.get(Hamake.SYS_PROPERTY_HADOOP_CONFIGURATION) != null? (Configuration)context.get(Hamake.SYS_PROPERTY_HADOOP_CONFIGURATION) : new Configuration();
        Collection<String> args = new ArrayList<String>();
        BufferedReader in = null;

        try {
            fs = script.getFileSystem(conf);

            List<Parameter> parameters = getParameters();
            if (parameters != null) {
                for (Parameter p : parameters) {
                    args.add(p.get(context));
                }
            }

            // TODO: Do we need to define some properties?
            Properties pigProps = new Properties();
            PigContext ctx = new PigContext(ExecType.MAPREDUCE, pigProps);

            // Run, using the provided file as a pig file
            Path p = fs.makeQualified(script);
            in = new BufferedReader(new InputStreamReader(fs.open(p)));
            // run parameter substitution preprocessor first
            File substFile = File.createTempFile("subst", ".pig");
            BufferedReader pin = preprocessPigScript(in, args, substFile, Config.getInstance().dryrun);
            if (Config.getInstance().dryrun) {
            	LOG.info("Substituted pig script is at " + substFile);
                return 0;
            }

            // Set job name based on name of the script
            ctx.getProperties().setProperty(PigContext.JOB_NAME,
                                                   "PigLatin:" + script);

            substFile.deleteOnExit();

            Grunt grunt = new Grunt(pin, ctx);
            int results[] = grunt.exec();
            // results:
            // 0: succeeded
            // 1: failed
            return results[1] == 0 ? 0 : -1000;

        } catch (ExecException ex) {
        	LOG.error("Failed to execute PIG command " + script, ex);
            return -1000;
        } catch (IOException ex) {
        	LOG.error("Failed to execute PIG command " + script, ex);
            return -1000;
        } catch (ParseException ex) {
        	LOG.error("Failed to execute PIG command " + script, ex);
            return -1000;
        } catch (Throwable ex) {
        	LOG.error("Failed to execute PIG command " + script, ex);
            return -1000;
        } finally {
            IOUtils.closeQuietly(in);
        }
    }

    public Path getScript() {
        return script;
    }

    public void setScript(Path script) {
        this.script = script;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("script", script).appendSuper(super.toString()).toString();
    }

    protected BufferedReader preprocessPigScript(BufferedReader origPigScript,
                                                 Collection<String> params,
                                                 File scriptFile,
                                                 boolean createFile)
            throws ParseException, IOException {
        ParameterSubstitutionPreprocessor psp = new ParameterSubstitutionPreprocessor(50);
        String[] type1 = new String[1];

        if (createFile) {
            BufferedWriter fw = new BufferedWriter(new FileWriter(scriptFile));
            psp.genSubstitutedFile(origPigScript, fw, params.size() > 0 ? params.toArray(type1) : null,
                    null);
            return new BufferedReader(new FileReader(scriptFile));

        } else {
            StringWriter writer = new StringWriter();
            psp.genSubstitutedFile(origPigScript, writer, params.size() > 0 ? params.toArray(type1) : null,
                    null);
            return new BufferedReader(new StringReader(writer.toString()));
        }
    }

}
