package com.codeminders.hamake.commands;

import com.codeminders.hamake.Config;
import com.codeminders.hamake.HamakePath;
import com.codeminders.hamake.Param;
import com.codeminders.hamake.NamedParam;
import com.codeminders.hamake.params.PigParam;
import com.codeminders.hamake.params.PathParam;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import java.util.Map;
import java.util.Properties;

public class PigCommand extends BaseCommand {
	
	public static final Log LOG = LogFactory.getLog(PigCommand.class);

    private HamakePath script;

    public PigCommand() {
    }

    public PigCommand(HamakePath script, List<Param> parameters) {
        setScript(script);
        setParameters(parameters);
    }

    public int execute(Map<String, Collection> parameters, Map<String, Object> context) {
        FileSystem fs;
        Collection<String> args = new ArrayList<String>();
        BufferedReader in = null;

        try {
            fs = script.getFileSystem();

            Collection<Param> scriptParams = getParameters();
            if (scriptParams != null) {
                for (Param p : scriptParams) {
                    if (p instanceof PigParam || p instanceof PathParam) {
                        Collection<String> values = getValues((NamedParam)p, parameters, fs);
                        if (values == null) return -1000;
                        args.add(((NamedParam)p).getName() + '=' + values.iterator().next());
                    }
                }
            }

            // TODO: Do we need to define some properties?
            Properties pigProps = new Properties();
            PigContext ctx = new PigContext(ExecType.MAPREDUCE, pigProps);

            // Run, using the provided file as a pig file
            Path p = fs.makeQualified(getScript().getPathName());
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
                                                   "PigLatin:" + getScript());

            substFile.deleteOnExit();

            Grunt grunt = new Grunt(pin, ctx);
            int results[] = grunt.exec();
            // results:
            // 0: succeeded
            // 1: failed
            return results[1] == 0 ? 0 : -1000;

        } catch (ExecException ex) {
        	LOG.error("Failed to execute PIG command " + getScript(), ex);
            return -1000;
        } catch (IOException ex) {
        	LOG.error("Failed to execute PIG command " + getScript(), ex);
            return -1000;
        } catch (ParseException ex) {
        	LOG.error("Failed to execute PIG command " + getScript(), ex);
            return -1000;
        } catch (Throwable ex) {
        	LOG.error("Failed to execute PIG command " + getScript(), ex);
            return -1000;
        } finally {
            IOUtils.closeQuietly(in);
        }
    }

    public HamakePath getScript() {
        return script;
    }

    public void setScript(HamakePath script) {
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

    protected Collection<String> getValues(NamedParam p, Map<String, Collection> parameters, FileSystem fs)
    {
        Collection<String> values;
        try {
            values = p.get(parameters, fs);
        } catch (IOException ex) {
        	LOG.error("Failed to extract parameter values from parameter " +
                    p.getName(), ex);
            return null;
        }
        if (values.size() != 1) {
        	LOG.error("Multiple values for param are no supported in PIG scripts");
            return null;
        }

        return values;
    }

}
