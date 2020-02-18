package core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import core.config.CommandLineArgs;
import core.config.ConfigKey;
import core.config.ProfilerConfig;
import joptsimple.OptionParser;
import metrics.Metrics;
import sources.Source;
import sources.SourceType;
import sources.YAMLParser;
import sources.config.SourceConfig;
import store.Store;
import store.StoreFactory;

public class Main {

    final private Logger LOG = LoggerFactory.getLogger(Main.class.getName());

    public enum ExecutionMode {
	ONLINE(0), OFFLINE_FILES(1), OFFLINE_DB(2), BENCHMARK(3);

	int mode;

	ExecutionMode(int mode) {
	    this.mode = mode;
	}
    }

    public void startProfiler(ProfilerConfig pc) {

	long start = System.nanoTime();

	// Default is elastic, if we have more in the future, just pass a
	// property to configure this
	Store s = StoreFactory.makeStoreOfType(pc.getInt(ProfilerConfig.STORE_TYPE), pc);

	Conductor c = new Conductor(pc, s);
	c.start();

	// TODO: Configure mode here ?
	// int executionMode = pc.getInt(ProfilerConfig.EXECUTION_MODE);
	// if (executionMode == ExecutionMode.ONLINE.mode) {
	// // Start infrastructure for REST server
	// WebServer ws = new WebServer(pc, c);
	// ws.init();
	// }

	// Parsing sources config file
	String sourceConfigFile = pc.getString(ProfilerConfig.SOURCE_CONFIG_FILE);
	LOG.info("Using {} as sources file", sourceConfigFile);
	List<SourceConfig> sourceConfigs = YAMLParser.processSourceConfig(sourceConfigFile);

	List<Source> allSources = new ArrayList<>();
	LOG.info("Found {} sources to profile", sourceConfigs.size());
	for (SourceConfig sourceConfig : sourceConfigs) {
	    String sourceName = sourceConfig.getSourceName();
	    SourceType sType = sourceConfig.getSourceType();
	    LOG.info("Processing source {} of type {}", sourceName, sType);
	    Source source = SourceType.instantiateSourceOfType(sType);
	    List<Source> sources = source.processSource(sourceConfig);
	    allSources.addAll(sources);
	}
	for (Source source : allSources) {
	    c.submitTask(source);
	}

	while (c.isTherePendingWork()) {
	    try {
		Thread.sleep(3000);
	    } catch (InterruptedException e) {
		e.printStackTrace();
	    }
	}

	c.stop();
	s.tearDownStore();

	long end = System.nanoTime();
	LOG.info("Finished processing in {}", (end - start));
    }

    public static void main(String args[]) {

	// Get Properties with command line configuration
	List<ConfigKey> configKeys = ProfilerConfig.getAllConfigKey();
	OptionParser parser = new OptionParser();
	// Unrecognized options are passed through to the query
	parser.allowsUnrecognizedOptions();
	CommandLineArgs cla = new CommandLineArgs(args, parser, configKeys);
	Properties commandLineProperties = cla.getProperties();

	// Check if the user requests help
	for (String a : args) {
	    if (a.contains("help") || a.equals("?")) {
		try {
		    parser.printHelpOn(System.out);
		    System.exit(0);
		} catch (IOException e) {
		    e.printStackTrace();
		}
	    }
	}

	Properties validatedProperties = validateProperties(commandLineProperties);

	ProfilerConfig pc = new ProfilerConfig(validatedProperties);

	// Start main
	configureMetricsReporting(pc);

	// config logs
	configLog();

	Main m = new Main();
	m.startProfiler(pc);

    }

    private static void configLog() {
	final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger("com.zaxxer.hikari");
	if (!(logger instanceof ch.qos.logback.classic.Logger)) {
	    return;
	}
	ch.qos.logback.classic.Logger logbackLogger = (ch.qos.logback.classic.Logger) logger;
	logbackLogger.setLevel(ch.qos.logback.classic.Level.WARN);

	// org.apache.log4j.Logger root =
	// org.apache.log4j.Logger.getRootLogger();
	// org.apache.log4j.Appender appender = new
	// org.apache.log4j.ConsoleAppender();
	// root.addAppender(appender);
    }

    static private void configureMetricsReporting(ProfilerConfig pc) {
	int reportConsole = pc.getInt(ProfilerConfig.REPORT_METRICS_CONSOLE);
	if (reportConsole > 0) {
	    Metrics.startConsoleReporter(reportConsole);
	}
    }

    public static Properties validateProperties(Properties p) {
	// TODO: Go over all properties configured here and validate their
	// ranges,
	// values
	// etc. Stop the program and spit useful doc message when something goes
	// wrong.
	// Return the unmodified properties if everything goes well.

	return p;
    }
}
