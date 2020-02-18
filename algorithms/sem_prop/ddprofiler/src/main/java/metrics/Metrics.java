package metrics;

import java.util.concurrent.TimeUnit;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;


public class Metrics extends MetricRegistry {
	
	final public static MetricRegistry REG = new MetricRegistry();
	
	private static JmxReporter jmxReporter;
	private static ConsoleReporter consoleReporter;
	private static CsvReporter csvReporter;
	private static Slf4jReporter slf4jReporter;
	
	
	public static void startJMXReporter() {
		jmxReporter = JmxReporter.forRegistry(REG).build();
		jmxReporter.start();
	}
	
	public static void startConsoleReporter(int period) {
		// TODO: implement console reporter, useful for debugging
		consoleReporter = ConsoleReporter.forRegistry(REG)
				.convertRatesTo(TimeUnit.SECONDS)
				.convertDurationsTo(TimeUnit.MILLISECONDS)
				.build();
		consoleReporter.start(period, TimeUnit.SECONDS);
	}
	
	public static void startCSVReporter() {
		// TODO: implement console reporter, useful for experiments
	}
	
	public static void startSLF4JReporter() {
		// TODO: implement slf4j reporter, useful for stable versions
	}
	
}

