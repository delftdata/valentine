/**
 * @author Raul - raulcf@csail.mit.edu
 *
 */
package comm;

import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import core.Conductor;
import core.config.ProfilerConfig;

public class WebServer {

    private Server server;

    public WebServer(ProfilerConfig pc, Conductor c) {
	silenceJettyLogger();
	WebHandler handler = new WebHandler(c);
	this.server = new Server(pc.getInt(ProfilerConfig.WEB_SERVER_PORT));

	// Configure servletHandler
	ServletHandler sHandler = new ServletHandler();
	ServletHolder sh = new ServletHolder(handler);
	sHandler.addServletWithMapping(sh, "/dd");

	// Configure all handlers
	HandlerList handlers = new HandlerList();
	handlers.setHandlers(new Handler[] { sHandler, new DefaultHandler() });
	server.setHandler(handlers);

	// Configure connector
	ServerConnector http = new ServerConnector(server);
	http.setIdleTimeout(30000);
	server.addConnector(http);
    }

    public void init() {
	try {
	    server.start();
	    server.join();
	} catch (Exception e) {
	    // TODO Handle this properly
	    e.printStackTrace();
	} finally {
	    server.destroy();
	}
    }

    public void close() {
	try {
	    server.stop();
	} catch (Exception e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
    }

    private void silenceJettyLogger() {
	final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger("org.eclipse.jetty");
	if (!(logger instanceof ch.qos.logback.classic.Logger)) {
	    return;
	}
	ch.qos.logback.classic.Logger logbackLogger = (ch.qos.logback.classic.Logger) logger;
	logbackLogger.setLevel(ch.qos.logback.classic.Level.INFO);
    }
}
