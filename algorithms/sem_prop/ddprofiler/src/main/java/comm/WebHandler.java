/**
 * @author Raul - raulcf@csail.mit.edu
 *
 */
package comm;

import java.io.IOException;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import core.Conductor;
import sources.config.CSVSourceConfig;

public class WebHandler extends HttpServlet {

    private static final long serialVersionUID = 1L;
    final private static Logger LOG = LoggerFactory.getLogger(WebHandler.class);

    // Jackson (JSON) serializer
    private ObjectMapper om = new ObjectMapper();
    private Conductor c;

    public WebHandler(Conductor c) {
	this.c = c;
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response)
	    throws ServletException, IOException {
	// Get parameter map
	Map<String, String[]> parameters = request.getParameterMap();
	String[] actionIdValues = parameters.get("actionid");
	String actionId = actionIdValues[0];
	String result = handleAction(actionId, parameters);
	response.setContentType("text");
	response.getWriter().println(result);
    }

    private String handleAction(String action, Map<String, String[]> parameters) {
	String response = null;

	switch (action) {
	case "initStore":
	    String[] dbname = parameters.get("dbname");
	    initStore(dbname[0]);
	    return "OK";
	case "processCSVDataSource":
	    String[] dbName = parameters.get("dbName");
	    String[] conn = parameters.get("path");
	    String[] name = parameters.get("source");
	    String[] separator = parameters.get("separator");
	    response = processCSVDataSource(dbName[0], conn[0], name[0], separator[0]);
	    return response;

	// test functions

	case "test":
	    test();
	    return "OK";
	case "test2":
	    String[] a = parameters.get("a");
	    String[] b = parameters.get("b");
	    String[] c = parameters.get("c");
	    response = test2(a[0], b[0], c[0]);
	    return response;
	case "test3":
	    a = parameters.get("a");
	    b = parameters.get("b");
	    response = test3(a[0], b[0]);
	    return response;
	default:
	}
	return "FAIL";
    }

    private String processCSVDataSource(String dbName, String path, String name, String separator) {
	// FIXME: Dummy csvconfig. Create fluent iface to configure these
	CSVSourceConfig csvConfig = new CSVSourceConfig();
	csvConfig.setSourceName(dbName);
	csvConfig.setPath(path);
	csvConfig.setSeparator(separator);
	// FIXME: make a source task for csv files

	boolean success = true; // c.submitTask(wt);
	if (success)
	    return "OK";
	return "FAIL";
    }

    public boolean initStore(String dbname) {
	// TODO: initialize connection to store
	return true;
    }

    /**
     * Embedded Testing functions
     */

    public String test() {
	return "Hello World";
    }

    public String test2(String a, String b, String c) {
	return a + "-" + b + "-" + c;
    }

    public String test3(String a, String b) {
	Test t = new Test(a, b);
	String response = "";
	try {
	    response = om.writeValueAsString(t);
	} catch (JsonProcessingException e) {
	    // TODO Auto-generated catch block
	    e.printStackTrace();
	}
	return response;
    }

    class Test {
	private String a;
	private String b;

	public Test(String a, String b) {
	    this.a = a;
	    this.b = b;
	}

	public String getA() {
	    return a;
	}

	public void setA(String a) {
	    this.a = a;
	}

	public String getB() {
	    return b;
	}

	public void setB(String b) {
	    this.b = b;
	}
    }
}
