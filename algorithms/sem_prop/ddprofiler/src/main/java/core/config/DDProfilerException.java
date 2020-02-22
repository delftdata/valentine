/**
 * @author Raul - raulcf@csail.mit.edu
 *
 */
package core.config;

public class DDProfilerException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public DDProfilerException(String message, Throwable cause) {
    super(message, cause);
  }

  public DDProfilerException(String message) { super(message); }

  public DDProfilerException(Throwable cause) { super(cause); }

  public DDProfilerException() { super(); }
}
