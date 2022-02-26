package ae.pegasus.torascheduler.commons;

import org.owasp.esapi.ESAPI;
import org.owasp.esapi.Logger;

public class ESAPILogger {

    private Logger logger = null;

    public ESAPILogger(Class classname){
       logger  = ESAPI.getLogger(classname);
    }

    public void error(String message){ logger.error(null, message); }

    public void info(String message){
        logger.info(null, message);
    }

    public void warn(String message){
        logger.warning(null,message);
    }
}
