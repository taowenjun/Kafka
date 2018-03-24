package cn.bit.tao.log4jproducer;

import org.apache.log4j.Logger;

/**
 *@author  Tao wenjun
 *
 */

public class Log4jProducer {
	private static final Logger LOG=Logger.getLogger(Log4jProducer.class);
    public static void main(String[] args) {
    	LOG.info("this is a info message");
    	LOG.error("this is a error message");
    }
}
