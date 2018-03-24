package cn.bit.tao.appender;

import org.apache.log4j.DailyRollingFileAppender;
import org.apache.log4j.Priority;

/**
 *@author  tao wenjun
 *×Ô¶¨ÒåAppender
 */

public class LogAppender extends DailyRollingFileAppender {
	
	@Override
	public boolean isAsSevereAsThreshold(Priority priority){
		return this.getThreshold().equals(priority);
	}
}
