package com.jpmorgan.resourcescheduler.gateways;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jpmorgan.resourcescheduler.service.Gateway;
import com.jpmorgan.resourcescheduler.service.Message;

public class GatewayImpl implements Gateway {
	private static final Logger log = LoggerFactory.getLogger(GatewayImpl.class);

	private final ExecutorService threadPool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
	
	public GatewayImpl() {
		
	}
	
	public void send(Message msg) {
		if (log.isDebugEnabled()) {
			log.debug("sending " + msg.toString() + "...");
		}
		threadPool.submit(new MessageTask(msg));
	}
	
    private class MessageTask implements Runnable {
        private final Message msg;

        private MessageTask(Message msg) {
            this.msg = msg;
        }

        public void run() {
    		if (log.isDebugEnabled()) {
    			log.debug("processing " + msg.toString() + "...");
    		}
        	//message processing here
            safeSleep(5);
            msg.completed();
        }
    }
    
    private void safeSleep(long l) {
        try {
            Thread.sleep(l);
        } catch (InterruptedException e) {
            //ignore
        }
    }

}
