package com.jpmorgan.resourcescheduler.resources;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jpmorgan.resourcescheduler.service.Message;
import com.jpmorgan.resourcescheduler.service.Resource;
import com.jpmorgan.resourcescheduler.service.ResourceScheduler;

public class ResourceImpl implements Resource {
	private static final Logger log = LoggerFactory.getLogger(ResourceImpl.class);

	private final int id;
    private boolean available;
    private final ResourceScheduler scheduler;
    
	public ResourceImpl(int id, ResourceScheduler scheduler) {
        this.id = id;
        this.scheduler = scheduler;
        this.available = true;
    }

    public int getId() {
        return id;
    }

    public boolean isAvailable() {
        return available;
    }

    public void send(Message msg) {
    	if (log.isInfoEnabled()) {
    		log.info(msg.toString() + " uses " + toString() + ". This resource is now busy.");
    	}
    	this.available = false;
    	msg.registerResource(this);
    	scheduler.getGateway().send(msg);
    }
    
    public void completed(Message msg) {
    	if (log.isDebugEnabled()) {
    		log.debug(msg.toString() + " is completed. " + toString() + " is now avaiable.");
    	}
    	this.available = true;
    	scheduler.completed(msg);
    }

    @Override
    public String toString() {
        String result = String.format("Resource Id=%s", id);
        return result;
    }
}
