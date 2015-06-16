package com.jpmorgan.resourcescheduler.messages;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jpmorgan.resourcescheduler.service.Message;
import com.jpmorgan.resourcescheduler.service.Resource;

public class MessageImpl implements Message {
	private static final Logger log = LoggerFactory.getLogger(MessageImpl.class);

	private final String groupId;
    private final int id;
    private final boolean termination;
    private Resource res;
    
    public MessageImpl(int id, String groupId) {
    	this(id, groupId, false);
    }
    
    public MessageImpl(int id, String groupId, boolean termination) {
    	this.id = id;
    	this.groupId = groupId;
    	this.termination = termination;
    }

    public void completed() {
		if (log.isDebugEnabled()) {
			log.debug(toString() + " is completed");
		}
        if (res != null) {
        	res.completed(this);
        }
    }
    
    public void registerResource(Resource res) {
    	this.res = res;
    }
    
    public String getGroupId() {
        return groupId;
    }

    public int getId() {
        return id;
    }
    
    public Resource getResource() {
    	return res;
    }
    
    public boolean IsTermination() {
    	return termination;
    }
    
    @Override
    public String toString() {
        String result = String.format("Message Id=%s GroupId=%s", id, groupId);
        return result;
    }

}
