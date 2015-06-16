package com.jpmorgan.resourcescheduler;

import java.util.Arrays;
import java.util.List;
import java.util.Vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jpmorgan.resourcescheduler.service.Gateway;
import com.jpmorgan.resourcescheduler.service.Message;
import com.jpmorgan.resourcescheduler.service.Resource;
import com.jpmorgan.resourcescheduler.service.ResourceScheduler;

public class ResourceSchedulerImpl implements ResourceScheduler {
    private static final Logger log = LoggerFactory.getLogger(ResourceSchedulerImpl.class);

    private final List<Resource> arrResources;
    private final List<Message> arrMessages;// the queue of messages to be processed
    private final List<Message> arrSentMessages;//the list of messages already sent
    private final List<String> arrCancelledGroup;//the list of cancelled groupId of message
    private final List<String> arrTerminationGroup;//the list of groupId which is finished.
    private List<String> arrGroupIdInProgress;
    private final Gateway gateway;
    
    public ResourceSchedulerImpl(Gateway gateway) {
        if (log.isDebugEnabled()) {
            log.debug("ResourceSchedulerImpl created");
        }
        this.arrResources = new Vector<Resource>();
        this.arrMessages = new Vector<Message>();
        this.arrSentMessages = new Vector<Message>();
        this.arrGroupIdInProgress = new Vector<String>();
        this.arrCancelledGroup = new Vector<String>();
        this.arrTerminationGroup = new Vector<String>();
        this.gateway = gateway;
    }
    
    public Gateway getGateway() {
    	return gateway;
    }
    
    public void registerResource(Resource res) {
        if (log.isDebugEnabled()) {
            log.debug("registerResource id = " + res.getId());
        }
        arrResources.add(res);
    }
    
    public void receiveMessage(Message[] arrMsg) {
        if (log.isDebugEnabled()) {
            log.debug("receiveMessage length = " + arrMsg.length);
            for(Message msg : arrMsg) {
                log.debug(msg.toString());
            }
        }
        arrMessages.addAll(Arrays.asList(arrMsg));
        process();
    }
    
    public void cancelGroup(String groupId) {
    	if (log.isDebugEnabled()) {
    		log.debug("cancel group " + groupId);
    	}
    	arrCancelledGroup.add(groupId);
    }
    
    public Message getFirstMessageByPriority() {
    	Message result = null;
    	for(int i=0; i<arrMessages.size(); i++) {
    		if (arrGroupIdInProgress.contains(arrMessages.get(i).getGroupId())) {
    			result = arrMessages.get(i);
    			break;
    		}
    	}
    	//if no message which is in a in progress group ==> take the first one
    	if (result == null && arrMessages.size() > 0) {
    		result = arrMessages.get(0);
    	}
    	return result;
    }

    //this method is synchronized
	private synchronized void process() {
		Message msg = getFirstMessageByPriority();
		//verify if the message's group was cancelled ==> cancel the message
		while (msg != null) {
			if (arrCancelledGroup.contains(msg.getGroupId())) {
				if (log.isDebugEnabled()) {
					log.debug(msg.toString() + " was cancelled ==> remove from queue");
				}
				arrMessages.remove(msg);
				msg = getFirstMessageByPriority();
			} else if (arrTerminationGroup.contains(msg.getGroupId())) {
				if (log.isDebugEnabled()) {
					log.debug("Already received termination message of group " + msg.getGroupId() + ". Do not process this group anymore ==> remove from queue");
				}
				arrMessages.remove(msg);
				msg = getFirstMessageByPriority();
			} else {//the message is OK
				break;
			}
		}
		if (msg == null) {
			if (log.isDebugEnabled()) {
				log.debug("No more message in the queue.");
			}
			return;
		}
		Resource res = null;
		for(Resource rs : arrResources) {
			if (rs.isAvailable()) {
				res = rs;
				break;
			}
		}
		if (res == null) {
			if (log.isDebugEnabled()) {
				log.debug("No more resource available to process " + msg.toString() + ". Waiting for the an available resource...");
			}
			return;
		}
		arrMessages.remove(msg);
		if (msg.IsTermination()) {
			arrTerminationGroup.add(msg.getGroupId());
		}
		res.send(msg);
		if (log.isDebugEnabled()) {
			log.debug("Continue processing...");
		}
		process();
	}
	
	public void completed(Message msg) {
		arrSentMessages.add(msg);
		//store all in progress GroupId
		if (!arrGroupIdInProgress.contains(msg.getGroupId())) {
			arrGroupIdInProgress.add(msg.getGroupId());
		}
		if (log.isDebugEnabled()) {
			log.debug("There is available resource. Find if there's any message in the queue.");
		}
		process();
	}
	
	public List<Message> getSentMessages() {
		return arrSentMessages;
	}
}
