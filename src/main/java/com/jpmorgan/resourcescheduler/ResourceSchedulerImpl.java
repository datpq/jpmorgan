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
    
    /**
     * return the associated Gateway
     */
    public Gateway getGateway() {
    	return gateway;
    }
    
    /**
     * register a Resource
     */
    public void registerResource(Resource res) {
        if (log.isDebugEnabled()) {
            log.debug("registerResource id = " + res.getId());
        }
        arrResources.add(res);
    }
    
    /**
     * Receive an array of messages
     */
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
    
    /**
     * Cancel a group of message
     */
    public void cancelGroup(String groupId) {
    	if (log.isDebugEnabled()) {
    		log.debug("cancel group " + groupId);
    	}
    	arrCancelledGroup.add(groupId);
    }
    
    /**
     * This method can be overridden to use a different Message prioritisation
     * algorithms to select the next Message from the queue
     */
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

    /**
     * Process the queue of message.
     * This method is synchronized because it may be called from other Thread.
     * Is called from main thread and from the thread where message.completed is called, resource is
     * available.
     */
	private synchronized void process() {
		Message msg = getFirstMessageByPriority();
		while (msg != null) {
			//if group is already cancelled ==> remove the message from queue
			if (arrCancelledGroup.contains(msg.getGroupId())) {
				if (log.isDebugEnabled()) {
					log.debug(msg.toString() + " was cancelled ==> remove from queue");
				}
				arrMessages.remove(msg);
				msg = getFirstMessageByPriority();
			//if group is already terminated ==> remove the message from queue
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
		//find the first available resource
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
		res.send(msg);//send message
		
		//once sent, continue processing, check if there's still message in the queue and available resource
		if (log.isDebugEnabled()) {
			log.debug("Continue processing...");
		}
		process();
	}
	
	/**
	 * When message completed
	 */
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
	
	/**
	 * specific for this Implementation
	 * @return
	 */
	public List<Message> getSentMessages() {
		return arrSentMessages;
	}
}
