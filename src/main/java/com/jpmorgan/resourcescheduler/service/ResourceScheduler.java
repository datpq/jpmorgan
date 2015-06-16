package com.jpmorgan.resourcescheduler.service;

public interface ResourceScheduler {
	public void cancelGroup(String groupId);
	public void registerResource(Resource rs);
	public void receiveMessage(Message[] arrMsg);
	public Message getFirstMessageByPriority();
	public Gateway getGateway();
	public void completed(Message msg);
}
