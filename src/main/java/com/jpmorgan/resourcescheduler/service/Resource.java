package com.jpmorgan.resourcescheduler.service;

public interface Resource {
    public int getId();
    public boolean isAvailable();
    
    //send a message
    public void send(Message msg);
    //called when message has completed processing
    public void completed(Message msg);
}
