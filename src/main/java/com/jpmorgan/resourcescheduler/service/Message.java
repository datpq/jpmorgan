package com.jpmorgan.resourcescheduler.service;

public interface Message {
    public void completed();
    public String getGroupId();
    public int getId();
    public boolean IsTermination();
    
    public void registerResource(Resource res);
}
