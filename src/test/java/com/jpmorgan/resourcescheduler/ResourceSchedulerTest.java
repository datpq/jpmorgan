package com.jpmorgan.resourcescheduler;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jpmorgan.resourcescheduler.gateways.GatewayImpl;
import com.jpmorgan.resourcescheduler.messages.MessageImpl;
import com.jpmorgan.resourcescheduler.resources.ResourceImpl;
import com.jpmorgan.resourcescheduler.service.Gateway;
import com.jpmorgan.resourcescheduler.service.Message;
import com.jpmorgan.resourcescheduler.service.Resource;
import com.jpmorgan.resourcescheduler.service.ResourceScheduler;

public class ResourceSchedulerTest {
    private static final Logger log = LoggerFactory.getLogger(ResourceSchedulerTest.class);
    private Gateway gateway;
    private ResourceScheduler scheduler;

    public ResourceSchedulerTest() {
    }

    @Before
    public void setUp() throws Exception {
    	gateway = new GatewayImpl();
    	scheduler = new ResourceSchedulerImpl(gateway);
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    /**
     * 2 Messages are sent in the correct order. And use the same resource
     */
    public void testOneResourceTwoMessages() {
    	Resource rs1 = new ResourceImpl(1, scheduler);
        scheduler.registerResource(rs1);
        
        Message msg1 = new MessageImpl(1, "group1");
        Message msg2 = new MessageImpl(2, "group2");
        scheduler.receiveMessage(new Message[] {msg1, msg2});
        safeSleep(100);
        
        //Test. The order of sending must be msg1, msg2. They uses the same resource rs1
        List<Message> arrSentMessages = ((ResourceSchedulerImpl)scheduler).getSentMessages();
        assertEquals(arrSentMessages.size(), 2);
        assertEquals(arrSentMessages.get(0), msg1);//msg1 was sent firstly
        assertEquals(((MessageImpl)msg1).getResource(), rs1);//msg1 has used rs1 to send over gateway
        assertEquals(arrSentMessages.get(1), msg2);//msg2 was sent finally
        assertEquals(((MessageImpl)msg2).getResource(), rs1);//msg2 has used rs1 to send over gateway
    }

    @Test
    /**
     * Send 4 messages in the 3 groups. Verify here the correct order of sending.
     */
    public void testOneResourceMessagesSentByGroup() {
    	Resource rs1 = new ResourceImpl(1, scheduler);
        scheduler.registerResource(rs1);
        
        Message msg1 = new MessageImpl(1, "group2");
        Message msg2 = new MessageImpl(2, "group1");
        Message msg3 = new MessageImpl(3, "group2");
        Message msg4 = new MessageImpl(4, "group3");
        scheduler.receiveMessage(new Message[] {msg1, msg2, msg3, msg4});
        safeSleep(100);
        
        //Test. The order of sending must be msg1, msg3, msg2, msg4
        List<Message> arrSentMessages = ((ResourceSchedulerImpl)scheduler).getSentMessages();
        assertEquals(arrSentMessages.size(), 4);
        assertEquals(arrSentMessages.get(0), msg1);//msg1 was sent firstly
        assertEquals(((MessageImpl)msg1).getResource(), rs1);//msg1 has used rs1 to send over gateway
        assertEquals(arrSentMessages.get(1), msg3);//msg3 was sent next
        assertEquals(((MessageImpl)msg3).getResource(), rs1);//msg3 has used rs1 to send over gateway
        assertEquals(arrSentMessages.get(2), msg2);//msg3 was sent next
        assertEquals(((MessageImpl)msg2).getResource(), rs1);//msg3 has used rs1 to send over gateway
        assertEquals(arrSentMessages.get(3), msg4);//msg4 was sent finally
        assertEquals(((MessageImpl)msg4).getResource(), rs1);//msg3 has used rs1 to send over gateway
    }

    @Test
    /**
     * 2 Messages are sent in the correct order. And uses 2 different resource
     */
    public void testTwoResourceTwoMessages() {
    	Resource rs1 = new ResourceImpl(1, scheduler);
    	Resource rs2 = new ResourceImpl(2, scheduler);
        scheduler.registerResource(rs1);
        scheduler.registerResource(rs2);
        
        Message msg1 = new MessageImpl(1, "group1");
        Message msg2 = new MessageImpl(2, "group2");
        scheduler.receiveMessage(new Message[] {msg1, msg2});
        safeSleep(100);
        
        //Test. The order of sending must be msg1, msg2. But 2 messages uses 2 different resources
        List<Message> arrSentMessages = ((ResourceSchedulerImpl)scheduler).getSentMessages();
        assertEquals(arrSentMessages.size(), 2);
        assertEquals(arrSentMessages.get(0), msg1);//msg1 was sent firstly
        assertEquals(((MessageImpl)msg1).getResource(), rs1);//msg1 has used rs1 to send over gateway
        assertEquals(arrSentMessages.get(1), msg2);//msg2 was sent finally
        assertEquals(((MessageImpl)msg2).getResource(), rs2);//msg2 has used rs2 to send over gateway
    }
    
    @Test
    /**
     * receive 4 messages, cancel 1 group ==> only 3 messages were sent
     */
    public void testCancelGroup() {
    	Resource rs1 = new ResourceImpl(1, scheduler);
        scheduler.registerResource(rs1);
        
        Message msg1 = new MessageImpl(1, "group2");
        Message msg2 = new MessageImpl(2, "group1");
        Message msg3 = new MessageImpl(3, "group2");
        Message msg4 = new MessageImpl(4, "group3");
        scheduler.receiveMessage(new Message[] {msg1, msg2, msg3, msg4});
        scheduler.cancelGroup("group1");
        safeSleep(100);
        
        //Test. The order of sending must be msg1, msg3, msg4
        List<Message> arrSentMessages = ((ResourceSchedulerImpl)scheduler).getSentMessages();
        assertEquals(arrSentMessages.size(), 3);
        assertEquals(arrSentMessages.get(0), msg1);//msg1 was sent firstly
        assertEquals(arrSentMessages.get(1), msg3);//msg3 was sent next
        assertEquals(arrSentMessages.get(2), msg4);//msg4 was sent finally
    }
    
    @Test
    /**
     * receive 4 messages, cancel 1 group ==> only 3 messages were sent
     */
    public void testReceiveTermination() {
    	Resource rs1 = new ResourceImpl(1, scheduler);
        scheduler.registerResource(rs1);
        
        Message msg1 = new MessageImpl(1, "group2");
        Message msg2 = new MessageImpl(2, "group1");
        Message msg3 = new MessageImpl(3, "group2", true);//Termination
        Message msg4 = new MessageImpl(4, "group2");//will not be sent
        scheduler.receiveMessage(new Message[] {msg1, msg2, msg3, msg4});
        safeSleep(100);
        
        //Test. The order of sending must be msg1, msg3, msg2
        List<Message> arrSentMessages = ((ResourceSchedulerImpl)scheduler).getSentMessages();
        assertEquals(arrSentMessages.size(), 3);
        assertEquals(arrSentMessages.get(0), msg1);//msg1 was sent firstly
        assertEquals(arrSentMessages.get(1), msg3);//msg3 was sent next
        assertEquals(arrSentMessages.get(2), msg2);//msg2 was sent finally
    }
    
    private void safeSleep(long l) {
        try {
            Thread.sleep(l);
        } catch (InterruptedException e) {
            //ignore
        }
    }
}
