/**
 *-------------------------------------------------------------------------
 * Copyright 2017 (C) by Thales Alenia Space France - all rights reserved
 *-------------------------------------------------------------------------
 */
package com.test;

import java.lang.reflect.InvocationTargetException;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.springframework.jms.support.JmsUtils;
import org.springframework.remoting.support.RemoteInvocation;
import org.springframework.remoting.support.RemoteInvocationResult;


/**
 * The Class JmsInvokerServiceExporter.
 *
 * @author T0130672
 */
public class JmsInvokerServiceExporter
  extends org.springframework.jms.remoting.JmsInvokerServiceExporter
{

  /** The remote proxy. */
  private Object proxy;
  
  /*
   * (non-Javadoc)
   * @see org.springframework.jms.remoting.JmsInvokerServiceExporter#afterPropertiesSet()
   */
  @Override
  public void afterPropertiesSet()
  {
    this.proxy = getProxyForService();
  }

  /**
   * Callback for processing a received JMS message.
   * <p>
   * Implementors are supposed to process the given Message, typically sending reply messages
   * through the given Session.
   *
   * @param requestMessage
   *          the request message
   * @param session
   *          the underlying JMS Session (never {@code null})
   * @throws JMSException
   *           if thrown by JMS methods
   */
  @Override
  public void onMessage(Message requestMessage, Session session) throws JMSException
  {
    RemoteInvocation invocation = readRemoteInvocation(requestMessage);
    if (invocation != null)
    {
      RemoteInvocationResult result = invokeAndCreateResult(invocation, this.proxy);
      writeRemoteInvocationResult(requestMessage, session, result);
    }
  }
  
  @Override
  protected void writeRemoteInvocationResult(Message requestMessage, Session session, RemoteInvocationResult result) throws JMSException
  {
    Message response = createResponseMessage(requestMessage, session, result);
    if (requestMessage.getStringProperty("responseID") != null && !requestMessage.getStringProperty("responseID").equals("")) {
    	response.setStringProperty("responseID", requestMessage.getStringProperty("responseID"));
    }
    MessageProducer producer = session.createProducer(requestMessage.getJMSReplyTo());
    try
    {
      producer.send(response);
    }
    finally
    {
      JmsUtils.closeMessageProducer(producer);
    }
  }

}
