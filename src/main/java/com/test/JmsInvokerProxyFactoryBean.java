/**
 *-------------------------------------------------------------------------
 * Copyright 2017 (C) by Thales Alenia Space France - all rights reserved
 *-------------------------------------------------------------------------
 */
package com.test;

import java.lang.reflect.Constructor;
import java.util.UUID;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;

import org.aopalliance.intercept.MethodInvocation;
import org.springframework.jms.connection.ConnectionFactoryUtils;
import org.springframework.jms.support.JmsUtils;
import org.springframework.remoting.support.RemoteInvocation;
import org.springframework.remoting.support.RemoteInvocationFactory;
import org.springframework.remoting.support.RemoteInvocationResult;

/**
 * @author T0130672
 */
public class JmsInvokerProxyFactoryBean extends org.springframework.jms.remoting.JmsInvokerProxyFactoryBean {

	private Destination mDestination;
	
	private Destination mAckDestination;

	/** default delivery mode */
	private static final int DEFAULT_DELIVERY_MODE = DeliveryMode.PERSISTENT;

	/** default time to live */
	private static final int DEFAULT_TTL = 0;

	/** default priority */
	private static final int DEFAULT_PRIORITY = 4;

	/** JMS message expiration flag */
	private boolean mJmsMessageExpiration = true;

	/** JMS message non persistent flag */
	private boolean mJmsMessageNonPersistent;

	@Override
	public void afterPropertiesSet() {
		super.afterPropertiesSet();
		if (mDestination == null) {
			throw new IllegalArgumentException("'destination' is required");
		}
		if (mAckDestination == null) {
			throw new IllegalArgumentException("'ackDestination' is required");
		}
	}

	protected Object getDestination() {
		return mDestination;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Object invoke(MethodInvocation pMethodInvocation) throws Throwable {
		// the Object to return
		Object lResult = null;

		try {
			lResult = super.invoke(pMethodInvocation);
		}
		// runtime exception are converted to an exception declared on the
		// invoked method (if possible)
		catch (RuntimeException lE)/// NOSONAR
		{
			Throwable lToThrow = lE;
			if (pMethodInvocation != null && pMethodInvocation.getMethod() != null) {
				// the exeptions classes present in methods from methods
				// invocation
				Class<?>[] lExceptionClasses = pMethodInvocation.getMethod().getExceptionTypes();
				if (lExceptionClasses.length > 0) {
					// rethrow a new exception
					Constructor<?> lConst = lExceptionClasses[0].getConstructor(String.class, Throwable.class);
					throw (Throwable) lConst.newInstance("A runtime exception occurred during the method invocation.",
							lE);
				}
			}
			// if no exception is handled by the methods, re-throw the exception
			throw lToThrow;
		}
		// return the result of super.invoke
		return lResult;
	}

	@Override
	protected RemoteInvocationResult executeRequest(RemoteInvocation invocation) throws JMSException {
		Connection con = createConnection();
		Session session = null;
		try {
			session = createSession(con);
			Destination destinationToUse = mDestination;
			Message requestMessage = createRequestMessage(session, invocation);
			con.start();
			Message responseMessage = doExecuteRequest(session, destinationToUse, requestMessage);
			if (responseMessage != null) {
				return extractInvocationResult(responseMessage);
			} else {
				return onReceiveTimeout(invocation);
			}
		} finally {
			JmsUtils.closeSession(session);
			ConnectionFactoryUtils.releaseConnection(con, getConnectionFactory(), true);
		}
	}


	/**
	 * Sets the JMS destination.
	 * 
	 * @param pDestination
	 *            the JMS destination to set.
	 */
	public void setDestination(Destination pDestination) {
		mDestination = pDestination;
		// Spring requires to have this configured !
		setQueueName("empty");
	}
	
	public void setAckDestination(Destination pDestination) {
		mAckDestination = pDestination;
	}

	protected Message doExecuteRequest(Session pSession, Destination pDestination, Message pRequestMessage)
			throws JMSException {
		MessageProducer lProducer = null;
		MessageConsumer lConsumer = null;
		try {
			// Create producer.
			String responseID = UUID.randomUUID().toString();
			lProducer = pSession.createProducer(pDestination);
			lConsumer = pSession.createConsumer(mAckDestination, "responseID='" + responseID + "'");
			pRequestMessage.setJMSReplyTo(mAckDestination);
			pRequestMessage.setStringProperty("responseID", responseID);
			if (mDestination instanceof Topic) {
				lProducer.send(pRequestMessage);
			} else if (mDestination instanceof Queue) {
				lProducer.send(pRequestMessage, getDeliveryMode(), DEFAULT_PRIORITY, getTTL());
			} else {
				throw new IllegalArgumentException("Either topic or queue as destination is supported");
			}
			
			long lTimeout = getReceiveTimeout();
			Message lMessage = null;
			// If timeout is defined
			if (lTimeout > 0) {
				lMessage = lConsumer.receive(lTimeout);
			} else {
				lMessage = lConsumer.receive();
			}
			return lMessage;
		} finally {
			// Close jms consumer and producer.
			JmsUtils.closeMessageConsumer(lConsumer);
			JmsUtils.closeMessageProducer(lProducer);
		}
	}

	private long getTTL() {
		long lResult = DEFAULT_TTL;
		if (mJmsMessageExpiration) {
			lResult = getReceiveTimeout();
		}
		return lResult;
	}

	/**
	 * Returns the delivery mode
	 * 
	 * @return the delivery mode
	 */
	private int getDeliveryMode() {
		int lResult = DEFAULT_DELIVERY_MODE;
		if (mJmsMessageNonPersistent) {
			lResult = DeliveryMode.NON_PERSISTENT;
		}
		return lResult;
	}

	 /**
	   * Sets the JMS message expiration flag
	   * 
	   * @param jmsMessageExpiration
	   *          the JMS message expiration flag
	   */
	  public void setJmsMessageExpiration(boolean jmsMessageExpiration)
	  {
	    this.mJmsMessageExpiration = jmsMessageExpiration;
	  }

	  /**
	   * Sets the JMS message non persistent flag
	   * 
	   * @param jmsMessageNonPersistent
	   *          the JMS message non persistent flag
	   */
	  public void setJmsMessageNonPersistent(boolean jmsMessageNonPersistent)
	  {
	    this.mJmsMessageNonPersistent = jmsMessageNonPersistent;
	  }
}
