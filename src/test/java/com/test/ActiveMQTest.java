package com.test;

import java.util.ArrayList;
import java.util.List;

import javax.jms.Destination;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.plugin.DiscardingDLQBrokerPlugin;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.jms.connection.SingleConnectionFactory;
import org.springframework.jms.listener.SimpleMessageListenerContainer;

public class ActiveMQTest {

	enum NobConf {
		ONE_DUPLEX, HALF_DUPLEX
	};

	private static BrokerService noBroker1;

	private static BrokerService noBroker2;

	private static final int CALL_NUMBER = 200;

	private static final int SERVICE_CONSUMER_NUMBER = 20;

	@BeforeClass
	   public static void initLog() {
	       BasicConfigurator.configure();
	       Logger.getRootLogger().setLevel(Level.DEBUG);
	   } 
	
	public static BrokerPlugin[] getPlugins() {
		DiscardingDLQBrokerPlugin brokerPlugin = new DiscardingDLQBrokerPlugin();
		brokerPlugin.setDropAll(true);
		brokerPlugin.setDropTemporaryQueues(true);
		brokerPlugin.setDropTemporaryTopics(true);
		brokerPlugin.setReportInterval(0);
		return new BrokerPlugin[] { brokerPlugin };
	}

	public static void startNetworkOfBroker(NobConf conf) throws Exception {
		noBroker1 = new BrokerService();
		noBroker1.setPersistent(false);
		noBroker1.setPlugins(getPlugins());
		noBroker1.setBrokerName("broker1");
		noBroker1.setUseJmx(false);
		noBroker1.addConnector("tcp://0.0.0.0:50001?maximumConnections=1000&wireFormat.maxFrameSize=104857600");
		noBroker1.addNetworkConnector("static://(tcp://127.0.0.1:50002?connectionTimeout=2000&soTimeout=2000)");

		noBroker2 = new BrokerService();
		noBroker2.setPersistent(false);
		noBroker2.setPlugins(getPlugins());
		noBroker2.setBrokerName("broker2");
		noBroker2.setUseJmx(false);
		noBroker2.addConnector("tcp://0.0.0.0:50002?maximumConnections=1000&wireFormat.maxFrameSize=104857600");

		if (conf.equals(NobConf.ONE_DUPLEX)) {
			noBroker1.getNetworkConnectors().get(0).setDuplex(true);
			// This configuration seems causing problems and by the way you told us it's not recommended
			// noBroker2.addNetworkConnector("static://(tcp://127.0.0.1:50001?connectionTimeout=2000&soTimeout=2000)");
			// noBroker2.getNetworkConnectors().get(0).setDuplex(true);
		} else if (conf.equals(NobConf.HALF_DUPLEX)) {
			noBroker1.getNetworkConnectors().get(0).setDuplex(false);
			noBroker2.addNetworkConnector("static://(tcp://127.0.0.1:50001?connectionTimeout=2000&soTimeout=2000)");
			noBroker2.getNetworkConnectors().get(0).setDuplex(false);
		}

		noBroker1.start();
		noBroker2.start();
		Thread.sleep(3000);
	}

	public static void stopNetworkOfBroker() throws Exception {
		Thread.sleep(1000);
		noBroker1.stop();
		noBroker2.stop();
		Thread.sleep(2000);
	}

	public JmsInvokerServiceExporter exporterFactory(Destination destination, Object service, String url) {
		JmsInvokerServiceExporter lJmsInvokerServiceExporter = new JmsInvokerServiceExporter();
		lJmsInvokerServiceExporter.setService(service);
		lJmsInvokerServiceExporter.setServiceInterface(IMockService.class);

		ActiveMQConnectionFactory exporterConnectionFactory = new ActiveMQConnectionFactory(url + "?connectionTimeout=2000&soTimeout=2000&wireFormat.maxInactivityDuration=2000");
		exporterConnectionFactory.setTrustAllPackages(true);
		SingleConnectionFactory lExporterSingleConnectionFactory = new SingleConnectionFactory();
		lExporterSingleConnectionFactory.setTargetConnectionFactory(exporterConnectionFactory);
		lExporterSingleConnectionFactory.setReconnectOnException(true);
		lExporterSingleConnectionFactory.afterPropertiesSet();

		SimpleMessageListenerContainer lSimpleMessageListenerContainer = new SimpleMessageListenerContainer();
		lSimpleMessageListenerContainer.setConnectionFactory(lExporterSingleConnectionFactory);
		lSimpleMessageListenerContainer.setDestination(destination);
		lSimpleMessageListenerContainer.setMessageListener(lJmsInvokerServiceExporter);
		lSimpleMessageListenerContainer.afterPropertiesSet();
		lJmsInvokerServiceExporter.afterPropertiesSet();
		lSimpleMessageListenerContainer.start();
		return lJmsInvokerServiceExporter;
	}

	public JmsInvokerProxyFactoryBean proxyFactory(Destination destination, Destination ackDestination, String url) {
		JmsInvokerProxyFactoryBean lJmsInvokerProxyFactoryBean = new JmsInvokerProxyFactoryBean();
		lJmsInvokerProxyFactoryBean.setServiceInterface(IMockService.class);

		ActiveMQConnectionFactory proxyConnectionFactory = new ActiveMQConnectionFactory(url + "?connectionTimeout=2000&soTimeout=2000&wireFormat.maxInactivityDuration=2000");
		proxyConnectionFactory.setTrustAllPackages(true);
		SingleConnectionFactory lProxySingleConnectionFactory = new SingleConnectionFactory();
		lProxySingleConnectionFactory.setTargetConnectionFactory(proxyConnectionFactory);
		lProxySingleConnectionFactory.setReconnectOnException(true);
		lProxySingleConnectionFactory.afterPropertiesSet();

		lJmsInvokerProxyFactoryBean.setConnectionFactory(lProxySingleConnectionFactory);
		lJmsInvokerProxyFactoryBean.setDestination(destination);
		lJmsInvokerProxyFactoryBean.setAckDestination(ackDestination);
		lJmsInvokerProxyFactoryBean.setReceiveTimeout(10000);
		lJmsInvokerProxyFactoryBean.afterPropertiesSet();
		return lJmsInvokerProxyFactoryBean;
	}

	@Test
	public void testNobQueueUsingOneDuplex() throws Exception {
		startNetworkOfBroker(NobConf.ONE_DUPLEX);

		IMockService service = new IMockService() {
			@Override
			public void call(String message) throws MockServiceException {
				// Nothing
			}

			@Override
			public void call() throws MockServiceException {
				// Nothing
			}
		};
		JmsInvokerServiceExporter lJmsInvokerServiceExporter = exporterFactory(new ActiveMQQueue("TEST_QUEUE"), service, "tcp://127.0.0.1:50001");
		JmsInvokerProxyFactoryBean lJmsInvokerProxyFactoryBean = proxyFactory(new ActiveMQQueue("TEST_QUEUE"), new ActiveMQTopic("ACK"), "tcp://127.0.0.1:50002");
		IMockService serviceRemote = (IMockService) lJmsInvokerProxyFactoryBean.getObject();

		Caller caller = new Caller(serviceRemote, "Mean time on a NOB using queue");
		caller.run();
		
		if (caller.hasFailed()) {
			Assert.fail("Test has failed");
		}

		stopNetworkOfBroker();
	}

	@Test
	public void testNobTopicUsingOneDuplex() throws Exception {
		startNetworkOfBroker(NobConf.ONE_DUPLEX);

		IMockService service = new IMockService() {
			@Override
			public void call(String message) throws MockServiceException {
				// Nothing
			}

			@Override
			public void call() throws MockServiceException {
				// Nothing
			}
		};
		JmsInvokerServiceExporter lJmsInvokerServiceExporter = exporterFactory(new ActiveMQTopic("TEST_TOPIC"), service, "tcp://127.0.0.1:50001");
		JmsInvokerProxyFactoryBean lJmsInvokerProxyFactoryBean = proxyFactory(new ActiveMQTopic("TEST_TOPIC"), new ActiveMQTopic("ACK"), "tcp://127.0.0.1:50002");
		IMockService serviceRemote = (IMockService) lJmsInvokerProxyFactoryBean.getObject();

		Caller caller = new Caller(serviceRemote, "Mean time on a NOB using topic");
		caller.run();
		
		if (caller.hasFailed()) {
			Assert.fail("Test has failed");
		}

		stopNetworkOfBroker();
	}

	@Test
	public void testNobQueueUsingHalfDuplex() throws Exception {
		startNetworkOfBroker(NobConf.HALF_DUPLEX);

		IMockService service = new IMockService() {
			@Override
			public void call(String message) throws MockServiceException {
				// Nothing
			}

			@Override
			public void call() throws MockServiceException {
				// TODO Auto-generated method stub
				
			}
		};
		JmsInvokerServiceExporter lJmsInvokerServiceExporter = exporterFactory(new ActiveMQQueue("TEST_QUEUE"), service, "tcp://127.0.0.1:50001");
		JmsInvokerProxyFactoryBean lJmsInvokerProxyFactoryBean = proxyFactory(new ActiveMQQueue("TEST_QUEUE"), new ActiveMQTopic("ACK"), "tcp://127.0.0.1:50002");
		IMockService serviceRemote = (IMockService) lJmsInvokerProxyFactoryBean.getObject();

		Caller caller = new Caller(serviceRemote, "Mean time on a NOB using queue");
		caller.run();
		
		if (caller.hasFailed()) {
			Assert.fail("Test has failed");
		}

		stopNetworkOfBroker();
	}

	@Test
	public void testNobTopicUsingHalfDuplex() throws Exception {
		startNetworkOfBroker(NobConf.HALF_DUPLEX);

		IMockService service = new IMockService() {
			@Override
			public void call(String message) throws MockServiceException {
				// Nothing
			}

			@Override
			public void call() throws MockServiceException {
				// TODO Auto-generated method stub
				
			}
		};
		JmsInvokerServiceExporter lJmsInvokerServiceExporter = exporterFactory(new ActiveMQTopic("TEST_TOPIC"), service, "tcp://127.0.0.1:50001");
		JmsInvokerProxyFactoryBean lJmsInvokerProxyFactoryBean = proxyFactory(new ActiveMQTopic("TEST_TOPIC"), new ActiveMQTopic("ACK"), "tcp://127.0.0.1:50002");
		IMockService serviceRemote = (IMockService) lJmsInvokerProxyFactoryBean.getObject();

		Caller caller = new Caller(serviceRemote, "Mean time on a NOB using topic");
		caller.run();

		if (caller.hasFailed()) {
			Assert.fail("Test has failed");
		}
		
		stopNetworkOfBroker();
	}

	@Test
	public void testOnTopicMultiConsumerOnNobUsingDuplex() throws Exception {
		startNetworkOfBroker(NobConf.ONE_DUPLEX);

		IMockService service = new IMockService() {
			@Override
			public void call(String message) throws MockServiceException {
				// System.out.println("Coming from " + message);
			}

			@Override
			public void call() throws MockServiceException {
			}
		};
		JmsInvokerServiceExporter lJmsInvokerServiceExporter = exporterFactory(new ActiveMQTopic("TEST_TOPIC"), service, "tcp://127.0.0.1:50001");

		List<Caller> callers = new ArrayList<ActiveMQTest.Caller>();

		for (int i = 1; i <= SERVICE_CONSUMER_NUMBER; i++) {
			JmsInvokerProxyFactoryBean lJmsInvokerProxyFactoryBean = proxyFactory(new ActiveMQTopic("TEST_TOPIC"), new ActiveMQTopic("ACK"), "tcp://127.0.0.1:50002");
			IMockService serviceRemote = (IMockService) lJmsInvokerProxyFactoryBean.getObject();
			System.out.println("Starting consumer #" + i);
			Caller caller = new Caller(serviceRemote, "Multi consumer #" + i + " mean time on a nob using topic", "" + i);
			callers.add(caller);
			new Thread(caller).start();
		}

		boolean allFinished = false;
		while (!allFinished) {
			allFinished = true;
			for (Caller caller : callers) {
				allFinished = allFinished & caller.isFinished();
			}
			Thread.currentThread().sleep(100);
		}
		for (Caller caller : callers) {
			if (caller.hasFailed) {
				Assert.fail("At least one consumer failed");
			}
		}

		stopNetworkOfBroker();
	}

	class Caller implements Runnable {

		private IMockService serviceRemote;

		private String textToPrint;

		private String toCall;

		private boolean hasFinished = false;

		private boolean hasFailed = false;

		public Caller(IMockService service, String text) {
			serviceRemote = service;
			textToPrint = text;
		}

		public Caller(IMockService service, String text, String toCallParam) {
			serviceRemote = service;
			textToPrint = text;
			toCall = toCallParam;
		}

		@Override
		public void run() {
			long sum = 0;
			for (int i = 0; i < CALL_NUMBER; i++) {
				long start = System.currentTimeMillis();
				try {
					serviceRemote.call(toCall);
					System.out.println("===================");
					System.out.println("THIS ONE IS FINE");
					System.out.println("===================");
				} catch (MockServiceException e) {
					hasFailed = true;
					System.out.println(">>>>>>>>>>>>>>>>>>>>>");
					System.out.println("ERROR OCCURED : " + e.getMessage());
					System.out.println("<<<<<<<<<<<<<<<<<<<<<");
				}
				long end = System.currentTimeMillis();
				sum = sum + (end - start);
			}
			System.out.println("###########################################################################");
			System.out.println(textToPrint + " (ms):" + (sum / CALL_NUMBER));
			System.out.println("###########################################################################");
			hasFinished = true;
		}

		public boolean isFinished() {
			return hasFinished;
		}

		public boolean hasFailed() {
			return hasFailed;
		}

	}

}
