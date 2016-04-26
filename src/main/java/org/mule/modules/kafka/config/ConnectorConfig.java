/**
 * (c) 2003-2016 MuleSoft, Inc. The software in this package is published under the terms of the Commercial Free Software license V.1 a copy of which has been included with this distribution in the LICENSE.md file.
 */
package org.mule.modules.kafka.config;

import java.io.IOException;
import java.util.*;

import org.mule.api.MuleContext;
import org.mule.api.annotations.components.Configuration;
import org.mule.api.annotations.display.Placement;
import org.mule.api.annotations.param.Optional;
import org.mule.api.annotations.Configurable;
import org.mule.api.annotations.Required;

import javax.inject.Inject;

@Configuration(friendlyName = "Configuration")
public class ConnectorConfig {

	@Configurable
	@Placement(tab = "Producer", group="Producer Settings", order=1)
	@Optional
	private String bootstrapServers;
	
	@Configurable
	@Placement(tab = "Producer", group="Producer Settings", order=2)
	@Optional
	private String producerExtendedProperties;
	
	@Configurable
	@Placement(tab = "Consumer Group", group="Consumer Group Settings", order=1)
	@Optional
    private String zookeeperUrl;
	
	@Configurable
	@Placement(tab = "Consumer Group", group="Consumer Group Settings", order=2)
	@Optional
	private String groupId;
	
	@Configurable
	@Placement(tab = "Consumer Group", group="Consumer Group Settings", order=3)
	@Optional
	private Map<String, String> consumerGroupExtendedProperties;
	
	@Configurable
	@Placement(tab="Simple Consumer", group="Simple Consumer Settings", order=1)
	@Optional
	private String brokerList;

	
	@Configurable
	@Placement(tab="Simple Consumer", group="Simple Consumer Settings", order=2)
	@Optional
	private String clientId;
	
	
	
	@Configurable
	@Placement(tab="Simple Consumer", group="Simple Consumer Settings", order=3)
	@Optional
	private int brokerPort;


	@Configurable
	@Placement(tab="Simple Consumer", group="Simple Consumer Settings", order=3)
	@Optional
	private Map<String, String> simpleConsumerExtendProperties;

	@Inject
	private MuleContext muleContext;
	
	public String getBrokerList() {
		return brokerList;
	}



	public void setBrokerList(String brokerList) {
		this.brokerList = brokerList;
	}


	public List<String> getParsedBrokerList() {
		return new ArrayList<String>(Arrays.asList(getBrokerList().split("\\s*,\\s*")));
	}
	
	public String getClientId() {
		return clientId;
	}



	public void setClientId(String clientId) {
		this.clientId = clientId;
	}



	public int getBrokerPort() {
		return brokerPort;
	}



	public void setBrokerPort(int port) {
		this.brokerPort = port;
	}



	public String getZookeeperUrl() {
		return zookeeperUrl;
	}



	public void setZookeeperUrl(String zookeeperUrl) {
		this.zookeeperUrl = zookeeperUrl;
	}



	public String getGroupId() {
		return groupId;
	}



	public void setGroupId(String groupId) {
		this.groupId = groupId;
	}

	public Properties getZookeeperProperties() {
		Properties props = null;
		
		if (getZookeeperUrl() != null && getGroupId() != null) {
			props = new Properties();
			
			if (getConsumerGroupExtendedProperties() != null)
				props.putAll(getConsumerGroupExtendedProperties());
			props.put("zookeeper.connect", getZookeeperUrl());
			props.put("group.id", getGroupId());
		}
		
		return props;
	}



	public Map<String, String> getConsumerGroupExtendedProperties() {
		return consumerGroupExtendedProperties;
	}



	public void setConsumerGroupExtendedProperties(
			Map<String, String> consumerGroupExtendedProperties) {
		this.consumerGroupExtendedProperties = consumerGroupExtendedProperties;
	}



	public Map<String, String> getSimpleConsumerExtendProperties() {
		return simpleConsumerExtendProperties;
	}



	public void setSimpleConsumerExtendProperties(
			HashMap<String, String> simpleConsumerExtendProperties) {
		this.simpleConsumerExtendProperties = simpleConsumerExtendProperties;
	}



	public String getBootstrapServers() {
		return bootstrapServers;
	}



	public void setBootstrapServers(String bootstrapServers) {
		this.bootstrapServers = bootstrapServers;
	}



	public String getProducerExtendedProperties() {
		return producerExtendedProperties;
	}



	public void setProducerExtendedProperties(
			String producerExtendedProperties) {
		this.producerExtendedProperties = producerExtendedProperties;
	}


	public Properties getProducerProperties() {
		Properties properties = new Properties();
		try {
			properties.load(muleContext.getExecutionClassLoader().getResourceAsStream(producerExtendedProperties));
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (properties.get("bootstrap.servers") == null)
			properties.put("bootstrap.servers", getBootstrapServers());
		
		return properties;
	}

	public MuleContext getMuleContext() {
		return muleContext;
	}

	public void setMuleContext(MuleContext muleContext) {
		this.muleContext = muleContext;
	}
}