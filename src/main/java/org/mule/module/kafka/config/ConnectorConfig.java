package org.mule.module.kafka.config;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Properties;

import org.mule.api.annotations.components.Configuration;
import org.mule.api.annotations.display.Placement;
import org.mule.api.annotations.param.Optional;
import org.mule.api.annotations.Configurable;
import org.mule.api.annotations.Required;

@Configuration(friendlyName = "Configuration")
public class ConnectorConfig {

	@Configurable
	@Placement(tab = "Producer", group="Producer Settings", order=1)
	@Optional
	private String bootstrapServers;
	
	@Configurable
	@Placement(tab = "Producer", group="Producer Settings", order=2)
	@Optional
	private HashMap<String, String> producerExtendedProperties;
	
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
	private HashMap<String, String> consumerGroupExtendedProperties;
	
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
	private HashMap<String, String> simpleConsumerExtendProperties;
	
	public String getBrokerList() {
		return brokerList;
	}



	public void setBrokerList(String brokerList) {
		this.brokerList = brokerList;
	}


	public ArrayList<String> getParsedBrokerList() {
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



	public HashMap<String, String> getConsumerGroupExtendedProperties() {
		return consumerGroupExtendedProperties;
	}



	public void setConsumerGroupExtendedProperties(
			HashMap<String, String> consumerGroupExtendedProperties) {
		this.consumerGroupExtendedProperties = consumerGroupExtendedProperties;
	}



	public HashMap<String, String> getSimpleConsumerExtendProperties() {
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



	public HashMap<String, String> getProducerExtendedProperties() {
		return producerExtendedProperties;
	}



	public void setProducerExtendedProperties(
			HashMap<String, String> producerExtendedProperties) {
		this.producerExtendedProperties = producerExtendedProperties;
	}


	public Properties getProducerProperties() {
		if (getProducerExtendedProperties().get("bootstrap.servers") == null)
			getProducerExtendedProperties().put("bootstrap.servers", getBootstrapServers());
		
		Properties properties = new Properties();
		properties.putAll(getProducerExtendedProperties()); 
		
		return properties;
	}

}