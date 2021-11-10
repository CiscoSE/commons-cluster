package com.cisco.commons.cluster.controller;

import java.util.List;

public interface ClusterControllerInterface {

	public void joinCluster();
	
	public boolean isLeader();
	
	public List<ClusterMember> getClusterMembers() throws Exception;
	
	public void shutdown();
}
