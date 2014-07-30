/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * NATERO NATERO NATERO NATERO
 * Custom NodeInfo returned by decommission web api
 * NATERO NATERO NATERO NATERO
 */

package org.apache.hadoop.yarn.server.resourcemanager.webapp.dao;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;

import org.apache.hadoop.yarn.api.records.NodeId;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ResourceScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNodeReport;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSSchedulerNode;

@XmlRootElement(name = "node")
@XmlAccessorType(XmlAccessType.FIELD)
public class DecomNodeInfo {

  protected String rack;
  protected NodeState state;
  protected String id;
  protected String nodeHostName;
  protected String nodeHTTPAddress;
  protected int numContainers;
  protected long usedMemoryMB;
  protected long availMemoryMB;

  public DecomNodeInfo() {
  } // JAXB needs this

  public DecomNodeInfo(FSSchedulerNode fsNode, ResourceScheduler sched) {
    NodeId id = fsNode.getNodeID();
    SchedulerNodeReport report = sched.getNodeReport(id);
    this.numContainers = 0;
    this.usedMemoryMB = 0;
    this.availMemoryMB = 0;
    if (report != null) {
      this.numContainers = report.getNumContainers();
      this.usedMemoryMB = report.getUsedResource().getMemory();
      this.availMemoryMB = report.getAvailableResource().getMemory();
    }
    this.id = id.toString();
    this.rack = fsNode.getRackName();
    this.nodeHostName = fsNode.getNodeName();
    this.state = NodeState.DECOMMISSIONED;
    this.nodeHTTPAddress = fsNode.getHttpAddress();
  }

  public String getRack() {
    return this.rack;
  }

  public String getState() {
    return String.valueOf(this.state);
  }

  public String getNodeId() {
    return this.id;
  }

  public String getNodeHTTPAddress() {
    return this.nodeHTTPAddress;
  }
  
  public void setNodeHTTPAddress(String nodeHTTPAddress) {
    this.nodeHTTPAddress = nodeHTTPAddress;
  }

  public int getNumContainers() {
    return this.numContainers;
  }

  public long getUsedMemory() {
    return this.usedMemoryMB;
  }

  public long getAvailableMemory() {
    return this.availMemoryMB;
  }

}
