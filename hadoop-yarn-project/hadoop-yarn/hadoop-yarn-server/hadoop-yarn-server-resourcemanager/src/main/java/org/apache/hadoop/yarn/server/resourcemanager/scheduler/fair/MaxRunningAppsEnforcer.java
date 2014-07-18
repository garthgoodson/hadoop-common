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
package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

/**
 * Handles tracking and enforcement for user and queue maxRunningApps
 * constraints
 */
public class MaxRunningAppsEnforcer {
  private static final Log LOG = LogFactory.getLog(FairScheduler.class);
  
  private final FairScheduler scheduler;

  // Tracks the number of running applications by user.
  private final Map<String, Integer> usersNumRunnableApps;
  @VisibleForTesting
  final ListMultimap<String, AppSchedulable> usersNonRunnableApps;

  public MaxRunningAppsEnforcer(FairScheduler scheduler) {
    this.scheduler = scheduler;
    this.usersNumRunnableApps = new HashMap<String, Integer>();
    this.usersNonRunnableApps = ArrayListMultimap.create();
  }

  /**
   * Checks whether making the application runnable would exceed any
   * maxRunningApps limits.
   */
  public boolean canAppBeRunnable(FSQueue queue, String user) {
    AllocationConfiguration allocConf = scheduler.getAllocationConfiguration();
    Integer userNumRunnable = usersNumRunnableApps.get(user);
    if (userNumRunnable == null) {
      userNumRunnable = 0;
    }
    if (userNumRunnable >= allocConf.getUserMaxApps(user)) {
      return false;
    }
    // Check queue and all parent queues
    // NATERO
    // NATERO
    // Interpret the queueMaxApps as a percentage of apps allowed to vcores, 50 -> 50%, assuming 20 vcores => maxApps = 10
    // NATERO
    // NATERO
    while (queue != null) {
      int queueMaxApps = allocConf.getQueueMaxApps(queue.getName());
      int calculatedMaxApps = calculateMaxApps(queueMaxApps);
      // calculatedMaxApps = queueMaxApps; //uncomment this to put everything back to normal

      LOG.info("EvAn eVaN EvAn: cur num runnable apps: "+queue.getNumRunnableApps());
      if (queue.getNumRunnableApps() >= calculatedMaxApps) {
        LOG.info("evan EVAN evan: App rejected, queue runnable app count:"+queue.getNumRunnableApps()+" >= "+calculatedMaxApps);
        return false;
      }
      queue = queue.getParent();
    }
    LOG.info("EVAN evan EVAN: App allowed!");
    return true;
  }

  /**
   * NATERO
   * NATERO
   * NATERO
   * Interpret the queueMaxApps as a percentage of apps allowed to vcores, 50 -> 50%, assuming 20 vcores => maxApps = 10
   * Helper function to automate calculating the number of max apps allowed from the percentage in allocation file
   */
  public int calculateMaxApps(int queueMaxApps){
    int clusterVcores = scheduler.getClusterCapacity().getVirtualCores();
    LOG.info("EVAN EVAN EVAN: Calculating max apps. cluster vcores: "+clusterVcores+", maxAppsPercent: "+queueMaxApps+"%");
    float maxAppsPercent = ((float)queueMaxApps) / 100.0f;
    int calculatedMaxApps = (int) (maxAppsPercent * clusterVcores);
    LOG.info("EVAN Evan eVan: calculated max apps: "+calculatedMaxApps);
    return calculatedMaxApps;
  }

  /**
   * Tracks the given new runnable app for purposes of maintaining max running
   * app limits.
   */
  public void trackRunnableApp(FSSchedulerApp app) {
    String user = app.getUser();
    FSLeafQueue queue = app.getQueue();
    // Increment running counts for all parent queues
    FSParentQueue parent = queue.getParent();
    while (parent != null) {
      parent.incrementRunnableApps();
      parent = parent.getParent();
    }

    Integer userNumRunnable = usersNumRunnableApps.get(user);
    usersNumRunnableApps.put(user, (userNumRunnable == null ? 0
        : userNumRunnable) + 1);
  }

  /**
   * Tracks the given new non runnable app so that it can be made runnable when
   * it would not violate max running app limits.
   */
  public void trackNonRunnableApp(FSSchedulerApp app) {
    String user = app.getUser();
    usersNonRunnableApps.put(user, app.getAppSchedulable());
  }

  /**
   * Checks to see whether any other applications runnable now that the given
   * application has been removed from the given queue.  And makes them so.
   * 
   * Runs in O(n log(n)) where n is the number of queues that are under the
   * highest queue that went from having no slack to having slack.
   */
  public void updateRunnabilityOnAppRemoval(FSSchedulerApp app, FSLeafQueue queue) {
    AllocationConfiguration allocConf = scheduler.getAllocationConfiguration();
    
    // childqueueX might have no pending apps itself, but if a queue higher up
    // in the hierarchy parentqueueY has a maxRunningApps set, an app completion
    // in childqueueX could allow an app in some other distant child of
    // parentqueueY to become runnable.
    // An app removal will only possibly allow another app to become runnable if
    // the queue was already at its max before the removal.
    // Thus we find the ancestor queue highest in the tree for which the app
    // that was at its maxRunningApps before the removal.
    
    //NATERO
    //NATERO
    //Interpret queueMaxApps as percentage again
    //NATERO
    int queueMaxApps = allocConf.getQueueMaxApps(queue.getName());
    int calculatedMaxApps = calculateMaxApps(queueMaxApps);
    //calculatedMaxApps = queueMaxApps; //Uncomment this to put things back to normal

    LOG.info("evaN Evan evaN: queue num runnable apps:"+queue.getNumRunnableApps()+", queue max apps:"+(calculatedMaxApps - 1));
    FSQueue highestQueueWithAppsNowRunnable = (queue.getNumRunnableApps() ==
        (calculatedMaxApps - 1)) ? queue : null;
    FSParentQueue parent = queue.getParent();
    while (parent != null) {
      //NATERO NATEOR NATERO
      calculatedMaxApps = calculateMaxApps(allocConf.getQueueMaxApps(parent.getName()));
      // calculatedMaxApp = allocConf.getQueueMaxApps(parent.getName());

      if (parent.getNumRunnableApps() == (calculatedMaxApps - 1)) {
        highestQueueWithAppsNowRunnable = parent;
      }
      parent = parent.getParent();
    }

    List<List<AppSchedulable>> appsNowMaybeRunnable =
        new ArrayList<List<AppSchedulable>>();

    // Compile lists of apps which may now be runnable
    // We gather lists instead of building a set of all non-runnable apps so
    // that this whole operation can be O(number of queues) instead of
    // O(number of apps)
    if (highestQueueWithAppsNowRunnable != null) {
      gatherPossiblyRunnableAppLists(highestQueueWithAppsNowRunnable,
          appsNowMaybeRunnable);
    }
    String user = app.getUser();
    Integer userNumRunning = usersNumRunnableApps.get(user);
    if (userNumRunning == null) {
      userNumRunning = 0;
    }
    if (userNumRunning == allocConf.getUserMaxApps(user) - 1) {
      List<AppSchedulable> userWaitingApps = usersNonRunnableApps.get(user);
      if (userWaitingApps != null) {
        appsNowMaybeRunnable.add(userWaitingApps);
      }
    }

    // Scan through and check whether this means that any apps are now runnable
    Iterator<FSSchedulerApp> iter = new MultiListStartTimeIterator(
        appsNowMaybeRunnable);
    FSSchedulerApp prev = null;
    List<AppSchedulable> noLongerPendingApps = new ArrayList<AppSchedulable>();
    while (iter.hasNext()) {
      FSSchedulerApp next = iter.next();
      if (next == prev) {
        continue;
      }
      
      if (canAppBeRunnable(next.getQueue(), next.getUser())) {
        trackRunnableApp(next);
        AppSchedulable appSched = next.getAppSchedulable();
        next.getQueue().getRunnableAppSchedulables().add(appSched);
        noLongerPendingApps.add(appSched);

        // No more than one app per list will be able to be made runnable, so
        // we can stop looking after we've found that many
        if (noLongerPendingApps.size() >= appsNowMaybeRunnable.size()) {
          break;
        }
      }

      prev = next;
    }
    
    // We remove the apps from their pending lists afterwards so that we don't
    // pull them out from under the iterator.  If they are not in these lists
    // in the first place, there is a bug.
    for (AppSchedulable appSched : noLongerPendingApps) {
      if (!appSched.getApp().getQueue().getNonRunnableAppSchedulables()
          .remove(appSched)) {
        LOG.error("Can't make app runnable that does not already exist in queue"
            + " as non-runnable: " + appSched + ". This should never happen.");
      }
      
      if (!usersNonRunnableApps.remove(appSched.getApp().getUser(), appSched)) {
        LOG.error("Waiting app " + appSched + " expected to be in "
        		+ "usersNonRunnableApps, but was not. This should never happen.");
      }
    }
  }
  
  /**
   * Updates the relevant tracking variables after a runnable app with the given
   * queue and user has been removed.
   */
  public void untrackRunnableApp(FSSchedulerApp app) {
    // Update usersRunnableApps
    String user = app.getUser();
    int newUserNumRunning = usersNumRunnableApps.get(user) - 1;
    if (newUserNumRunning == 0) {
      usersNumRunnableApps.remove(user);
    } else {
      usersNumRunnableApps.put(user, newUserNumRunning);
    }
    
    // Update runnable app bookkeeping for queues
    FSLeafQueue queue = app.getQueue();
    FSParentQueue parent = queue.getParent();
    while (parent != null) {
      parent.decrementRunnableApps();
      parent = parent.getParent();
    }
  }
  
  /**
   * Stops tracking the given non-runnable app
   */
  public void untrackNonRunnableApp(FSSchedulerApp app) {
    usersNonRunnableApps.remove(app.getUser(), app.getAppSchedulable());
  }

  /**
   * Traverses the queue hierarchy under the given queue to gather all lists
   * of non-runnable applications.
   */
  private void gatherPossiblyRunnableAppLists(FSQueue queue,
      List<List<AppSchedulable>> appLists) {

    //NATERO
    //NATERO
    //NATERO
    int queueMaxApps = scheduler.getAllocationConfiguration().getQueueMaxApps(queue.getName());
    int calculatedMaxApps = calculateMaxApps(queueMaxApps);
    //calculatedMaxApps = queueMaxApps; //Uncomment to put things back to normal

    if (queue.getNumRunnableApps() < calculatedMaxApps) {
      if (queue instanceof FSLeafQueue) {
        appLists.add(((FSLeafQueue)queue).getNonRunnableAppSchedulables());
      } else {
        for (FSQueue child : queue.getChildQueues()) {
          gatherPossiblyRunnableAppLists(child, appLists);
        }
      }
    }
  }

  /**
   * Takes a list of lists, each of which is ordered by start time, and returns
   * their elements in order of start time.
   * 
   * We maintain positions in each of the lists.  Each next() call advances
   * the position in one of the lists.  We maintain a heap that orders lists
   * by the start time of the app in the current position in that list.
   * This allows us to pick which list to advance in O(log(num lists)) instead
   * of O(num lists) time.
   */
  static class MultiListStartTimeIterator implements
      Iterator<FSSchedulerApp> {

    private List<AppSchedulable>[] appLists;
    private int[] curPositionsInAppLists;
    private PriorityQueue<IndexAndTime> appListsByCurStartTime;

    @SuppressWarnings("unchecked")
    public MultiListStartTimeIterator(List<List<AppSchedulable>> appListList) {
      appLists = appListList.toArray(new List[appListList.size()]);
      curPositionsInAppLists = new int[appLists.length];
      appListsByCurStartTime = new PriorityQueue<IndexAndTime>();
      for (int i = 0; i < appLists.length; i++) {
        long time = appLists[i].isEmpty() ? Long.MAX_VALUE : appLists[i].get(0)
            .getStartTime();
        appListsByCurStartTime.add(new IndexAndTime(i, time));
      }
    }

    @Override
    public boolean hasNext() {
      return !appListsByCurStartTime.isEmpty()
          && appListsByCurStartTime.peek().time != Long.MAX_VALUE;
    }

    @Override
    public FSSchedulerApp next() {
      IndexAndTime indexAndTime = appListsByCurStartTime.remove();
      int nextListIndex = indexAndTime.index;
      AppSchedulable next = appLists[nextListIndex]
          .get(curPositionsInAppLists[nextListIndex]);
      curPositionsInAppLists[nextListIndex]++;

      if (curPositionsInAppLists[nextListIndex] < appLists[nextListIndex].size()) {
        indexAndTime.time = appLists[nextListIndex]
            .get(curPositionsInAppLists[nextListIndex]).getStartTime();
      } else {
        indexAndTime.time = Long.MAX_VALUE;
      }
      appListsByCurStartTime.add(indexAndTime);

      return next.getApp();
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Remove not supported");
    }

    private static class IndexAndTime implements Comparable<IndexAndTime> {
      public int index;
      public long time;

      public IndexAndTime(int index, long time) {
        this.index = index;
        this.time = time;
      }

      @Override
      public int compareTo(IndexAndTime o) {
        return time < o.time ? -1 : (time > o.time ? 1 : 0);
      }

      @Override
      public boolean equals(Object o) {
        if (!(o instanceof IndexAndTime)) {
          return false;
        }
        IndexAndTime other = (IndexAndTime)o;
        return other.time == time;
      }

      @Override
      public int hashCode() {
        return (int)time;
      }
    }
  }
}
