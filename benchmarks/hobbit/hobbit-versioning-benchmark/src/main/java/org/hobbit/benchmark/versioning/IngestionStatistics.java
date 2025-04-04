/**
 * 
 */
package org.hobbit.benchmark.versioning;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.hobbit.benchmark.versioning.components.VersioningEvaluationModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author papv
 *
 */
public class IngestionStatistics {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(VersioningEvaluationModule.class);

	// a map containing the version number as key and an arraylist with the number 
	// of total added, deleted and total triples at indexes 0, 1 and 2 respectively.
	private HashMap<Integer,int[]> versionStats = new HashMap<Integer,int[]>(); // anti gia integer arraylist me added, deleted, total
	// the total number of each version's changes with respect to the previous one
	private HashMap<Integer,Long> loadingTimes = new HashMap<Integer,Long>();
	
	private AtomicLong runsCount;
	private AtomicLong failuresCount;
	
	private boolean changesComputed = false;
	private float avgChangesPS = 0;
	
	public IngestionStatistics() {
		runsCount = new AtomicLong(0);
		failuresCount = new AtomicLong(0);
	}
		
	public synchronized void reportSuccess(int version, int triplesToBeAdded, int triplesToBeDeleted, int triplesToBeLoaded, long currentLoadingTimeMs) {
		loadingTimes.put(version, currentLoadingTimeMs);
		versionStats.put(version, new int[] { triplesToBeAdded, triplesToBeDeleted, triplesToBeLoaded });
	}
	
	public void reportFailure() {
		failuresCount.incrementAndGet();
	}
	
	public float getInitialVersionIngestionSpeed() {
		if(loadingTimes.containsKey(0) && versionStats.containsKey(0)) {
			long initVersionLoadingTimeMS = loadingTimes.get(0);
			int initVersionTriples = versionStats.get(0)[2];
			// result should be returned in seconds
			return  initVersionTriples / (initVersionLoadingTimeMS / 1000f);
		} else {
			return 0f;
		}
	}
	
	private void computeChanges() {
		for(int i = 1; i < versionStats.size(); i++) {
			int changedTriples = getVersionAddedTriples(i) + getVersionDeletedTriples(i);
			float loadingTime = loadingTimes.get(i) / 1000f;
			avgChangesPS += changedTriples / loadingTime;
		}
		changesComputed = true;
	}
	
	public double getAvgChangesPS() {
		if(!changesComputed) {
			computeChanges();
		}
		return avgChangesPS / (loadingTimes.size() - 1);
	}
	
	public int getVersionAddedTriples(int version) {
		return versionStats.get(version)[0];
	}
	
	
	public int getVersionDeletedTriples(int version) {
		return versionStats.get(version)[1];
	}
	
	public int getVersionTriples(int version) {
		return versionStats.get(version)[2];
	}

	
	public long getRunsCount() {
		return runsCount.get();
	}
	
	public long getFailuresCount() {
		return failuresCount.get();
	}	
}
