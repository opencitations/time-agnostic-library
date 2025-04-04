/**
 * 
 */
package org.hobbit.benchmark.versioning;

import java.util.concurrent.atomic.AtomicLong;

/**
 * @author papv
 *
 */
public class QueryTypeStatistics {
	private int queryType;
	private AtomicLong queryId;
	private AtomicLong runsCount;
	private AtomicLong failuresCount;
	
	private long minExecutionTimeMs = 0;
	private long maxExecutionTimeMs = 0;
	private long avgExecutionTimeMs = 0;

	public QueryTypeStatistics(int queryType) {
		this.queryType = queryType;
		queryId = new AtomicLong(0);
		runsCount = new AtomicLong(0);
		failuresCount = new AtomicLong(0);
	}
	
	public synchronized void reportSuccess(long currentExecutionTimeMs) {
		runsCount.incrementAndGet();
		minExecutionTimeMs = (minExecutionTimeMs == 0) ? currentExecutionTimeMs : (minExecutionTimeMs > currentExecutionTimeMs ? currentExecutionTimeMs : minExecutionTimeMs);
		maxExecutionTimeMs = (currentExecutionTimeMs > maxExecutionTimeMs) ? currentExecutionTimeMs : maxExecutionTimeMs;
		avgExecutionTimeMs += currentExecutionTimeMs;
	}
	
	public void reportFailure() {
		failuresCount.incrementAndGet();
	}
	
	public int getQueryType() {
		return queryType;
	}
	
	public long getRunsCount() {
		return runsCount.get();
	}
	
	public long getFailuresCount() {
		return failuresCount.get();
	}	
	
	public long getMinExecutionTimeMs() {
		return minExecutionTimeMs;
	}
	
	public long getMaxExecutionTimeMs() {
		return maxExecutionTimeMs;
	}
	
	public float getAvgExecutionTimeMs() {
		if (runsCount.get() == 0) {
			return 0;
		}
		return avgExecutionTimeMs / runsCount.floatValue();
	}
	
	public float getTotalExecutionTimeMs() {
		if (runsCount.get() == 0) {
			return 0;
		}
		return avgExecutionTimeMs;
	}
	
	public long getNewQueryId() {
		return queryId.getAndIncrement();
	}
	
	public AtomicLong getRunsCountAtomicLong() {
		return runsCount;
	}
}
