/**
 * 
 */
package org.hobbit.benchmark.versioning;

import java.io.Serializable;
import java.nio.ByteBuffer;

import org.hobbit.core.rabbit.RabbitMQUtils;

/**
 * @author papv
 *
 */
@SuppressWarnings("serial")
public class Task implements Serializable {
	
	private String taskId;
	private String query;
	private int queryType;
	private int querySubType;
	private int querySubstitutionParam;
	private byte[] expectedAnswers;
	
	public Task(int queryType, int querySubType, int querySubParam, String id, String query, byte[] expectedAnswers) {
		this.queryType = queryType;
		this.querySubType = querySubType;
		this.querySubstitutionParam = querySubParam;
		this.taskId = id;
		this.query = query;
		this.expectedAnswers = expectedAnswers;
	}
	
	public Task(String taskId) {
		this.taskId = taskId;
	}

	public void setId(String id) {
		this.taskId = id;
	}
	
	public String getTaskId() {
		return this.taskId;
	}
	
	public void setQuery(String query) {
		this.query = query;
	}
	
	public String getQuery() {
		return this.query;
	}
	
	public void setQueryType(int queryType) {
		this.queryType = queryType;
	}
	
	public int getQueryType() {
		return this.queryType;
	}
	
	public void setQuerySubstitutionParam(int querySubParam) {
		this.querySubstitutionParam = querySubParam;
	}
	
	public int getQuerySubstitutionParam() {
		return this.querySubstitutionParam;
	}
	
	public void setQuerySubType(int queryType) {
		this.querySubType = queryType;
	}
	
	public int getQuerySubType() {
		return this.querySubType;
	}
	
	// the results are preceded by the query type as this information required
	// by the evaluation module. 
	public void setExpectedAnswers(byte[] res) {
		ByteBuffer buffer = ByteBuffer.allocate(12);
		buffer.putInt(queryType);
		buffer.putInt(querySubType);
		buffer.putInt(querySubstitutionParam);
		this.expectedAnswers = RabbitMQUtils.writeByteArrays(buffer.array(), new byte[][]{res}, null);
	}
	
	public byte[] getExpectedAnswers() {
		return this.expectedAnswers;
	}
}
