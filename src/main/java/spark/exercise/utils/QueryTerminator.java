package spark.exercise.utils;

import org.apache.spark.sql.streaming.StreamingQuery;

public class QueryTerminator implements Runnable {

	private StreamingQuery[] queries;
		
	public QueryTerminator(StreamingQuery... queries) {
		super();
		this.queries = queries;
	}

	@Override
	public void run() {
		for(StreamingQuery sq : queries) {
			System.out.println("STOPPING query "+sq.id());
			sq.stop();
		}
	}

}
