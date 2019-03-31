package spark.exercise.utils;

import org.apache.spark.sql.streaming.StreamingQueryListener;

public class CustomStreamQueryListener extends StreamingQueryListener {

	@Override
	public void onQueryProgress(QueryProgressEvent qpe) {
		System.out.println("PROGRESS query: "+qpe.progress().json());
	}

	@Override
	public void onQueryStarted(QueryStartedEvent qse) {
		System.out.println("STARTED query: "+qse.id());
	}

	@Override
	public void onQueryTerminated(QueryTerminatedEvent qte) {
		System.out.println("TERMINATED query: "+ qte.id());

	}

}
