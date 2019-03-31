package spark.exercise.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Random;

public class TweetStreamProducer {

	public static class TweetsReader implements Runnable {

		private static Random RNG = new Random();
		
		private final Socket socket;
		private final String inputPath;
		
		public TweetsReader(Socket socket, String inputPath) {
			this.socket = socket;
			this.inputPath = inputPath;
		}
		
		@Override
		public void run() {
			
			System.out.println("happily serving client socket "+socket);
			
			try(BufferedWriter bw = 
					new BufferedWriter(
							new OutputStreamWriter(socket.getOutputStream()));
					
			) {
				File[] files = new File(inputPath).listFiles();
				
				if(files == null) {
					System.err.println("error: no files found for path "+inputPath);
					return;
				}
				
				for(File f : files) {
					try(BufferedReader br = Files.newBufferedReader(
											Paths.get(f.getAbsolutePath()),
												StandardCharsets.UTF_8)) {
						String line;
						while((line=br.readLine())!= null) {
							System.out.println(socket + " sending -> "+ line);
							bw.write(line);
							bw.newLine();
							bw.flush();
							try {
								//introduce random artificial pausing
								//of at most 250ms between sending tweets
								Thread.sleep(RNG.nextInt(250));
							} catch (InterruptedException e) {}
						}
					} catch(IOException exc) {
						System.err.println("error: "+exc.getMessage());
					}
				}
			} catch (IOException exc) {
				System.err.println("error: "+exc.getMessage());
			}
			
		}
		
	}
		
	public static void main(String[] args) {
		
		try(ServerSocket ss = new ServerSocket(9999)) {
			System.out.println("waiting for spark streaming app to connect...");
			while(true) {
				Thread tweetProducer = new Thread(
								new TweetsReader(ss.accept(),
										"data/input/lv/tweets/"));
				tweetProducer.start();
			}
		} catch (Exception exc) {
			exc.printStackTrace();
		}
		
	}

}
