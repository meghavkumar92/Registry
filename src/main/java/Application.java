import java.io.IOException;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event;


public class Application implements Watcher {
	private ZooKeeper zooKeeper;
	private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
	private static final int SESSION_TIMEOUT = 3000;
	private static final int DEFAULT_PORT = 8080;
	
	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
		// TODO Auto-generated method stub
		System.out.println("Hallo");
		int currentServerPort = args.length == 1 ? Integer.parseInt(args[0]) : DEFAULT_PORT;
		
		Application application = new Application();
		ZooKeeper zooKeeper = application.connectToZooKeeper();
		
		ServiceRegistry serviceRegistry = new ServiceRegistry(zooKeeper);
		
		OnElectionAction onElectionAction = new OnElectionAction(serviceRegistry, currentServerPort);
		
		LeaderElection leaderElection = new LeaderElection(zooKeeper, onElectionAction);
		leaderElection.volunteerforLeardership();
		leaderElection.reelectLeader();
		
		application.run();
		application.close();
		System.out.println("Disconnected from ZooKeeper, exiting application");
		
	}
	public ZooKeeper connectToZooKeeper() throws IOException{		
		Application Watcher;
		 this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS,SESSION_TIMEOUT,Watcher= this);
		 return zooKeeper;
	}
	public void run() throws InterruptedException{
		synchronized (zooKeeper) {
			System.out.println("run method");
			zooKeeper.wait();
		}
	}
	public void close() throws InterruptedException{
		zooKeeper.close();
	}
	public void process(WatchedEvent event) {
		// TODO Auto-generated method stub
		System.out.println("Type: "+event.getType()+" state: "+event.getState());
		switch(event.getType()){
		case None:
			if(event.getState() == Event.KeeperState.SyncConnected){
				System.out.println("Successfully connected to ZooKeeper server.");
			} else{
				synchronized (zooKeeper) {
					System.out.println("Disconnected from ZooKeeper event");
					zooKeeper.notifyAll();
				}
			}
		}
	}
}
