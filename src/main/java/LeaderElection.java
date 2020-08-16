import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event;
import org.apache.zookeeper.data.Stat;

public class LeaderElection implements Watcher {
	private static final String ELECTION_NAMESPACE = "/election";
	private String currentZnodeName;
	private final ZooKeeper zooKeeper;
	private final OnElectionCallBack onElectionCallBack;
	
	public LeaderElection(ZooKeeper zooKeeper,OnElectionCallBack onElectionCallBack){
		this.zooKeeper = zooKeeper;
		this.onElectionCallBack = onElectionCallBack;
	}
	public void volunteerforLeardership() throws KeeperException, InterruptedException{
		String znodePrefix = ELECTION_NAMESPACE+ "/c_";
		String znodeFullPath = zooKeeper.create(znodePrefix, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		System.out.println("znode name " + znodeFullPath);
		String replacement;
		String target;
		this.currentZnodeName = znodeFullPath.replace(target= ELECTION_NAMESPACE+ "/", replacement= "");
		
	}
	
	public void reelectLeader() throws KeeperException, InterruptedException{
		boolean watch;
		Stat predecessorStat = null;
		String predecessorZnodeName = "";
		while(predecessorStat == null){
			List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, watch=false);
			Collections.sort(children);
			String smallestChild = children.get(0);
			
			if(smallestChild.equals(currentZnodeName))
			{
				System.out.println("I am the leader: strating webservice");
				onElectionCallBack.onElectedToBeLeader();
				//String command = "C:\\Program Files\\Java\\jdk1.8.0_141\\bin\\java -jar F:\\Udemy\\DS\\demo\\target\\demo-0.0.1-SNAPSHOT.jar";
				//String command = "C:\\Program Files\\Java\\jdk1.8.0_141\\bin\\java -jar F:\\Programs\\DS\\workspace\\Spring_MongoDB\\Application\\target\\Application-0.0.2-SNAPSHOT.jar";
				String command = "F:\\Programs\\React\\dummyIOT\\my-app\\src\\runServer.bat";
				try
		        { 	 
		           // Running the above command 
		           Runtime run  = Runtime.getRuntime(); 
		           Process proc = run.exec(command);
		           InputStream stderr = proc.getErrorStream();
		           InputStreamReader isr = new InputStreamReader(stderr);
		           BufferedReader br = new BufferedReader(isr);
		           String line = null;
		           System.out.println("Starting Webservice...");
					
					  int exitVal = proc.waitFor(); 
					  if(exitVal==1) 
					  { System.exit(1);
					  	System.out.println("Process exitValue: " + exitVal); 
					  }
					  System.out.println("Process retured: " + exitVal);
					 
		           //return;
		           
		        }catch (Throwable t)
		          {
		            t.printStackTrace();
		          }
			}
			else{
				System.out.println("I am not the leader");
				int predecessorIndex = Collections.binarySearch(children,currentZnodeName) -1;
				predecessorZnodeName = children.get(predecessorIndex);
				//race condition - if the znode gets deleted and we call the exists() then we get a null so we wrap this logic in while loop.
				predecessorStat = zooKeeper.exists(ELECTION_NAMESPACE+ "/"+ predecessorZnodeName, this);
			}
		}
		
		onElectionCallBack.onWorker();
		System.out.println("Watching znode: "+ predecessorZnodeName);
		
	}
	public void process(WatchedEvent event) {
		// TODO Auto-generated method stub
		System.out.println("Type: "+event.getType()+" state: "+event.getState());
		switch(event.getType()){		
		case NodeDeleted:  
			try {
				reelectLeader();
			} catch (KeeperException e) {				
				e.printStackTrace();
			} catch (InterruptedException e) {				
				e.printStackTrace();
			}
		}
	}
}
