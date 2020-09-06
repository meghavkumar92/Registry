import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ServiceRegistry implements Watcher{

	private static final String REGISTRY_ZNODE= "/service_registry";
	private final ZooKeeper zooKeeper;
	private String currentZnode = null;
	private List<String> allServiceAddresses = null;
	
	public ServiceRegistry(ZooKeeper zooKeeper){
		this.zooKeeper = zooKeeper;
		createServiceRegistryZnode();
	}
	
	public void registerToCluster(String metadata) throws KeeperException, InterruptedException{
		this.currentZnode = zooKeeper.create(REGISTRY_ZNODE + "/n_",metadata.getBytes(),ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		System.out.println("Registered to service registry"+metadata);
	}
	
	public void registerForUpdates() {
		try {
			updateAddresses();
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public synchronized List<String> getAllServiceAddresses() throws KeeperException, InterruptedException{
		if(allServiceAddresses == null){
			updateAddresses();
		}
		return allServiceAddresses;
	}
	
	
	public void unregisterFromCluster() throws InterruptedException, KeeperException{
		boolean watch;
		if(currentZnode != null && zooKeeper.exists(currentZnode, watch=false) != null){
			int version;
			zooKeeper.delete(currentZnode, version= -1);
		}
	}
	
	
	
	private void createServiceRegistryZnode(){
		boolean watch;
		try {
			if(zooKeeper.exists(REGISTRY_ZNODE, watch=false)== null){
				zooKeeper.create(REGISTRY_ZNODE, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			}
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	
	
	
	private synchronized void updateAddresses() throws KeeperException, InterruptedException{		
		ServiceRegistry watcher;
		List<String> workerZnodes = zooKeeper.getChildren(REGISTRY_ZNODE, watcher=this);
		List<String> addresses = new ArrayList<String>(workerZnodes.size());
		System.out.println("workerZnodes: "+workerZnodes.size());
		
		for(String workerZnode: workerZnodes){
			String workerZnodeFullPath = REGISTRY_ZNODE+"/"+ workerZnode;
			boolean watch;
			System.out.println("workerZnodeFullPath: "+workerZnodeFullPath);
			Stat stat = zooKeeper.exists(workerZnodeFullPath, watch=false);
			if(stat == null){
				System.out.println("workerZnodeFullPath: "+workerZnodeFullPath);
				continue;
			}
			byte[] addressBytes = zooKeeper.getData(workerZnodeFullPath, watch=false, stat);
			String address = new String(addressBytes);			
			addresses.add(address);
			System.out.println("addresses:: "+addresses);
		}
		
		this.allServiceAddresses = Collections.unmodifiableList(addresses);
		System.out.println("The cluster addresses are : "+this.allServiceAddresses);
	}

	public void process(WatchedEvent event) {
		// TODO Auto-generated method stub
		try {
			updateAddresses();
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	

}
