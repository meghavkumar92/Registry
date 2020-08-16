import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.zookeeper.KeeperException;

public class OnElectionAction implements OnElectionCallBack{
	private final ServiceRegistry serviceRegistry;
	private final int port;
	
	public OnElectionAction(ServiceRegistry serviceRegistry, int port){
		this.serviceRegistry = serviceRegistry;
		this.port = port;
	}
	
	public void onElectedToBeLeader() {
		// TODO Auto-generated method stub
		System.out.println("onElectedToBeLeader method");
		try {
			serviceRegistry.unregisterFromCluster();
			System.out.println("after unregisterFromCluster method call ");
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (KeeperException e) {
			e.printStackTrace();
		}
		serviceRegistry.registerForUpdates();
		System.out.println("Exit onElectedToBeLeader method");
	}

	public void onWorker() {
		// TODO Auto-generated method stub
		try {
			String currentServerAddress = String.format("http://%s:%d",InetAddress.getLocalHost().getCanonicalHostName(),port);
			System.out.println("currentServerAddress: "+currentServerAddress);
			serviceRegistry.registerToCluster(currentServerAddress);
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (KeeperException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

}
