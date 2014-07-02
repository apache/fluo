package accismus.impl.support;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;

public class CuratorCnxnListener implements ConnectionStateListener{

	private boolean isConnected;

	public synchronized boolean isConnected() {
		return isConnected;
	}

	@Override public void stateChanged(CuratorFramework curatorFramework, ConnectionState connectionState) {
		if(connectionState.equals(ConnectionState.CONNECTED) || connectionState.equals(ConnectionState.RECONNECTED))
			isConnected = true;

		else if(connectionState.equals(ConnectionState.LOST) || connectionState.equals(ConnectionState.SUSPENDED))
			isConnected = false;
	}
}
