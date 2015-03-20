package com.xgd.curator.leader;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;

import com.google.common.collect.Lists;

public class LeaderLatchExample {

	private static final int CLIENT_QTY = 10;
    private static final String PATH = "/examples/leader";
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		List<CuratorFramework> clients = Lists.newArrayList();
		List<LeaderLatch> examples = Lists.newArrayList();
		TestingServer server = new TestingServer();
		
		try {
			
			/**
			 * ？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？
			 * 这里有一点很让我不解，apache的Curator站点说：You only need one CuratorFramework object for each ZooKeeper cluster you are connecting to
			 * 还有Curator Framework页面里提到：IMPORTANT: CuratorFramework instances are fully thread-safe. You should share one CuratorFramework per ZooKeeper cluster in your application.
			 * 但是这里创建了CLIENT_QTY个CuratorFramework object for ZooKeeper cluster
			 * 这是怎么回事？为什么要创建这么多CuratorFramework object(创建了那么多CuratorFramework object也没用到)？
			 * 可不可以试一下只创建一个CuratorFramework object for ZooKeeper cluster？
			 */
			for (int i = 0; i < CLIENT_QTY; ++i) {
                CuratorFramework client = CuratorFrameworkFactory.newClient(server.getConnectString(), new ExponentialBackoffRetry(1000, 3));
                clients.add(client);
                LeaderLatch example = new LeaderLatch(client, PATH, "Client #" + i);
                examples.add(example);
                client.start();
                // 启动leader election
                example.start();
            }
			
			// leader election要费一些时间，所以主线程要等待一下
			Thread.sleep(20000);
			
			// 通过调用hasLeadership()方法进行轮询，看看是哪个LeaderLatch获得leadership
			LeaderLatch currentLeader = null;
            for (int i = 0; i < CLIENT_QTY; ++i) {
                LeaderLatch example = examples.get(i);
                if (example.hasLeadership()) {
                    currentLeader = example;
                }
            }
            
            System.out.println("current leader is " + currentLeader.getId());
            System.out.println("release the leader " + currentLeader.getId());
            // LeaderLatch一根筋到死， 除非调用close方法，否则它不会释放领导权
            currentLeader.close();
            examples.get(0).await(2, TimeUnit.SECONDS);
            System.out.println("Client #0 maybe is elected as the leader or not although it want to be");
            System.out.println("the new leader is " + examples.get(0).getLeader().getId());

            System.out.println("Press enter/return to quit\n");
            new BufferedReader(new InputStreamReader(System.in)).readLine();
            
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
            System.out.println("Shutting down...");
            for (LeaderLatch exampleClient : examples) {
                CloseableUtils.closeQuietly(exampleClient);
            }
            for (CuratorFramework client : clients) {
                CloseableUtils.closeQuietly(client);
            }
            CloseableUtils.closeQuietly(server);
        }
	}
}