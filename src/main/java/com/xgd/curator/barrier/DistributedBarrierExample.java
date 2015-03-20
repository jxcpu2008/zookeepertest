package com.xgd.curator.barrier;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.curator.utils.CloseableUtils;

public class DistributedBarrierExample {

	private static final int QTY = 5;
	private static final String PATH = "/example/barrier";
	
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		TestingServer server = new TestingServer();
		CuratorFramework client = null;
		
		try {
			client = CuratorFrameworkFactory.newClient(server.getConnectString(), new ExponentialBackoffRetry(1000, 3));
			client.start();
			
			// 创建controlBarrier来设置栅栏
			// 这步很关键，如果开始不设置栅栏，所有的线程就不会阻塞，即使线程调用waitOnBarrier()方法
			DistributedBarrier controlBarrier = new DistributedBarrier(client, PATH);
			controlBarrier.setBarrier();
			
			ExecutorService service = Executors.newFixedThreadPool(QTY);
			for (int i = 0; i < QTY; ++i) {
				final DistributedBarrier barrier = new DistributedBarrier(client, PATH);
				final int index = i;
				
				Callable<Void> task = new Callable<Void>() {
					@Override
					public Void call() throws Exception {
						// TODO Auto-generated method stub
						Thread.sleep((long) (3 * Math.random()));
						System.out.println("Client #" + index + " waits on Barrier");
						// 线程调用waitOnBarrier()方法，线程阻塞，直到栅栏被移除
						barrier.waitOnBarrier();
						System.out.println("Client #" + index + " begins");
						return null;
					}
				};
				
				service.submit(task);
			}
			
			Thread.sleep(10000);
			System.out.println("all Barrier instances should wait the condition");
			// controlBarrier移除栅栏，这样所有阻塞的线程才继续执行
			controlBarrier.removeBarrier();
			service.shutdown();
			service.awaitTermination(10, TimeUnit.MINUTES);
			
		} finally {
			CloseableUtils.closeQuietly(client);
			CloseableUtils.closeQuietly(server);
		}
	}
}