package ConcreteClasses;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import Interface.Logger;

public class RateLimitedLogger implements Logger {

	private Lock lock;
	private int tokenBucketCapacity;
	private int currentTokens;
	private ScheduledExecutorService executor;
	private int refillRate;
	private Condition hasTokens;
	private BlockingQueue<String> queue;
	private ScheduledExecutorService queuepoller;

	public RateLimitedLogger(int currentTokens, int tokenBucketCapacity) {
		// TODO Auto-generated constructor stub
		this.lock = new ReentrantLock(true);
		this.hasTokens = lock.newCondition();
		this.tokenBucketCapacity = tokenBucketCapacity;
		this.currentTokens = currentTokens;
		this.executor = Executors.newScheduledThreadPool(1);
		this.refillRate = 60 / tokenBucketCapacity;
		this.executor.scheduleAtFixedRate(() -> this.tokenRefill(), 0, refillRate, TimeUnit.SECONDS);
		this.queue = new ArrayBlockingQueue<>(5);
		this.queuepoller = Executors.newScheduledThreadPool(1);
		this.queuepoller.scheduleAtFixedRate(() -> {
			try {
				this.poll();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}, 0, 120, TimeUnit.SECONDS);
	}

	@Override
	public void push(String message) throws InterruptedException {
		// TODO Auto-generated method stub
		lock.lock();
		try {
			if (this.currentTokens <= 0) {
				if (this.queue.remainingCapacity() > 0) {
					System.out.println("Log producer going to put message in blocking queue + "
							+ Thread.currentThread().getName());
					this.queue.put(message);
				} else {
					System.out.println("Queue got full, Log Producer going into waiting mode + "
							+ Thread.currentThread().getName());
					hasTokens.await();
					System.out.println("Log Producer came outof waiting mode + " + Thread.currentThread().getName());
				}
			} else {
				this.currentTokens--;
				System.out.println("log producer - " + Thread.currentThread().getName() + " pushed message to logger!");
				this.storeLog(message);
			}
		} finally {
			lock.unlock();
		}
	}

	@Override
	public void poll() throws InterruptedException {
		// TODO Auto-generated method stub
		lock.lock();
		try {
			if (this.currentTokens > 0 && !queue.isEmpty()) {
				System.out.println(Thread.currentThread().getName() + " polled the message!");
				this.storeLog(queue.poll());
				this.currentTokens--;
			}
		} finally {
			System.out.println("yo");
			lock.unlock();
		}
	}

	@Override
	public void tokenRefill() {
		// TODO Auto-generated method stub
		lock.lock();

		try {
			if (this.currentTokens < this.tokenBucketCapacity) {
				this.currentTokens++;
				hasTokens.signal();
			}
		} finally {
			lock.unlock();
		}

	}

	@Override
	public void storeLog(String message) {
		// TODO Auto-generated method stub
		System.out.println(message);
	}

	public static void main(String[] args) {
		RateLimitedLogger logger = new RateLimitedLogger(5, 5);
		ExecutorService executor = Executors.newFixedThreadPool(5);
		for (int i = 0; i < 20; i++) {
			int message = i;
			executor.execute(() -> {
				try {
					logger.push("Message Number - " + message);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			});
		}
		
		// Gracefully shutdown executor
	    executor.shutdown();
	    try {
	        if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
	            executor.shutdownNow();
	        }
	    } catch (InterruptedException e) {
	        executor.shutdownNow();
	    }

	}
}
