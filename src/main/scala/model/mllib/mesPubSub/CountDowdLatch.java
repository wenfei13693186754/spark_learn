package model.mllib.mesPubSub;

import java.util.concurrent.CountDownLatch;

/**
 * 闭锁
 * java.util.concurrent.CountDownLatch是一个并发构造，它允许一个或多个线程等待一系列指定操作的完成
 * CountDownLatch以一个给定的数量初始化。countDown()每被调用一次，这一数量减一。
 * 通过调用await()方法之一，线程可以阻塞等待这一数量到达零
 * @author Administrator
 *
 */  
public class CountDowdLatch {
	public static void main(String[] args) {
		CountDownLatch latch = new CountDownLatch(3);
		Waiter waiter = new Waiter(latch);
		Decrementer d = new Decrementer(latch);
		
		new Thread(waiter).start();
		new Thread(d).start();
		
	}
}
class Waiter implements Runnable{
	
	CountDownLatch latch = null;
	
	public Waiter(CountDownLatch latch){
		this.latch = latch;
	}

	@Override
	public void run() {

		try {
			latch.await();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("Wwaiter released");
	}
	
}

class Decrementer implements Runnable{

	CountDownLatch latch = null;
	
	public Decrementer(CountDownLatch latch) {
		this.latch = latch;
	}
	@Override
	public void run() {
		try {
			Thread.sleep(1000);
			System.out.println("1");
			this.latch.countDown();
			Thread.sleep(1000);
			System.out.println("2");
			this.latch.countDown();
			Thread.sleep(1000);
			System.out.println("3");
			this.latch.countDown();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
