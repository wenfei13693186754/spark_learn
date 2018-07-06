package source.atomic;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 在java.util.concurrent.atomic包下有AtomicBoolean，AtomicInteger，AtomicLong，
 * AtomicReference等类，它们
 * 的基本特性就是在多线程环境下，执行这些类所包含的方法时候，是原子性的。即当某个线程进入方法，执行其中指令时，不会被
 * 其它线程打断，而别的线程会一直等待该方法执行完成，才由JVM从等待队列中选择另一个线程进入。
 * 
 * @author hadoop
 *
 */
public class AtomicBooleanTest implements Runnable {

	public static AtomicBoolean exits = new AtomicBoolean(false);

	private String name;

	public AtomicBooleanTest(String name) {
		this.name = name;
	}

	@Override
	public void run() {
		if (exits.compareAndSet(false, true)) {//Atomically sets the value to the given updated value if the current value == the expected value
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			System.out.println(name + ", step 1");

			System.out.println(name + ", step 2");

			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			System.out.println(name + ", step 3");
			exits.set(false);
		} else {
			System.out.println(name + ", step else");
		}
	}
	public static void main(String[] args) {
		Thread t1 = new Thread(new AtomicBooleanTest("张三"));
		Thread t2 = new Thread(new AtomicBooleanTest("李四"));
		t1.start();
		t2.start();
		/*
		 * 当程序启动后，线程t1先进入run方法的if判断中，exits.compareAndSet(false, true)是用来判断exits预期值是否和实际值一致，然后将实际值改为设定的值，
		 * 张三来了后，判断为true，并将实际值false改为true.然后等待1s，李四进来，判断实际值和预期值不一致返回false，所以进入else循环，输出step else.
		 * 当张三等待时间够了后继续往下走，输出对应信息。
		 * 
		 *  李四, step else
			张三, step 1
			张三, step 2
			张三, step 3
		 */
	}
}
