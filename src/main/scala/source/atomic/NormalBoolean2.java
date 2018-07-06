package source.atomic;

/**
 * AutomicBoolean多线程测试代码
 * 
 * @author hadoop
 *
 */
public class NormalBoolean2 implements Runnable {

	public static boolean exits = false;

	private String name;

	public NormalBoolean2(String name) {
		this.name = name;
	}

	@Override
	public void run() {
		if (!exits) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			exits = true;
			System.out.println(name + ", step 1");
			System.out.println(name + ", step 2");

			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}

			System.out.println(name + ", step 3");
			exits = false;
		} else {
			System.out.println(name + ", step else");
		}
	}

	public static void main(String[] args) {
		Thread t1 = new Thread(new NormalBoolean2("张三"));
		Thread t2 = new Thread(new NormalBoolean2("李四"));
		t1.start();
		t2.start();
		/*
		 * 这里存在线程安全问题，当张三进入if(!exits)判断后，等待了1s，还没来得及该exits状态的时候，
		 * 李四所在线程进来了，然后就出现了两个线程都执行下边代码的情况。输出结果如下：
		 *  李四, step 1
			张三, step 1
			张三, step 2
			李四, step 2
			张三, step 3
			李四, step 3
		 */
	}
}
