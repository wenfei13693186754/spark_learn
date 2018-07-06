package source.InheritableThreadLocal;

/**
 * InheritableThreadLocal继承自ThreadLocal，并重写了其三个方法，为子线程提供从父线程哪里继承的值：
 * 当创建子线程的时候，子线程会接收所有可继承的局部变量的初始值，以获得父线程所具有的值。通常，子线程的值
 * 与父线程的值是一致的；但是，通过重写这个类的childValue方法，子线程的值可以作为父线程值的一个任意函数。
 * 当必须将变量（如用户ID和事物ID）中维护的没个线程属性自动传递给创建的所有子线程时，应尽可能的采用可继承
 * 的线程局部变量，而不是采用普通的线程局部变量。
 * 
 * 下边案例中，如果th是ThreadLocal类型的，那么最终结果是main = 123和MyThread = null
 * 可以看到MyThread线程无法拿到主线程set的值。
 * 
 * 但是如果将th改为InheritableThreadLocal类型，那么MyThread线程就可以拿到主线程set的值了。
 * @author hadoop
 *
 */
public class InheritableThreadLocalDemo {
	//public static ThreadLocal<Integer> th = new ThreadLocal<Integer>();
	public static ThreadLocal<Integer> th = new InheritableThreadLocal<Integer>();
	
	public static void main(String[] args) {
		th.set(new Integer(123));
		
		Thread t1 = new MyThread();
		t1.start();
		
		System.out.println("main = "+th.get());
	}
	
	static class MyThread extends Thread{
		@Override
		public void run(){
			System.out.println("MyThread = "+th.get());
		}
	}
}

