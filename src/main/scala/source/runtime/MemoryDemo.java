package source.runtime;

/**
 * java.lang.Runtime类的使用
 * java.lang.Runtime类封装了运行时环境，每个java应用程序都有一个Runtime类实例，使应用程序可以和运行环境进行交互
 * 一般不能实例化一个Runtime对象，应用程序也不能自己创建Runtime实例，但是可以通过getRuntime方法获得当前Runtime运行时对象的引用
 * 一旦得到了Runtime的引用，就可以调用Runtime的方法取控制java虚拟机的状态和行为
 * 当Applet和其它不被信任的代码调用任何Runtime方法时，常常会引起SecurityException。
 * 
 * 程序最终的运行结果是：
 * 	total memory is 124256256
	free memory is 122274016
	free memory after gc is 123501160
	free memory after allocate is 122179808
	memory used by allocate is 1321352
	free memory after collecting discard integers is 123502160
 * @author hadoop
 *
 */
public class MemoryDemo {
	
	public static void main(String[] args) {
		Runtime rt = Runtime.getRuntime();
		
		long mem1, mem2;
		Integer somenits[] = new Integer[1000];
		System.out.println("total memory is "+rt.totalMemory());

		mem1 = rt.freeMemory();
		System.out.println("free memory is " + mem1);
		rt.gc();
		mem1 = rt.freeMemory();
		System.out.println("free memory after gc is " + mem1);

		// allocate integers
		for (int i = 0; i < 1000; i++)
			somenits[i] = new Integer(i);
		mem2 = rt.freeMemory();
		System.out.println("free memory after allocate is " + mem2);
		System.out.println("memory used by allocate is " + (mem1 - mem2));
		
		//discard Integer
		for(int i = 0; i<1000;i++)somenits[i] = null;
		rt.gc();
		mem2 = rt.freeMemory();
		System.out.println("free memory after collecting discard integers is "+ mem2);
	}
}
