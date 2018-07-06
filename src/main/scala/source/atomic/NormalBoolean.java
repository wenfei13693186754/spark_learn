package source.atomic;

/**
 * AtomicBoolean单线程测试代码
 * 
 * @author hadoop
 *
 */
public class NormalBoolean implements Runnable {

	public static boolean exits = false;

	private String name;

	public NormalBoolean(String name) {
		this.name = name;
	}

	@Override
	public void run() {
		if (!exits) {
			exits = true;
			System.out.println(name+", step 1");
			System.out.println(name+", step 2");
			System.out.println(name+", step 3");
			exits = false;
		}else{
			System.out.println(name+",step else");
		}
	}
	
	public static void main(String[] args) {
		new NormalBoolean("张三").run();
		/*
		 * 张三, step 1
		      张三, step 2
		      张三, step 3
		 */
	}

}
