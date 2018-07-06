package source.runtime;

import java.io.IOException;
import java.io.OutputStream;

/**
 * 在安全环境中，可以在多任务操作系统中使用java去执行其它特别大的进程，exec()方法有几种形式去命名想要运行的程序和它的输入参数。
 * exec方法返回一个Process对象，可以使用这个对象控制java程序与新运行进程的交互。
 * exec方法本质上依赖于运行环境。
 * @author hadoop
 *
 */
public class ExecDemo {

	public static void main(String[] args) {
		Runtime rt = Runtime.getRuntime();
		
		Process p = null;
		
		try {
			p = rt.exec("notepad");
			p.waitFor();
		} catch (Exception e) {
			System.out.println("execute notepad faild");
		}
		System.out.println("notepad returned "+p.exitValue());
	}
}
