package model.graphx.operate;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;

/**
 * 
 * @author Administrator
 *
 */
public class CreateData {
	public static void main(String[] args) throws Exception {
		// 产生每个用户和好友之间的关系,用于协同过滤中数据的产生
		// createRelationBetweenFriends();
		// 产品1 2这样的边关系
		// createDataFor1To2();

		String path = "E:\\spark\\Spark-GraphX\\data\\recItemBasedCircleData3\\relInGood1";
		// 为图计算用到的attr文件生成测试数据
		// 分别产生person、circle、book类型数据
	    createAttrDataForGraphX(path, 100000);

		// 为图计算产生edges数据
		// person_1 person_2|100 0 0 0 0 0|2016-09-06 11:08:08
		// createEdgesDateForGraphX1(path, 500000);
		
		// 用来生成向hbase中写入的测试数据
		// 数据格式是：person_4 person person_5 person 1 0 0 0 0 0 0 2016-09-06
		// 11:08:08
		createEdgesDateForGraphX2();
		
		// 为图计算产生feat数据
		// person_1 1 1 1 1 1 1 1 1 1 1
		// createFeatDateForGraphX(path,10000);

		// 为图产生.circle文件
		// 123456|person_1 person_2....
		// createCircleDataForGraphX();

		// String path1 =
		// "E:\\spark\\Spark-GraphX\\data\\hashingOfGuavaTest\\hashingData";
		// createDataLikeX23asXds23(20, 10000000, path1,"item");
	}

	// 生成随机数字和字母,
	private static void createDataLikeX23asXds23(int length, int data, String path, String name) throws Exception {

		Random random = new Random();

		// 参数length，表示生成几位随机数
		FileOutputStream fs = new FileOutputStream(new File(path));
		PrintStream p = new PrintStream(fs);
		for (int i = 1; i <= data; i++) {
			String val = "";
			for (int j = 0; j < length; j++) {
				String charOrNum = random.nextInt(2) % 2 == 0 ? "char" : "num";
				// 输出字母还是数字
				if ("char".equalsIgnoreCase(charOrNum)) {
					// 输出是大写字母还是小写字母
					int temp = random.nextInt(2) % 2 == 0 ? 65 : 97;
					val += (char) (random.nextInt(26) + temp);
				} else if ("num".equalsIgnoreCase(charOrNum)) {
					val += String.valueOf(random.nextInt(10));
				}
			}

			p.println(val);
		}
		p.close();
	}

	private static void createDataFor1To2() throws FileNotFoundException {
		Random rand = new Random();
		FileOutputStream fs = new FileOutputStream(
				new File("E:\\spark\\Spark-GraphX\\data\\recItemBasedCircleAndSimUser\\1.txt"));
		PrintStream p = new PrintStream(fs);

		// 产生十个圈子，每个圈子里边有100个人
		for (int i = 0; i < 100; i++) {
			int l1 = rand.nextInt(1000);
			int l2 = rand.nextInt(1000);
			p.println(l1 + " " + l2);
		}
		p.close();
	}

	private static void createCircleDataForGraphX() throws FileNotFoundException {
		Random rand = new Random();
		FileOutputStream fs = new FileOutputStream(
				new File("E:\\spark\\Spark-GraphX\\data\\circleData\\userCircle.circle"));
		PrintStream p = new PrintStream(fs);

		// 产生十个圈子，每个圈子里边有100个人
		for (int i = 0; i < 10; i++) {
			int circleId = 0;
			while (circleId < 100000) {
				circleId = (int) (Math.random() * 1000000);
			}
			p.print(circleId + "|");
			for (int j = 1; j <= 10; j++) {
				p.print("person_" + rand.nextInt(30) + " ");
			}
			p.println();
		}
		p.close();
	}

	// 为图计算产生feat数据
	// person_1 1 1 1 1 1 1 1 1 1 1
	private static void createFeatDateForGraphX(String path, int data) throws FileNotFoundException {
		Random rand = new Random();
		FileOutputStream fs = new FileOutputStream(new File(path + ".feat"));
		PrintStream p = new PrintStream(fs);
		for (int i = 1; i <= data; i++) {
			int p1 = i;
			int f1 = rand.nextInt(2);
			int f2 = rand.nextInt(2);
			int f3 = rand.nextInt(2);
			int f4 = rand.nextInt(2);
			int f5 = rand.nextInt(2);
			int f6 = rand.nextInt(2);
			int f7 = rand.nextInt(2);
			int f8 = rand.nextInt(2);
			int f9 = rand.nextInt(2);
			int f10 = rand.nextInt(2);
			p.println("person_" + p1 + " " + f1 + " " + f2 + " " + f3 + " " + f4 + " " + f5 + " " + f6 + " " + f7 + " "
					+ f8 + " " + f9 + " " + f10);
		}

		for (int i = 1; i <= data; i++) {
			int p1 = i;
			int f1 = rand.nextInt(2);
			int f2 = rand.nextInt(2);
			int f3 = rand.nextInt(2);
			int f4 = rand.nextInt(2);
			int f5 = rand.nextInt(2);
			int f6 = rand.nextInt(2);
			int f7 = rand.nextInt(2);
			int f8 = rand.nextInt(2);
			int f9 = rand.nextInt(2);
			int f10 = rand.nextInt(2);
			p.println("circle_" + p1 + " " + f1 + " " + f2 + " " + f3 + " " + f4 + " " + f5 + " " + f6 + " " + f7 + " "
					+ f8 + " " + f9 + " " + f10);
		}

		for (int i = 1; i <= data; i++) {
			int p1 = i;
			int f1 = rand.nextInt(2);
			int f2 = rand.nextInt(2);
			int f3 = rand.nextInt(2);
			int f4 = rand.nextInt(2);
			int f5 = rand.nextInt(2);
			int f6 = rand.nextInt(2);
			int f7 = rand.nextInt(2);
			int f8 = rand.nextInt(2);
			int f9 = rand.nextInt(2);
			int f10 = rand.nextInt(2);
			p.println("book_" + p1 + " " + f1 + " " + f2 + " " + f3 + " " + f4 + " " + f5 + " " + f6 + " " + f7 + " "
					+ f8 + " " + f9 + " " + f10);
		}
		p.close();
	}

	// 为图计算产生edges数据
	// person_1 person_2|100 0 0 0 0 0|2016-09-06 11:08:08
	private static void createEdgesDateForGraphX1(String path, int data) throws FileNotFoundException {
		FileOutputStream fs = new FileOutputStream(new File(path + ".edges"));
		PrintStream p = new PrintStream(fs);
		for (int i = 0; i < data; i++) {
			int p1 = (int) (Math.random() * 10000);
			int p2 = (int) (Math.random() * 10000);
			int at1 = (int) (Math.random() * 10);
			int at2 = (int) (Math.random() * 10);
			int at3 = (int) (Math.random() * 10);
			int at4 = (int) (Math.random() * 10);
			int at5 = (int) (Math.random() * 10);
			int at6 = (int) (Math.random() * 100);
			if (p1 == p2) {
				i--;
				continue;
			}
			// 用来产生随机时间
			Date date = randomDate("2016-01-06 11:08:08", "2016-09-06 11:08:08");
			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
			String strDate = format.format(date);
			Random rand = new Random();
			int num = rand.nextInt(3);

			switch (num) {
			case 0:
				p.println("person_" + p1 + " " + "person_" + p2 + "|" + at1 + " " + at2 + " " + at3 + " " + at4 + " "
						+ at5 + " " + at6 + "|" + strDate);
			case 1:
				p.println("person_" + p1 + " " + "circle_" + p2 + "|" + at1 + " " + at2 + " " + at3 + " " + at4 + " "
						+ at5 + " " + at6 + "|" + strDate);
			default:
				p.println("person_" + p1 + " " + "book_" + p2 + "|" + at1 + " " + at2 + " " + at3 + " " + at4 + " "
						+ at5 + " " + at6 + "|" + strDate);
			}
		}
		p.close();
	}

	// 为图计算产生edges数据
	// person_4 person person_5 person 1 0 0 0 0 0 0 2016-09-06 11:08:08
	private static void createEdgesDateForGraphX2() throws FileNotFoundException {
		FileOutputStream fs = new FileOutputStream(
				new File("E:\\spark\\Spark-GraphX\\data\\writeToHbaseData\\relInGood2.edges"));
		PrintStream p = new PrintStream(fs);

		Random rand = new Random();

		for (int i = 1; i <= 100000; i++) {
			// 每个用户对应60个好友数据的生成
			Set<Integer> set = new HashSet<Integer>();
			while (set.size() < 60) {//好友id不能重复，所以使用set集合
				int fId = (int) (Math.random() * 100000) + 1;
				if(fId == i){
					continue;
				}
				set.add(fId);
			}
			for (int fId : set) {
				// 用来产生随机时间
				Date date = randomDate("2016-01-06 11:08:08", "2016-10-14 11:08:08");
				long time = date.getTime();
				p.println("person_" + i + " person" + " person_" + fId + " person " + 1 + " " + 1 + " " + 1 + " " + 1
						+ " " + 1 + " " + 1 + " " + 1 + " " + time);
			}
			
			/*
			 * 每个用户对应50个物品数据的生成 这50 个物品是随机的，包括圈子(共100000个，随机选择几个)，40种物品中的多个
			 * 
			 * 圈子id格式：circle_2 circle friends 10 beijing play 1 3 物品id格式：book1_1
			 * book java 10 河北 java编程思想 1 3
			 */
			for (int k = 0; k < 50; k++) {
				int num1 = (int) (Math.random() * 50);
				// 生成人和圈子的关系数据
				if (num1 == 0) {// person_2 person circle_2 circle 1 0 0 0 0 0 0 123124124123
					// 用来产生随机时间
					Date date = randomDate("2016-01-06 11:08:08", "2016-10-14 11:08:08");
					long time = date.getTime();
					int cId = (int) (Math.random() * 100000);
					p.println("person_" + i + " person " + "circle_" + cId + " circle " + 1 + " " + 1 + " " + 1 + " "
							+ 1 + " " + 1 + " " + 1 + " " + 1 + " " + time);
					// 生成人和book的关系数据
				} else {
					Date date = randomDate("2016-01-06 11:08:08", "2016-10-14 11:08:08");
					long time = date.getTime();
					int bId = (int) (Math.random() * 100000)+1;
					p.println("person_" + i + " person " + "book" + num1 + "_" + bId + " book"+ num1 + " " + 1 + " " + 1 + " " + 1
							+ " " + 1 + " " + 1 + " " + 1 + " " + 1 + " " + time);
				}
			}
		}
	}

	// 为图计算用到物品的attr文件生成测试数据
	private static void createAttrDataForGraphX(String path, int data) throws FileNotFoundException {
		FileOutputStream fs = new FileOutputStream(
				new File("E:\\spark\\Spark-GraphX\\data\\writeToHbaseData\\relInGoodItem9.attr"));
		PrintStream p = new PrintStream(fs);
		// int avgData = data/3;
		/*
		 for (int i = 1; i <= 10; i++) {//person的数据格式：person_1 person girl 25 beijing rose 1 3
			 p.println("person_" + i + " " + "person girl 25 beijing rose 1 3");
		 }*/
		 
		for (int i = 1; i <= 10; i++) { // circle_2 circle friends 10
											// beijing play 1 3
			p.println("circle_" + i + " " + "circle friends 10 beijing play 1 3");
		}
		for (int i = 1; i <= 49; i++) {// book1_1 book java 10 河北 java编程思想 1 3
			for (int j = 1; j <= 10; j++) {
				p.println("book" + i + "_" + j + " " + "book" + i + " java 10 河北 java编程思想 1 3");
			}
		}
		p.close();
	}

	// 产生每个用户和好友之间的关系,用于协同过滤中数据的产生
	private static void createRelationBetweenFriends() throws FileNotFoundException {
		FileOutputStream fs = new FileOutputStream(new File("E:\\Spark-MLlib\\data\\user6.txt"));
		PrintStream p = new PrintStream(fs);
		// p.println(100);
		// p.close();
		for (int i = 0; i < 10000; i++) {
			// for(int j=0;j<1;j++){
			// if(j!=i){
			double fdata = (Double) (Math.random() * 0.9);
			double num = (Double) (Math.random() * 0.9);
			double cz = (Double) (Math.random() * 0.9);
			double other = (Double) (Math.random() * 0.9);
			p.println(i + "|" + fdata + " " + num + " " + cz + " " + other);
			// }
			// }
		}
		p.close();
	}

	// ****************生成随机时间**************************************************************************
	private static Date randomDate(String beginDate, String endDate) {

		try {

			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");

			Date start = format.parse(beginDate);// 构造开始日期

			Date end = format.parse(endDate);// 构造结束日期

			// getTime()表示返回自 1970 年 1 月 1 日 00:00:00 GMT 以来此 Date 对象表示的毫秒数。

			if (start.getTime() >= end.getTime()) {

				return null;

			}

			long date = random(start.getTime(), end.getTime());

			return new Date(date);

		} catch (Exception e) {

			e.printStackTrace();

		}

		return null;

	}

	private static long random(long begin, long end) {

		long rtn = begin + (long) (Math.random() * (end - begin));

		// 如果返回的是开始时间和结束时间，则递归调用本函数查找随机值

		if (rtn == begin || rtn == end) {

			return random(begin, end);

		}

		return rtn;

	}
}
