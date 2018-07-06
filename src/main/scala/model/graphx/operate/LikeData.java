package model.graphx.operate;

import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.HashSet;
import java.util.Set;

/**
 * 产生每个用户的喜好项目对应编号
 * @author Administrator
 *
 */
public class LikeData {
	public static void main(String[] args) throws Exception {
		FileOutputStream fs = new FileOutputStream(new File("E:\\friendsRec\\userLike.txt"));
		PrintStream p = new PrintStream(fs);
		Set<Integer> set = new HashSet<Integer>();
		for(int i=0;i<10000;i++){
			while(set.size()<4){
				int num = (int) (Math.random()*10);
				set.add(num);
			}
			String num = "|";
			for(Integer n : set){
				num+=n+" ";
			}
			p.println(i+num);
			set.clear();
		}
		p.close();
	}
}
