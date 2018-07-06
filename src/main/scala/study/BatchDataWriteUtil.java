package study;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by xyf on 2017/8/18.
 * 写入大量数据到指定文件中
 */

public class BatchDataWriteUtil {

    private static final Logger logger = LoggerFactory.getLogger(BatchDataWriteUtil.class);

    static final int START_NUM = 10;

    static final int END_NUM = 10;

    static  final String VERTEX_START_PRE = "Vertex";

    static  final String VERTEX_END_PRE = "End";

    public static void main(String[] args) {

        String filePath = "E:\\Jmeter-testPlan\\";
        String DataFile_Vertex = "VertexData20";
        String DataFile_Vertex_Cypher = "VertexDataCypher";
        String DataFile_Edge = "EdgeData1000000";
        String DataFile_Edge_Random = "EdgeDataRandom100";
        final int rows = 200000;
        //MakeCypherForVertx(filePath,DataFile_Vertex_Cypher,rows);
        MakeDateForVertx(filePath,DataFile_Vertex,rows);
        //MakeDataForEdge(filePath,DataFile_Edge,rows);
        //MakeDataForRandomEdge(filePath,DataFile_Edge_Random,rows);
    }


    /**
     * 创建批量随机的关系数据
     * @param filePath 文件路径
     * @param DataFile_Edge 文件名称
     * @param rows 数据行数
     */
    public static void MakeDataForRandomEdge(String filePath, String DataFile_Edge, int rows) {

        BufferedWriter fileOutputStream = null;
        List<String> randomList = new ArrayList<String>();

        for(int i = 0 ; i < START_NUM ; i ++){
            randomList.add("\"" + VERTEX_START_PRE + String.format("%09d", i) + "\"");
        }
        for(int i = 0 ; i < END_NUM ; i ++){
            randomList.add("\"" + VERTEX_END_PRE + String.format("%09d", i) + "\"");
        }
        try {
            String fullPath_Edge = filePath + File.separator + DataFile_Edge + ".csv";
            File fileVertex = new File(fullPath_Edge);
            /**
             * 开始顶点，关系，结束顶点数据文件
             */
            if (!fileVertex.getParentFile().exists()) {
                fileVertex.getParentFile().mkdirs();
            }

            if (fileVertex.exists()) {
                fileVertex.delete();
            }
            fileVertex = new File(fullPath_Edge);
            fileVertex.createNewFile();
            fileOutputStream = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileVertex), "UTF-8"), 1024);
            String field;
            String start;
            String end;
            int size = randomList.size();
            Random random = new Random();
            fileOutputStream.write("sid,tid");
            fileOutputStream.newLine();
            for (int i = 0; i <= rows; i++) {
                start = randomList.get(random.nextInt(size));
                end = randomList.get(random.nextInt(size));
                field =  start + "," + end;
                logger.error(field);
                fileOutputStream.write(field);
                fileOutputStream.newLine();
            }
            fileOutputStream.flush();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                fileOutputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 创建多条节点新增语句
     */
    public static void MakeCypherForVertx(String filePath, String DataFile_Vertex, int rows) {

        BufferedWriter fileOutputStream = null;
        String fullPath_Vertex = filePath + File.separator + DataFile_Vertex + ".csv";
        /**
         * 顶点文件
         */
        try {
            File fileVertex = new File(fullPath_Vertex);
            if (!fileVertex.getParentFile().exists()) {
                fileVertex.getParentFile().mkdirs();
            }
            if (fileVertex.exists()) {
                fileVertex.delete();
            }
            fileVertex = new File(fullPath_Vertex);
            fileVertex.createNewFile();
            fileOutputStream = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileVertex), "UTF-8"), 1024);
            StringBuilder stringBuilder = new StringBuilder();
            for (int i = 0; i <= rows; i++) {
                stringBuilder.append("Create (n:JmeterTest{id:'")
                        .append("Vertex" + String.format("%09d", i))
                        .append("',name:'")
                        .append("Name" + String.format("%09d", i))
                        .append(",Age:")
                        .append("Age" + String.format("%09d", i))
                        .append("})");
                fileOutputStream.write(stringBuilder.toString());
                fileOutputStream.newLine();
            }
            fileOutputStream.flush();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                fileOutputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 创建批量顶点数据
     * @param filePath 文件路径
     * @param DataFile_Vertex 文件名称
     * @param rows 数据行数
     */
    public static void MakeDateForVertx(String filePath, String DataFile_Vertex, int rows) {

        BufferedWriter fileOutputStream = null;
        String fullPath_Vertex = filePath + File.separator + DataFile_Vertex + ".csv";
        /**
         * 顶点文件
         */
        try {
            File fileVertex = new File(fullPath_Vertex);
            if (!fileVertex.getParentFile().exists()) {
                fileVertex.getParentFile().mkdirs();
            }
            if (fileVertex.exists()) {
                fileVertex.delete();
            }
            fileVertex = new File(fullPath_Vertex);
            fileVertex.createNewFile();
            fileOutputStream = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileVertex), "UTF-8"), 1024);
            String field;
            fileOutputStream.write("id");
            fileOutputStream.newLine();
            for (int i = 0; i < rows; i++) {
//                field = "\"End" + String.format("%09d", i) +
//                        "\",\"Name" +  String.format("%09d", i) +
//                        "\"," + i;
                field = ""+i;

                //logger.error(field);
                fileOutputStream.write(field);
                fileOutputStream.newLine();
            }
            fileOutputStream.flush();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                fileOutputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 创建大批量关系数据
     * @param filePath 文件路径
     * @param DataFile_Edge 文件名称
     * @param rows 数据行数
     */
    public static void MakeDataForEdge(String filePath, String DataFile_Edge, int rows) {

        BufferedWriter fileOutputStream = null;
        try {
            String fullPath_Edge = filePath + File.separator + DataFile_Edge + ".csv";
            File fileVertex = new File(fullPath_Edge);
            /**
             * 开始顶点，关系，结束顶点数据文件
             */
            if (!fileVertex.getParentFile().exists()) {
                fileVertex.getParentFile().mkdirs();
            }

            if (fileVertex.exists()) {
                fileVertex.delete();
            }
            fileVertex = new File(fullPath_Edge);
            fileVertex.createNewFile();
            fileOutputStream = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(fileVertex), "UTF-8"), 1024);
            String field;
            fileOutputStream.write("sid,tid");
            fileOutputStream.newLine();
            for (int i = 0; i <= rows; i++) {
                field = "\"Vertex" + String.format("%09d", i) +
                        "\",\"End" +  String.format("%09d", i) + "\"";

                logger.error(field);
                fileOutputStream.write(field);
                fileOutputStream.newLine();
            }
            fileOutputStream.flush();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                fileOutputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
