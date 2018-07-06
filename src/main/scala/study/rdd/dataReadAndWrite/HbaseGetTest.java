package study.rdd.dataReadAndWrite;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 */
public class HbaseGetTest {
	public static final Logger LOGGER = LoggerFactory.getLogger(HbaseGetTest.class);
	public final static String HBASE_MASTER = "hbase.master";
	public final static String HBASE_ZOOKEEPER_PORT = "hbase.zookeeper.property.clientPort";
	public final static String HBASE_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";
	public static HTable htable;

	public static void main(String[] args) {
		String hbaseMaster = "10.17.139.121:60010";
		String zookeeperPort = "2181";
		String zookeeperQuorum = "10.17.139.121";
		String tableName = "testHbaseHdfsFileCopy";
		Configuration config = HBaseConfiguration.create();
		config.set(HBASE_MASTER, hbaseMaster);
		config.set(HBASE_ZOOKEEPER_PORT, zookeeperPort);
		config.set(HBASE_ZOOKEEPER_QUORUM, zookeeperQuorum);

		try {
			htable = new HTable(config, tableName);
		} catch (IOException e) {
			e.printStackTrace();
		}

		if (null == htable) {
			LOGGER.error("htable is null");
			System.exit(-1);
		}
		int i = 0;
		while (true) {
			String rowkey = "";
			if (i < 1000) {
				rowkey = "201609000" + i;
			} else if (i < 2000) {
				rowkey = "201609001" + i;
			} else if (i < 3000) {
				rowkey = "201609002" + i;
			} else if (i < 4000) {
				rowkey = "201609003" + i;
			} else if (i < 5000) {
				rowkey = "201609004" + i;
			} else if (i < 6000) {
				rowkey = "201609005" + i;
			} else if (i < 7000) {
				rowkey = "201609006" + i;
			} else if (i < 8000) {
				rowkey = "201609007" + i;
			} else if (i < 9000) {
				rowkey = "201609008" + i;
			} else if (i < 10000) {
				rowkey = "201609009" + i;
			} else if (i < 11000) {
				rowkey = "201609010" + i;
			}
			String family = "info";
			String column = "" + i;
			Result result = getDataFromTable(rowkey, family, column);
			try {
				LOGGER.info(new String(result.getValue(Bytes.toBytes(family), Bytes.toBytes(column)), "utf-8"));
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			i++;
			if (i == 11000) {
				i = 0;
			}
		}
	}

	/**
	 * @Description:往表中插入数据
	 * @param rowkey
	 * @param ts
	 * @param family
	 * @param column
	 * @param value
	 *            void:
	 */
	public static void putDataToTable(String rowkey, long ts, String family, String column, int value) {
		Put put = new Put(Bytes.toBytes(rowkey), ts);
		put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
		try {
			htable.put(put);
		} catch (RetriesExhaustedWithDetailsException e) {
			e.printStackTrace();
		} catch (InterruptedIOException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @Description:往表中插入数据
	 * @param rowkey
	 * @param ts
	 * @param family
	 * @param column
	 * @param value
	 *            void:
	 */
	public static void putDataToTable(String rowkey, long ts, String family, String column, String value) {
		Put put = new Put(Bytes.toBytes(rowkey), ts);
		put.add(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
		try {
			htable.put(put);
		} catch (RetriesExhaustedWithDetailsException e) {
			e.printStackTrace();
		} catch (InterruptedIOException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @Description:从表中查询数据
	 * @param rowkey
	 * @param family
	 * @param column
	 * @return Result:
	 */
	public static Result getDataFromTable(String rowkey, String family, String column) {
		Get get = new Get(Bytes.toBytes(rowkey));
		get.addColumn(Bytes.toBytes(family), Bytes.toBytes(column));
		Result dbresult = null;
		try {
			dbresult = htable.get(get);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return dbresult;
	}

	/**
	 * @Description:从表中查询数据
	 * @param rowkey
	 * @param family
	 * @param column
	 * @return Result:
	 */
	public static Result getDataFromTable(String rowkey, String family, String... columns) {
		Get get = new Get(Bytes.toBytes(rowkey));
		for (String colu : columns) {
			get.addColumn(Bytes.toBytes(family), Bytes.toBytes(colu));
		}
		Result dbresult = null;
		try {
			dbresult = htable.get(get);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return dbresult;
	}

	/**
	 * @Description:一次请求获取多行数据，它允许用户快速高效的从远处服务器获取相关的或者完全随机的多行数据
	 * 				get(List<T>)要么返回和给定列表大小相同的result列表，要么抛出异常
	 * @param rowkeys rowkey集合
	 * @param family
	 * @param column
	 * @return Result[]: Result 集合
	 */
	public static Result[] getDataFromTable(Iterator<String> rowkeys, String family, String... columns) {
		List<Get> gets = new ArrayList<Get>();
		Result[] results = null;
		try {
			while (rowkeys.hasNext()) {
				String rk = rowkeys.next();
				Get g1 = new Get(Bytes.toBytes(rk));
				for (String colu : columns) {
					g1.addColumn(Bytes.toBytes(family), Bytes.toBytes(colu));
				}
				gets.add(g1);
			}
			results = htable.get(gets);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return results;
	}

}
