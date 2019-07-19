package com.ljl.lucene.demo.jdbc;


import java.io.File;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
 
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.wltea.analyzer.lucene.IKAnalyzer;

public class SearchLogic {

	private static Connection conn = null;
	private static Statement stmt = null;
	private static ResultSet rs = null;
	// 索引保存目录
	private String indexDir = "D:\\lucence\\index2";
	private static IndexSearcher searcher = null;
	//创建分词器
	private static Analyzer standardAnalyzer = new StandardAnalyzer();
	private static Analyzer iKAnalyzernAlyzer = new IKAnalyzer(true);
 
	/**
	 * 获取数据库数据
	 * @param queryStr 需要检索的关键字
	 * @return
	 * @throws Exception
	 */
	public List<UserInfo> getResult(String queryStr) throws Exception {
		List<UserInfo> result = null;
		conn = JdbcUtil.getConnection();
		if (conn == null) {
			throw new Exception("数据库连接失败！");
		}
		String sql = "select id, name ,age,sex from user";
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sql);
			// 给数据库创建索引,此处执行一次，不要每次运行都创建索引
			// 以后数据有更新可以后台调用更新索引
			this.createIndex(rs);
			TopDocs topDocs = this.search(queryStr);
			ScoreDoc[] scoreDocs = topDocs.scoreDocs;
			result = this.addHits2List(scoreDocs);
		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception("数据库查询sql出错！ sql : " + sql);
		} finally {
			if (rs != null)
				rs.close();
			if (stmt != null)
				stmt.close();
			if (conn != null)
				conn.close();
		}
		return result;
	}
 
	/**
	 * 为数据库检索数据创建索引
	 * @param 访问数据库返回的ResultSet
	 * @throws Exception
	 */
	private void createIndex(ResultSet rs) throws Exception {
		// 创建或打开索引
		Directory directory = FSDirectory.open(new File(indexDir).toPath());
		// 创建IndexWriter
		IndexWriterConfig conf = new IndexWriterConfig(standardAnalyzer);
		IndexWriter indexWriter = new IndexWriter(directory, conf);
		// 遍历ResultSet创建索引
		while (rs.next()) {
			// 创建document并添加field
			Document doc = new Document();
			doc.add(new TextField("id", rs.getString("id"), Field.Store.YES));
			doc.add(new TextField("name", rs.getString("name"),Field.Store.YES));
			// 将doc添加到索引中
			indexWriter.addDocument(doc);
		}
		indexWriter.commit();
		indexWriter.close();
		directory.close();
	}
 
	/**
	 * 检索索引
	 * @param queryStr 需要检索的关键字
	 * @return
	 * @throws Exception
	 */
	private TopDocs search(String queryStr) throws Exception {
		//创建或打开索引目录
		Directory directory = FSDirectory.open(new File(indexDir).toPath());
		
		DirectoryReader ireader = DirectoryReader.open(directory);
		if (searcher == null) {
			searcher = new IndexSearcher(ireader);
		}
		//使用查询解析器创建Query
		QueryParser parser = new QueryParser("name",standardAnalyzer);
		Query query = parser.parse(queryStr);
		//Query query = new TermQuery(new Term("name", queryStr));
		//从索引中搜索得到排名前10的文档
		TopDocs topDocs = searcher.search(query, 10);
		return topDocs;
	}
 
	/**
	 * 将检索结果添加到List中
	 * @param scoreDocs
	 * @return
	 * @throws Exception
	 */
	private List<UserInfo> addHits2List(ScoreDoc[] scoreDocs) throws Exception {
		List<UserInfo> listBean = new ArrayList<UserInfo>();
		UserInfo bean = null;
		for (int i = 0; i < scoreDocs.length; i++) {
			int docId = scoreDocs[i].doc;
			Document doc = searcher.doc(docId);
			bean = new UserInfo();
			bean.setId(Integer.valueOf(doc.get("id")));
			bean.setName(doc.get("name"));
			listBean.add(bean);
		}
		return listBean;
	}
 
	public static void main(String[] args) {
		SearchLogic logic = new SearchLogic();
		try {
			Long startTime = System.currentTimeMillis();
			List<UserInfo> result = logic.getResult("na1");
			int i = 0;
			for (UserInfo bean : result) {
				if (i == 10)
					break;
				System.out.println("bean.name " + bean.getClass().getName()
						+ " : bean.id " + bean.getId() + " : bean.username "
						+ bean.getName());
				i++;
			}
 
			System.out.println("searchBean.result.size : " + result.size());
			Long endTime = System.currentTimeMillis();
			System.out.println("查询所花费的时间为：" + (endTime - startTime) / 1000);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println(e.getMessage());
		}
	}
}
