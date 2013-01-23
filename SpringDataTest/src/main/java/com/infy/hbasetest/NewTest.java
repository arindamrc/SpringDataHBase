package com.infy.hbasetest;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.RowMapper;
import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import com.infy.hbase.context.ApplicationContextHelper;
import com.infy.hbase.dao.HBaseDao;
import com.infy.hbase.dao.HBaseGenericDao;
import com.infy.hbase.dao.impl.HBaseDaoSpringDataImpl;
import com.infy.hbase.dao.support.SpringDataDaoSupport;
import com.infy.hbase.entity.AbstractHBaseEntity;
import com.infy.hbase.entity.Product;
import com.infy.hbase.entity.ReplenishmentEntity;
import com.infy.hbase.entity.UserActivityEntity;
import com.infy.hbase.entity.annotations.HBaseTable;
import com.infy.hbase.schema.conversion.hbase.HBaseSchemaConversionUtility;
import com.infy.hbase.schema.conversion.text.XMLDOMSchemaConversionUtility;
import com.infy.hbase.schema.definition.ColumnFamilyDefinition;
import com.infy.hbase.schema.definition.TableSchemaDefinition;
import com.infy.hbase.schema.representation.TextRepresentation;
import com.infy.hbase.schema.representation.XMLDOMTextRepresentation;
import com.sun.codemodel.JAnnotationUse;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JClassAlreadyExistsException;
import com.sun.codemodel.JCodeModel;
import com.sun.codemodel.JDefinedClass;
import com.sun.codemodel.JMod;
import com.sun.codemodel.JType;
import com.sun.codemodel.writer.FileCodeWriter;

public class NewTest {

	private static final String[] CONFIGS = new String[] { "classpath*:springData.xml" };

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub
		// new NewTest().getCustomerInfo("1498");
//		 new NewTest().testMyAPI_1();
		// new NewTest().testMyAPI_2();
		// new NewTest().testHBaseScan();
		// new NewTest().testSpringData();
		// new NewTest().testCodeModel();
		// new NewTest().testTypeCast();
		// new NewTest().testDaoSupport();
		// new NewTest().testDaoSupport1();
//		new NewTest().testGenericDaoSupport1();
//		 new NewTest().testGenericDaoSupport2();
//		new NewTest().testGenericDaoSupport3();
		 new NewTest().testAhamTables();
	}

	private void testGenericDaoSupport3() {
		HBaseGenericDao<Product, String> productInfoDao = (HBaseGenericDao<Product, String>) ApplicationContextHelper
				.getBean("productInfoDao");
//		Product entity = productInfoDao.read("00994161000 | Model");
		Product entity = productInfoDao.read("00911221000P");
		System.out.println(entity);
	}

	private void testGenericDaoSupport2() {
		HBaseGenericDao<UserActivityEntity, String> userActivityDao = (HBaseGenericDao<UserActivityEntity, String>) ApplicationContextHelper
				.getBean("userActivityDao");
		UserActivityEntity entity = userActivityDao.read("11540010");
		System.out.println(entity);
		/*
		 * List<UserActivityEntity> entities = userActivityDao.readAll();
		 * System.out.println("--------------read all----------------"); for
		 * (UserActivityEntity e : entities) { System.out.println(e); }
		 */
	}

	private void testGenericDaoSupport1() {
		HBaseGenericDao<ReplenishmentEntity, String> replenishmentDao = (HBaseGenericDao<ReplenishmentEntity, String>) ApplicationContextHelper
				.getBean("replenishmentDao");
		ReplenishmentEntity entity = replenishmentDao.read("9990000043911911P");
		System.out.println(entity);
		List<ReplenishmentEntity> entities = replenishmentDao.readAll();
		System.out.println("--------------read all----------------");
		for (ReplenishmentEntity e : entities) {
			System.out.println(e);
		}
	}

	private void testDaoSupport1() {
		AbstractApplicationContext ctx = new ClassPathXmlApplicationContext(
				CONFIGS);
		SpringDataDaoSupport daoSupport = (SpringDataDaoSupport) ctx
				.getBean("hbaseDaoSupport");
		Map<Object, Class> qualifiersValueType = new HashMap<Object, Class>();
		qualifiersValueType.put("STANDARD_DEVIATION_MANUAL", Double.class);
		qualifiersValueType.put("USER_PREFERENCE", String.class);
		qualifiersValueType.put("STANDARD_DEVIATION_COMPUTED", Double.class);
		qualifiersValueType.put("FREQUENCY_MANUAL", Double.class);
		qualifiersValueType.put("FREQUENCY_COMPUTED", Double.class);
		qualifiersValueType.put("ITEM_TYPE", String.class);
		qualifiersValueType.put("PARENT_KEY", String.class);
		Map<Object, Map<Object, Object>> result = null;
		try {
			result = daoSupport.readAllData("REPLENISHMENT_DETAILS",
					String.class, "REPLENISHMENT_CONFIGURATION",
					qualifiersValueType);
		} catch (Exception e) {
		}
		System.out.println("size: " + result.size());
		for (Map.Entry<Object, Map<Object, Object>> entry : result.entrySet()) {
			System.out.println(entry.getKey() + "-->" + entry.getValue());
			System.out.println("value size: " + entry.getValue().size());
		}
		ctx.registerShutdownHook();
	}

	private void testDaoSupport() {
		AbstractApplicationContext ctx = new ClassPathXmlApplicationContext(
				CONFIGS);
		SpringDataDaoSupport daoSupport = (SpringDataDaoSupport) ctx
				.getBean("hbaseDaoSupport");
		Map<Object, Class> qualifiersValueType = new HashMap<Object, Class>();
		qualifiersValueType.put("STANDARD_DEVIATION_MANUAL", Long.class);
		qualifiersValueType.put("USER_PREFERENCE", String.class);
		qualifiersValueType.put("STANDARD_DEVIATION_COMPUTED", Long.class);
		qualifiersValueType.put("FREQUENCY_MANUAL", Long.class);
		qualifiersValueType.put("FREQUENCY_COMPUTED", Long.class);
		qualifiersValueType.put("ITEM_TYPE", String.class);
		Map<Object, Object> result = daoSupport.readData(
				"REPLENISHMENT_DETAILS", "Clothing_Boys_Bottoms",
				"REPLENISHMENT_CONFIGURATION", qualifiersValueType);
		System.out.println("size: " + result.size());
		for (Map.Entry<Object, Object> entry : result.entrySet()) {
			System.out.println(entry.getKey() + "-->" + entry.getValue());
		}
		ctx.registerShutdownHook();
	}

	private void testTypeCast() {
		// TODO Auto-generated method stub
		Long myLong = new Long(23);
		Object myObject = myLong;
		if (myObject instanceof Integer) {
			System.out.println("original retrieved!!");
		} else {
			System.out.println("boo hoo!!");
		}
		System.out.println(myObject.getClass());
	}

	private void testCodeModel() {
		// TODO Auto-generated method stub
		JCodeModel codeModel = new JCodeModel();
		try {
			JDefinedClass definedClass = codeModel
					._class("com.infy.hbasetest.TestHBaseEntity");
			JClass superClass = codeModel.ref(AbstractHBaseEntity.class);
			definedClass = definedClass._extends(superClass);
			definedClass.field(JMod.PRIVATE, String.class, "partNumber");
			JAnnotationUse annotationUse = definedClass
					.annotate(HBaseTable.class);
			annotationUse.param("name", "hbase_table");
			codeModel.build(new FileCodeWriter(new File("src/main/java")));
		} catch (JClassAlreadyExistsException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private void testSpringData() {
		// TODO Auto-generated method stub
		AbstractApplicationContext ctx = new ClassPathXmlApplicationContext(
				CONFIGS);
		HbaseTemplate hbaseTemplate = (HbaseTemplate) ctx.getBean("hTemplate");
		// shutdown the context along with the VM
		List<String> rows = hbaseTemplate.find("REPLENISHMENT_DETAILS",
				"REPLENISHMENT_CONFIGURATION", new RowMapper<String>() {

					@Override
					public String mapRow(Result result, int rowNum)
							throws Exception {
						System.out.println("rowNum: " + rowNum);
						Map<byte[], NavigableMap<byte[], byte[]>> familyMap = result
								.getNoVersionMap();
						System.out.println("result size: " + result.size());
						System.out.println("no version map size: "
								+ familyMap.size());
						Collection<NavigableMap<byte[], byte[]>> keyValuesSet = familyMap
								.values();
						for (Iterator iterator = keyValuesSet.iterator(); iterator
								.hasNext();) {
							NavigableMap<byte[], byte[]> navigableMap = (NavigableMap<byte[], byte[]>) iterator
									.next();
							System.out.println("navigableMap size: "
									+ navigableMap.size());
							Set<byte[]> keySet = navigableMap.keySet();
							// System.out.println("keySet size: "+keySet.size());
							for (byte[] qualName : keySet) {
								System.out.println(new String(qualName));
							}
						}
						return result.toString();
					}

				});
		for (String s : rows) {
			System.out.println("data: " + s);
		}
		ctx.registerShutdownHook();
	}

	private void testHBaseScan() {
		// TODO Auto-generated method stub
		HTable hTable = null;
		try {
			hTable = new HTable(config, "REPLENISHMENT_DETAILS");
			Scan scan = new Scan();
			ResultScanner scanner = hTable.getScanner(Bytes
					.toBytes("REPLENISHMENT_CONFIGURATION"));
			for (Result result : scanner) {
				Map<byte[], NavigableMap<byte[], byte[]>> familyMap = result
						.getNoVersionMap();
				System.out.println("result size: " + result.size());
				System.out.println("no version map size: " + familyMap.size());
				Collection<NavigableMap<byte[], byte[]>> keyValuesSet = familyMap
						.values();
				for (Iterator iterator = keyValuesSet.iterator(); iterator
						.hasNext();) {
					NavigableMap<byte[], byte[]> navigableMap = (NavigableMap<byte[], byte[]>) iterator
							.next();
					System.out.println("navigableMap size: "
							+ navigableMap.size());
					Set<byte[]> keySet = navigableMap.keySet();
					// System.out.println("keySet size: "+keySet.size());
					for (byte[] qualName : keySet) {
						System.out.println(new String(qualName));
					}
				}
			}

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (hTable != null) {
				try {
					hTable.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	private void testMyAPI_2() {
		// TODO Auto-generated method stub
		try {
			File fXmlFile = new File("file.xml");
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory
					.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document document = dBuilder.parse(fXmlFile);
			document.getDocumentElement().normalize();
			XMLDOMSchemaConversionUtility conversionUtility = new XMLDOMSchemaConversionUtility();
			TextRepresentation<Document> textRepresentation = new XMLDOMTextRepresentation();
			textRepresentation.setRepresentation(document);
			TableSchemaDefinition schemaDefinition = conversionUtility
					.convertTextToSchema((XMLDOMTextRepresentation) textRepresentation);
			System.out.println("table name:" + schemaDefinition.getTableName());
			System.out.println("column family 1:"
					+ ((List<ColumnFamilyDefinition>) schemaDefinition
							.getColumnFamilies()).get(0).getName());// type from
																	// collection
																	// to
																	// List in
																	// class??
			System.out.println("map to entity?? :"
					+ ((List<ColumnFamilyDefinition>) schemaDefinition
							.getColumnFamilies()).get(0).isMapToEntity());

		} catch (ParserConfigurationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SAXException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static Configuration config = HBaseConfiguration.create();

	private static final String HBASE_CUSTOMER_INFO = "CUSTOMER_INFO";
	private static final String CF_CUSTOMER_DATA = "CUSTOMER_DATA";
	private static final String CATEGORY_CLUSTER = "CATEGORY_CLUSTER";
	private static final String BRAND_CLUSTER = "BRAND_CLUSTER";

	private enum Cluster {
		BRAND, SEGMENT
	}

	public void getCustomerInfo(String userId) throws Exception {

		HTable table = null;
		try {
			byte[] userIdBytes = Bytes.toBytes(userId);
			table = new HTable(config, HBASE_CUSTOMER_INFO);
			Get g = new Get(userIdBytes);
			Result result = table.get(g);
			if (result.getRow() != null) {
			} else {
				System.out.println("not found");
			}

			if (result.getValue(Bytes.toBytes(CF_CUSTOMER_DATA),
					Bytes.toBytes(CATEGORY_CLUSTER)) != null) {
				System.out.println(new String(result.getValue(
						Bytes.toBytes(CF_CUSTOMER_DATA),
						Bytes.toBytes(CATEGORY_CLUSTER))));
			}

			if (result.getValue(Bytes.toBytes(CF_CUSTOMER_DATA),
					Bytes.toBytes(BRAND_CLUSTER)) != null) {
				System.out.println(new String(result.getValue(
						Bytes.toBytes(CF_CUSTOMER_DATA),
						Bytes.toBytes(BRAND_CLUSTER))));
			}

			table.close();

		} catch (Exception e) {
			e.printStackTrace();
			throw new Exception("System Error !!");
		} finally {
			table.close();
		}
	}

	public void testMyAPI_1() {
		HBaseAdmin admin = null;
		HTableDescriptor descriptor = null;
		try {
			admin = new HBaseAdmin(config);
			descriptor = admin.getTableDescriptor(Bytes
					.toBytes("REPLENISHMENT_DETAILS"));
			TableSchemaDefinition schemaDefinition = HBaseSchemaConversionUtility
					.convertDescriptorToSchema(descriptor);
			System.out.println(schemaDefinition.getTableName());
			System.out.println(schemaDefinition.getColumnFamilies().size());
			Collection<ColumnFamilyDefinition> cfds = schemaDefinition
					.getColumnFamilies();
			for (ColumnFamilyDefinition cd : cfds) {
				System.out.println(cd.getName());
				// System.out.println(cd.getQualifiers().size());
			}
			XMLDOMSchemaConversionUtility conversionUtility = new XMLDOMSchemaConversionUtility();
			Document document = conversionUtility.convertSchemaToText(
					schemaDefinition).getRepresentation();

			TransformerFactory transformerFactory = TransformerFactory
					.newInstance();
			Transformer transformer = transformerFactory.newTransformer();
			DOMSource source = new DOMSource(document);
			StreamResult result = new StreamResult(new File("src/main/resources/table-schemas/file.xml"));
			transformer.transform(source, result);
			System.out.println(document);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void testAhamTables() {
		String qualifier=AhamTablesClasses.ReplenishmentDetails.ReplenishmentConfiguration.ItemType.value();
		String cf=AhamTablesClasses.ReplenishmentDetails.ReplenishmentConfiguration.value();
		String table=AhamTablesClasses.ReplenishmentDetails.value();
		
		System.out.println("cf= "+cf);
	}

}
