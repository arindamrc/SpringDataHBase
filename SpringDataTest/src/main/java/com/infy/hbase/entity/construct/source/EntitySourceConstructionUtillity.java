package com.infy.hbase.entity.construct.source;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;

import org.apache.commons.lang.StringUtils;

import com.infy.hbase.entity.AbstractHBaseEntity;
import com.infy.hbase.entity.annotations.HBaseTable;
import com.infy.hbase.schema.definition.ColumnFamilyDefinition;
import com.infy.hbase.schema.definition.QualifierDefinition;
import com.sun.codemodel.JAnnotationUse;
import com.sun.codemodel.JClass;
import com.sun.codemodel.JClassAlreadyExistsException;
import com.sun.codemodel.JCodeModel;
import com.sun.codemodel.JDefinedClass;
import com.sun.codemodel.JMod;
import com.sun.codemodel.writer.FileCodeWriter;

public class EntitySourceConstructionUtillity {

	private static JCodeModel codeModel;

	static {
		codeModel = new JCodeModel();
	}

	public void convertDefinitionToSource(
			ColumnFamilyDefinition columnFamilyDefinition) {
		try {
			JDefinedClass definedClass = codeModel
					._class("com.infy.hbase.entity."
							+ columnFamilyDefinition.getEntityName());
			JClass superClass = codeModel.ref(AbstractHBaseEntity.class);
			definedClass = definedClass._extends(superClass);
			JAnnotationUse annotationUse=definedClass.annotate(HBaseTable.class);
			annotationUse.param("name", columnFamilyDefinition.getTableName());

			Collection<QualifierDefinition> qualifierDefinitions = columnFamilyDefinition
					.getQualifiers();
			for (QualifierDefinition qualifierDefinition : qualifierDefinitions) {
				definedClass.field(JMod.PRIVATE,
						getDataType(qualifierDefinition.getType()),
						qualifierDefinition.getName());// add private field to
														// class
				definedClass
						.method(JMod.PUBLIC,
								codeModel.VOID,
								"set"
										+ StringUtils
												.capitalize(qualifierDefinition
														.getName()));// add
																		// setter
																		// method

				definedClass.method(
						JMod.PUBLIC,
						getDataType(
								qualifierDefinition.getType()),
								"get"
										+ StringUtils
												.capitalize(qualifierDefinition
														.getName()));// add
																		// getter
																		// method
			}

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

	public ColumnFamilyDefinition convertSourceToDefinition(
			Class<? extends AbstractHBaseEntity> entity) {
		return null;
	}

	private Class<?> getDataType(String type) {
		if ("string".equalsIgnoreCase(type)) {
			return String.class;
		} else if ("int".equalsIgnoreCase(type)) {
			return Integer.class;
		} else if ("short".equalsIgnoreCase(type)) {
			return Short.class;
		} else if ("long".equalsIgnoreCase(type)) {
			return Long.class;
		} else if ("float".equalsIgnoreCase(type)) {
			return Float.class;
		} else if ("double".equalsIgnoreCase(type)) {
			return Double.class;
		} else if ("char".equalsIgnoreCase(type)) {
			return Character.class;
		} else if ("boolean".equalsIgnoreCase(type)) {
			return Boolean.class;
		}
		return null;
	}
}
