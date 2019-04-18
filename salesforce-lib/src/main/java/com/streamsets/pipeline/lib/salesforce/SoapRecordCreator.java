/*
 * Copyright 2017 StreamSets Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.streamsets.pipeline.lib.salesforce;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.sforce.soap.partner.sobject.SObject;
import com.sforce.ws.bind.XmlObject;
import com.streamsets.pipeline.api.Field;
import com.streamsets.pipeline.api.Record;
import com.streamsets.pipeline.api.Stage;
import com.streamsets.pipeline.api.StageException;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class SoapRecordCreator extends SobjectRecordCreator {
  enum XmlType {
    DATE_TIME("dateTime", Field.Type.DATETIME),
    DATE("date", Field.Type.DATE),
    INT("int", Field.Type.INTEGER),
    DOUBLE("double", Field.Type.DOUBLE);

    private final String xmlType;
    private final Field.Type fieldType;

    XmlType(String xmlType, Field.Type fieldType) {
      this.xmlType = xmlType;
      this.fieldType = fieldType;
    }

    public String getXmlType() {
      return xmlType;
    }

    public Field.Type getFieldType() {
      return fieldType;
    }

    private static ImmutableMap<String, XmlType> reverseLookup =
        Maps.uniqueIndex(Arrays.asList(XmlType.values()), XmlType::getXmlType);

    public static XmlType fromString(final String xmlType) {
      return reverseLookup.get(xmlType);
    }
  }

  private static final String AGGREGATE_RESULT = "aggregateresult";
  private static final String QUERY_RESULT = "QueryResult";
  private static final String RECORDS = "records";

  public SoapRecordCreator(Stage.Context context, ForceInputConfigBean conf, String sobjectType) {
    super(context, conf, sobjectType);
  }

  @Override
  public Record createRecord(String sourceId, Object source) throws StageException {
    SObject record = (SObject)source;

    Record rec = context.createRecord(sourceId);
    rec.set(Field.createListMap(addFields(record, null)));
    rec.getHeader().setAttribute(SOBJECT_TYPE_ATTRIBUTE, sobjectType);

    return rec;
  }

  public LinkedHashMap<String, Field> addFields(
      XmlObject parent,
      Map<String, DataType> columnsToTypes
  ) throws StageException {
    LinkedHashMap<String, Field> map = new LinkedHashMap<>();

    Iterator<XmlObject> iter = parent.getChildren();
    String type = null;
    while (iter.hasNext()) {
      XmlObject obj = iter.next();

      String key = obj.getName().getLocalPart();
      if ("type".equals(key)) {
        // Housekeeping field
        type = obj.getValue().toString().toLowerCase();
        continue;
      }

      if (obj.hasChildren()) {
        if (QUERY_RESULT.equals(obj.getXmlType().getLocalPart())) {
          // Nested subquery - need to make an array
          Iterator<XmlObject> records = obj.getChildren(RECORDS);
          List<Field> recordList = new ArrayList<>();
          while (records.hasNext()) {
            XmlObject record = records.next();
            recordList.add(Field.createListMap(addFields(record, columnsToTypes)));
          }
          map.put(key, Field.create(recordList));
        } else {
          // Following a relationship
          map.put(key, Field.createListMap(addFields(obj, columnsToTypes)));
        }
      } else {
        Object val = obj.getValue();
        if ("Id".equalsIgnoreCase(key) && null == val) {
          // Get a null Id if you don't include it in the SELECT
          continue;
        }
        if (type == null) {
          throw new StageException(
              Errors.FORCE_04,
              "No type information for " + obj.getName().getLocalPart() +
                  ". Specify component fields of compound fields, e.g. Location__Latitude__s or BillingStreet"
          );
        }

        DataType dataType = (columnsToTypes != null)
            ? columnsToTypes.get(key.toLowerCase())
            : DataType.USE_SALESFORCE_TYPE;
        if (dataType == null) {
          dataType = DataType.USE_SALESFORCE_TYPE;
        }

        Field field;
        if (AGGREGATE_RESULT.equals(type)) {
          field = getField(obj, dataType);
        } else {
          com.sforce.soap.partner.Field sfdcField = getFieldMetadata(type, key);
          if (sfdcField == null) {
            // null relationship
            field = Field.createListMap(new LinkedHashMap<>());
          } else {
            field = createField(val, dataType, sfdcField);
          }
          if (conf.createSalesforceNsHeaders) {
            setHeadersOnField(field, sfdcField);
          }
        }
        map.put(key, field);
      }
    }

    return map;
  }

  @NotNull
  private Field getField(XmlObject obj, DataType userSpecifiedType) throws StageException {
    Object val = obj.getValue();

    if (userSpecifiedType != DataType.USE_SALESFORCE_TYPE) {
      return Field.create(Field.Type.valueOf(userSpecifiedType.getLabel()), val);
    }

    Field field;
    if (obj.getXmlType() == null) {  // String data does not contain an XML type!
      field = Field.create(Field.Type.STRING, (val == null) ? null : val.toString());
    } else {
      XmlType xmlType = XmlType.fromString(obj.getXmlType().getLocalPart());

      switch (xmlType) {
        case DATE_TIME:
          field = Field.create(xmlType.getFieldType(), (val == null) ? null : ((GregorianCalendar)val).getTime());
          break;
        case DATE:
        case INT:
        case DOUBLE:
          field = Field.create(xmlType.getFieldType(), val);
          break;
        default:
          throw new StageException(Errors.FORCE_04, UNEXPECTED_TYPE + xmlType);
      }
    }

    return field;
  }
}
