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
package com.streamsets.pipeline.stage.origin.s3;

import com.streamsets.pipeline.stage.common.AmazonS3TestSuite;
import com.streamsets.pipeline.stage.lib.aws.AWSUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestAmazonS3Source extends AmazonS3TestSuite {


  private S3ConfigBean createConfigLexicographically() {
    S3ConfigBean config = new S3ConfigBean();
    config.s3FileConfig = new S3FileConfig();
    config.s3FileConfig.objectOrdering = ObjectOrdering.LEXICOGRAPHICAL;

    return config;
  }

  private S3ConfigBean createConfigTimestamp() {
    S3ConfigBean config = new S3ConfigBean();

    config.s3FileConfig = new S3FileConfig();
    config.s3FileConfig.objectOrdering = ObjectOrdering.TIMESTAMP;

    return config;
  }

  @Test
  public void testNormalizePrefix() {
    String prefix = "/";
    String delimiter = "/";
    prefix = AWSUtil.normalizePrefix(prefix, delimiter);
    Assert.assertEquals("", prefix);

    prefix = "/foo";
    delimiter = "/";
    prefix = AWSUtil.normalizePrefix(prefix, delimiter);
    Assert.assertEquals("foo/", prefix);
  }

  @Test
  public void testToStringExcelFile() {
    S3Offset s3Offset = new S3Offset("test.xlsx", "test sheet::1234", "0dd65bf073ad061", "1534360");
    Assert.assertEquals("test.xlsx::test sheet:\\:1234::0dd65bf073ad061::1534360", s3Offset.toString());
  }

  @Test
  public void testToStringTextFile() {
    S3Offset s3Offset = new S3Offset("test.txt", "1234", "0dd65bf073ad061", "1534360");
    Assert.assertEquals("test.txt::1234::0dd65bf073ad061::1534360", s3Offset.toString());
  }

  @Test
  public void testFromStringExcelFile() throws Exception {
    String offset = "FL_insurance.xlsx::Sheet 1 - FL_insurance:\\:1000::0dd65bf073ad0616a91901c9349dd5a4::1534360";
    S3Offset s3Offset = S3Offset.fromString(offset);
    Assert.assertEquals("FL_insurance.xlsx", s3Offset.getKey());
    Assert.assertEquals("Sheet 1 - FL_insurance::1000", s3Offset.getOffset());
    Assert.assertEquals("0dd65bf073ad0616a91901c9349dd5a4", s3Offset.geteTag());
    Assert.assertEquals("1534360", s3Offset.getTimestamp());
  }

  @Test
  public void testFromStringTextFile() throws Exception {
    String offset = "FL_insurance.txt::1000::0dd65bf073ad0616a91901c9349dd5a4::1534360";
    S3Offset s3Offset = S3Offset.fromString(offset);
    Assert.assertEquals("FL_insurance.txt", s3Offset.getKey());
    Assert.assertEquals("1000", s3Offset.getOffset());
    Assert.assertEquals("0dd65bf073ad0616a91901c9349dd5a4", s3Offset.geteTag());
    Assert.assertEquals("1534360", s3Offset.getTimestamp());
  }

  @Test
  public void testOrderOffsetsLexicographically() throws Exception {
    AmazonS3SourceImpl amazonS3Source = new AmazonS3SourceImpl(createConfigLexicographically());

    String offset1 = "cFL_insurance.txt::1000::0dd65bf073ad0616a91901c9349dd5a4::1534360";
    String offset2 = "aFL_insurance.txt::1000::0dd65bf073ad0616a91901c9349dd5a4::1534360";
    String offset3 = "bFL_insurance.txt::1000::0dd65bf073ad0616a91901c9349dd5a4::1534360";
    List<S3Offset> listOfOffsets = new ArrayList<>();
    listOfOffsets.add(S3Offset.fromString(offset1));
    listOfOffsets.add(S3Offset.fromString(offset2));
    listOfOffsets.add(S3Offset.fromString(offset3));


    Assert.assertEquals(S3Offset.fromString(offset1).toString(),
        amazonS3Source.orderOffsets(listOfOffsets).get(2).toString()
    );
  }

  @Test
  public void testOrderOffsetsTimestamp() throws Exception {
    AmazonS3SourceImpl amazonS3Source = new AmazonS3SourceImpl(createConfigTimestamp());

    String offset1 = "FL_insurance.txt::1000::0dd65bf073ad0616a91901c9349dd5a4::1534360";
    String offset2 = "FL_insurance.txt::1000::0dd65bf073ad0616a91901c9349dd5a4::1534362";
    String offset3 = "FL_insurance.txt::1000::0dd65bf073ad0616a91901c9349dd5a4::1534361";
    List<S3Offset> listOfOffsets = new ArrayList<>();
    listOfOffsets.add(S3Offset.fromString(offset1));
    listOfOffsets.add(S3Offset.fromString(offset2));
    listOfOffsets.add(S3Offset.fromString(offset3));


    Assert.assertEquals(S3Offset.fromString(offset2).toString(),
        amazonS3Source.orderOffsets(listOfOffsets).get(2).toString()
    );
  }

  @Test
  public void testCreateInitialOffsetMapTimestamp() throws Exception {
    AmazonS3SourceImpl amazonS3Source = new AmazonS3SourceImpl(createConfigTimestamp());

    String offset1 = "FL_insurance.txt::1000::0dd65bf073ad0616a91901c9349dd5a4::1534360";
    String offset2 = "FL_insurance.txt::1000::0dd65bf073ad0616a91901c9349dd5a4::1534362";
    String offset3 = "FL_insurance.txt::1000::0dd65bf073ad0616a91901c9349dd5a4::1534361";

    List<S3Offset> listOfOffsets = new ArrayList<>();
    listOfOffsets.add(S3Offset.fromString(offset1));
    listOfOffsets.add(S3Offset.fromString(offset2));
    listOfOffsets.add(S3Offset.fromString(offset3));

    Map<String, String> mapOfOffsets = new HashMap<>();
    for (int iterator = 0; iterator < listOfOffsets.size(); iterator++) {
      mapOfOffsets.put(String.valueOf(iterator), listOfOffsets.get(iterator).toString());
    }

    amazonS3Source.createInitialOffsetsMap(mapOfOffsets);

    List<S3Offset> expectedList = amazonS3Source.orderOffsets(listOfOffsets);
    List<S3Offset> resultList = new ArrayList<>(amazonS3Source.offsetsMap.values());
    for (int iterator = 0; iterator < 3; iterator++) {
      Assert.assertEquals(expectedList.get(iterator).toString(), resultList.get(iterator).toString());
    }
  }

  @Test
  public void testCreateInitialOffsetMapLexicographically() throws Exception {
    AmazonS3SourceImpl amazonS3Source = new AmazonS3SourceImpl(createConfigLexicographically());

    String offset1 = "cFL_insurance.txt::1000::0dd65bf073ad0616a91901c9349dd5a4::1534360";
    String offset2 = "aFL_insurance.txt::1000::0dd65bf073ad0616a91901c9349dd5a4::1534360";
    String offset3 = "bFL_insurance.txt::1000::0dd65bf073ad0616a91901c9349dd5a4::1534360";

    List<S3Offset> listOfOffsets = new ArrayList<>();
    listOfOffsets.add(S3Offset.fromString(offset1));
    listOfOffsets.add(S3Offset.fromString(offset2));
    listOfOffsets.add(S3Offset.fromString(offset3));

    Map<String, String> mapOfOffsets = new HashMap<>();
    for (int iterator = 0; iterator < listOfOffsets.size(); iterator++) {
      mapOfOffsets.put(String.valueOf(iterator), listOfOffsets.get(iterator).toString());
    }

    amazonS3Source.createInitialOffsetsMap(mapOfOffsets);

    List<S3Offset> expectedList = amazonS3Source.orderOffsets(listOfOffsets);
    List<S3Offset> resultList = new ArrayList<>(amazonS3Source.offsetsMap.values());
    for (int iterator = 0; iterator < 3; iterator++) {
      Assert.assertEquals(expectedList.get(iterator).toString(), resultList.get(iterator).toString());
    }
  }

  @Test
  public void testAllFilesAreFinished() throws Exception {
    AmazonS3SourceImpl amazonS3Source = new AmazonS3SourceImpl(createConfigTimestamp());

    String offset1 = "FL_insurance.txt::1000::0dd65bf073ad0616a91901c9349dd5a4::1534360";
    String offset2 = "FL_insurance.txt::1000::0dd65bf073ad0616a91901c9349dd5a4::1534362";
    String offset3 = "FL_insurance.txt::1000::0dd65bf073ad0616a91901c9349dd5a4::1534361";

    List<S3Offset> listOfOffsets = new ArrayList<>();
    listOfOffsets.add(S3Offset.fromString(offset1));
    listOfOffsets.add(S3Offset.fromString(offset2));
    listOfOffsets.add(S3Offset.fromString(offset3));

    Map<String, String> mapOfOffsets = new HashMap<>();
    for (int iterator = 0; iterator < listOfOffsets.size(); iterator++) {
      mapOfOffsets.put(String.valueOf(iterator), listOfOffsets.get(iterator).toString());
    }

    amazonS3Source.createInitialOffsetsMap(mapOfOffsets);
    Assert.assertFalse(amazonS3Source.allFilesAreFinished());

    // Start again with different offsets
    amazonS3Source = new AmazonS3SourceImpl(createConfigTimestamp());
    offset1 = "FL_insurance.txt::-1::0dd65bf073ad0616a91901c9349dd5a4::1534360";
    offset2 = "FL_insurance.txt::-1::0dd65bf073ad0616a91901c9349dd5a4::1534362";
    offset3 = "FL_insurance.txt::1000::0dd65bf073ad0616a91901c9349dd5a4::1534361";

    listOfOffsets = new ArrayList<>();
    listOfOffsets.add(S3Offset.fromString(offset1));
    listOfOffsets.add(S3Offset.fromString(offset2));
    listOfOffsets.add(S3Offset.fromString(offset3));

    mapOfOffsets = new HashMap<>();
    for (int iterator = 0; iterator < listOfOffsets.size(); iterator++) {
      mapOfOffsets.put(String.valueOf(iterator), listOfOffsets.get(iterator).toString());
    }

    amazonS3Source.createInitialOffsetsMap(mapOfOffsets);
    Assert.assertFalse(amazonS3Source.allFilesAreFinished());

    // Start again with different offsets
    amazonS3Source = new AmazonS3SourceImpl(createConfigTimestamp());
    offset1 = "FL_insurance.txt::-1::0dd65bf073ad0616a91901c9349dd5a4::1534360";
    offset2 = "FL_insurance.txt::-1::0dd65bf073ad0616a91901c9349dd5a4::1534362";
    offset3 = "FL_insurance.txt::-1::0dd65bf073ad0616a91901c9349dd5a4::1534361";

    listOfOffsets = new ArrayList<>();
    listOfOffsets.add(S3Offset.fromString(offset1));
    listOfOffsets.add(S3Offset.fromString(offset2));
    listOfOffsets.add(S3Offset.fromString(offset3));

    mapOfOffsets = new HashMap<>();
    for (int iterator = 0; iterator < listOfOffsets.size(); iterator++) {
      mapOfOffsets.put(String.valueOf(iterator), listOfOffsets.get(iterator).toString());
    }

    amazonS3Source.createInitialOffsetsMap(mapOfOffsets);
    Assert.assertTrue(amazonS3Source.allFilesAreFinished());
  }

  @Test
  public void testGetOffsetNoOrphanThreads() throws Exception {
    AmazonS3SourceImpl amazonS3Source = new AmazonS3SourceImpl(createConfigTimestamp());

    String emptyOffsetString = "::0::::0";

    Assert.assertEquals(emptyOffsetString, amazonS3Source.getOffset(0).toString());

    String offset1 = "FL_insurance.txt::1000::0dd65bf073ad0616a91901c9349dd5a4::1534360";

    Map<String, String> mapOfOffsets = new HashMap<>();
    mapOfOffsets.put("0", S3Offset.fromString(offset1).toString());
    amazonS3Source.createInitialOffsetsMap(mapOfOffsets);

    Assert.assertEquals(offset1, amazonS3Source.getOffset(0).toString());

    String offset2 = "FL_insurance.txt::-1::0dd65bf073ad0616a91901c9349dd5a4::1534360";

    mapOfOffsets = new HashMap<>();
    mapOfOffsets.put("0", S3Offset.fromString(offset2).toString());
    amazonS3Source.createInitialOffsetsMap(mapOfOffsets);

    Assert.assertEquals(offset2, amazonS3Source.getOffset(0).toString());
  }

  @Test
  public void testGetOffsetOrphanThreadsEmptyMap() throws Exception {
    AmazonS3SourceImpl amazonS3Source = new AmazonS3SourceImpl(createConfigTimestamp());


    String offset1 = "FL_insurance.txt::-1::0dd65bf073ad0616a91901c9349dd5a4::1534360";
    String offset2 = "FL_insurance.txt::1000::0dd65bf073ad0616a91901c9349dd5a4::1534360";

    Map<String, String> mapOfOffsets = new HashMap<>();
    mapOfOffsets.put("0", S3Offset.fromString(offset1).toString());
    mapOfOffsets.put("1", S3Offset.fromString(offset2).toString());
    amazonS3Source.createInitialOffsetsMap(mapOfOffsets);

    amazonS3Source.orphanThreads.add(S3Offset.fromString(offset1));

    Assert.assertEquals(offset1, amazonS3Source.getOffset(0).toString());
  }
}
