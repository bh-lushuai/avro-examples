package com.avro.example01;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.*;

import java.io.File;
import java.io.IOException;

/**
 * 非“代码生成”情况：无需通过Schema生成java代码，开发者需要在运行时指定Schema。
 *
 * Created by lushuai on 16-10-5.
 */
public class NoJavaApiTest {

    public static void main(String[] args) {

        //1.序列化
        //user.avsc放置在“resources/avro”目录下
        InputStream inputStream = ClassLoader.getSystemResourceAsStream("avro/user.avsc");
        Schema schema = null;
        try {
            schema = new Schema.Parser().parse(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }

        GenericRecord user = new GenericData.Record(schema);
        user.put("name", "张三");
        user.put("age", 30);
        user.put("email","zhangsan@*.com");

        File diskFile = new File("/tmp/users.avro");
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);
        try {
            dataFileWriter.create(schema, diskFile);
            dataFileWriter.append(user);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                dataFileWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        //2.反序列化
        DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
        DataFileReader<GenericRecord> dataFileReader = null;
        try {
            dataFileReader= new DataFileReader<GenericRecord>(diskFile, datumReader);
            GenericRecord _current = null;
            while (dataFileReader.hasNext()) {
                _current = dataFileReader.next(_current);
                System.out.println(user);
             }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                if(dataFileReader!=null)dataFileReader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }


}
