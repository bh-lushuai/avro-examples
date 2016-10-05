package com.avro.example01;

import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

/**
 * Created by lushuai on 16-10-5.
 */
public class UseJavaApiTest {

    public static void main(String[] args) {
        //对象序列化到avro文件
        write2avro();
        //从avro文件反序列化对象
        readFromAvro();
    }


    public static void write2avro(){
        // 声明并初始化User对象
        // 方式一
        User user1 = new User();
        user1.setName("zhangsan");
        user1.setAge(21);
        user1.setEmail(null);

        // 方式二 使用构造函数
        // Alternate constructor
        User user2 = new User("Ben", 7, "123@mail.com");
        
        // 方式三，使用Build方式
        // Construct via builder
        User user3 = User.newBuilder()
                .setName("Charlie")
                .setAge(20)
                .setEmail("2342@jd.com")
                .build();
        String path = "/tmp/user.avro"; // avro文件存放目录
        File file=new File(path);
        DatumWriter<User> userDatumWriter = new SpecificDatumWriter<User>(User.class);
        DataFileWriter<User> dataFileWriter = new DataFileWriter<User>(userDatumWriter);
        try {
            if(file.exists()){
                //如果文件已经存在则增量把数据append到文件尾部
                dataFileWriter.appendTo(file);
            }else{
                //创建文件
                dataFileWriter.create(user1.getSchema(),  file);
            }
            // 把生成的user对象写入到avro文件
            dataFileWriter.append(user1);
            dataFileWriter.append(user2);
            dataFileWriter.append(user3);
            dataFileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    public static void readFromAvro(){
        DatumReader<User> reader = new SpecificDatumReader<User>(User.class);
        DataFileReader<User> dataFileReader = null;
        try {
            dataFileReader = new DataFileReader<User>(new File("/tmp/user.avro"), reader);
            User user = null;
            while (dataFileReader.hasNext()) {
                user = dataFileReader.next();
                System.out.println(user);
            }
            dataFileReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
