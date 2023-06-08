package com.spring.minio.fileapi.service;


import com.spring.minio.fileapi.http.dto.FileDto;
import io.minio.*;
import io.minio.messages.Item;
import lombok.extern.slf4j.Slf4j;

import org.checkerframework.checker.units.qual.s;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;



@Slf4j
@Service
public class MinioService {


    

    private final String url = "jdbc:postgresql://localhost:5432/fileMinio";
    private final String user = "minio_admin";
    private final String password = "2102";

    private static final String INSERT_FILE_SQL = "INSERT INTO file" + "  (file_id, name) VALUES " + " (?, ?);";

    public void insertRecord(String[] arr) throws SQLException {
        System.out.println(INSERT_FILE_SQL);
        // Step 1: Establishing a Connection
        try (Connection connection = DriverManager.getConnection(url, user, password);

            // Step 2:Create a statement using connection object
            PreparedStatement preparedStatement = connection.prepareStatement(INSERT_FILE_SQL)) {
            preparedStatement.setString(1, arr[0]);
            preparedStatement.setString(2, arr[1]);


            System.out.println(preparedStatement);
            // Step 3: Execute the query or update query
            preparedStatement.executeUpdate();
        } catch (SQLException e) {

            // print SQL exception information
            printSQLException(e);
        }

        // Step 4: try-with-resource statement will auto close the connection.
    }

    public static void printSQLException(SQLException ex) {
        for (Throwable e: ex) {
            if (e instanceof SQLException) {
                e.printStackTrace(System.err);
                System.err.println("SQLState: " + ((SQLException) e).getSQLState());
                System.err.println("Error Code: " + ((SQLException) e).getErrorCode());
                System.err.println("Message: " + e.getMessage());
                Throwable t = ex.getCause();
                while (t != null) {
                    System.out.println("Cause: " + t);
                    t = t.getCause();
                }
            }
        }
    }



    @Autowired
    private MinioClient minioClient;

    @Value("${minio.bucket.name}")
    private String bucketName;

    public List<FileDto> getListObjects() throws SQLException {
        
        List<FileDto> objects = new ArrayList<>();
        try {
            Iterable<Result<Item>> result = minioClient.listObjects(ListObjectsArgs.builder()
                    .bucket(bucketName)
                    .recursive(true)
                    .build());
            for (Result<Item> item : result) {
                objects.add(FileDto.builder()
                        .filename(item.get().objectName())
                        .size(item.get().size())
                        .etag(item.get().etag())
                        .url(getPreSignedUrl(item.get().objectName()))
                        .build());
            }
            return objects;
        } catch (Exception e) {
            log.error("Happened error when get list objects from minio: ", e);
        }

        return objects;
    }

    public InputStream getObject(String filename) {
        InputStream stream;
        try {
            stream = minioClient.getObject(GetObjectArgs.builder()
                    .bucket(bucketName)
                    .object(filename)
                    .build());
        } catch (Exception e) {
            log.error("Happened error when get list objects from minio: ", e);
            return null;
        }

        return stream;
    }

    public FileDto uploadFile(FileDto request) throws SQLException {
        try {
            minioClient.putObject(PutObjectArgs.builder()
                    .bucket(bucketName)
                    .object(request.getFile().getOriginalFilename())
                    .stream(request.getFile().getInputStream(), request.getFile().getSize(), -1)
                    .build());
        } catch (Exception e) {
            log.error("Happened error when upload file: ", e);
        }


        UUID uuid = UUID.randomUUID();
        String[] array = new String[10];
        array[0] = uuid.toString();
        array[1] = request.getFile().getOriginalFilename().toString();
        insertRecord(array);
        return FileDto.builder()
                .title(request.getFile().getOriginalFilename())
                .description(request.getDescription())
                .size(request.getFile().getSize())
                .url(getPreSignedUrl(request.getFile().getOriginalFilename()))
                .uuid(uuid.toString())
                .build();
    }

    public FileDto deleteFile(FileDto request) {
       
        try {
            minioClient.removeObject(RemoveObjectArgs.builder()
                    .bucket(bucketName)
                    .object(request.getEtag())
                    .build());
        } catch (Exception e) {
            System.out.println("Happened error when deleting file");
            log.error("Happened error when deleting file: ", e);
        }
        return FileDto.builder()
        .etag(request.getEtag())
        .build();
    }
    
    private String getPreSignedUrl(String filename) {
        return "http://localhost:9010/file/".concat(filename);
    }

    
 

}