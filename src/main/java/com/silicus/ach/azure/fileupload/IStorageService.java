package com.silicus.ach.azure.fileupload;

import java.util.List;
import java.util.stream.Stream;

import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;

public interface IStorageService {

 public void createBlobContainer(String blobContainerName);

	
 public void createBlockBlob(String blobName, byte[] data);

 
 public CloudBlockBlob getBlockBlobReference(String blobName);
 
 public CloudBlobContainer getBlobContainerReference(String containerName);

 public Stream getBlockBlobDataAsStream(String blobName);


 public String getBlockBlobDataAsString(String blobName);

  
 public List<String> listBlobsInContainer();

 
 public void deleteBlobContainer(String containerName);


 public void deleteBlob(String blobName);

}
