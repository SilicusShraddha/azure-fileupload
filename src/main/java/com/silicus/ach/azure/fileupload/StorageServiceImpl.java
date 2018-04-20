package com.silicus.ach.azure.fileupload;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.EnumSet;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TimeZone;
import java.util.UUID;
import java.util.stream.Stream;

import com.microsoft.azure.storage.Constants;
import com.microsoft.azure.storage.LoggingOperations;
import com.microsoft.azure.storage.MetricsLevel;
import com.microsoft.azure.storage.MetricsProperties;
import com.microsoft.azure.storage.ServiceProperties;
import com.microsoft.azure.storage.ServiceStats;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobContainerPermissions;
import com.microsoft.azure.storage.blob.BlobContainerPublicAccessType;
import com.microsoft.azure.storage.blob.BlobProperties;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.SharedAccessBlobPermissions;
import com.microsoft.azure.storage.blob.SharedAccessBlobPolicy;

public class StorageServiceImpl implements IStorageService {

	public static CloudBlobClient blobClient = null;
	public static CloudBlobContainer container = null;
	
	public StorageServiceImpl() {
		 // Create a blob client for interacting with the blob service
		try {
			blobClient = BlobClientProvider.getBlobClientReference();
		} catch (InvalidKeyException | RuntimeException | IOException | URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void createBlobContainer(String blobContainerName) {
            try {
				container = createContainer(blobClient, blobContainerName);
			} catch (InvalidKeyException | StorageException | RuntimeException | IOException | URISyntaxException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}

	public void createBlockBlob(String blobName, byte[] data) {
		 try {
			CloudBlockBlob blockBlob = container.getBlockBlobReference(blobName);
			blockBlob.uploadFromByteArray(data, 0, data.length);
			System.out.println("\t\tSuccessfully uploaded the blob.");
		} catch (URISyntaxException | StorageException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void createBlockBlob(String blobName, String filePath) {
		 try {
			CloudBlockBlob blockBlob = container.getBlockBlobReference(blobName);
			blockBlob.uploadFromFile(filePath);
			System.out.println("\t\tSuccessfully uploaded the blob.");
		} catch (URISyntaxException | StorageException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public CloudBlobContainer getBlobContainerReference(String containerName) {
		CloudBlobContainer container = null;
		try {
			container = blobClient.getContainerReference(containerName);
		} catch (URISyntaxException | StorageException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return container;
	}
	
	public CloudBlockBlob getBlockBlobReference(String blobName) {
		CloudBlockBlob blockBlob = null;
		try {
			blockBlob = container.getBlockBlobReference(blobName);
		} catch (URISyntaxException | StorageException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return blockBlob;
	}

	public Stream getBlockBlobDataAsStream(String blobName) {
		 
		return null;
	}

	public String getBlockBlobDataAsString(String blobName) {
		 
		return null;
	}

	public List<String> listBlobsInContainer() {
		 
		return null;
	}

	public void deleteBlobContainer(String containerName) {
		 try {
			CloudBlobContainer container = blobClient.getContainerReference(containerName);
			container.deleteIfExists();
		} catch (RuntimeException | URISyntaxException | StorageException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	public void deleteBlob(String blobName) {
		try {
		CloudBlockBlob blockBlob = container.getBlockBlobReference(blobName);
			blockBlob.deleteIfExists();
		} catch (StorageException | URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			
	}
	
	 /**
     * Creates and returns a container for the sample application to use.
     *
     * @param blobClient CloudBlobClient object
     * @param containerName Name of the container to create
     * @return The newly created CloudBlobContainer object
     *
     * @throws StorageException
     * @throws RuntimeException
     * @throws IOException
     * @throws URISyntaxException
     * @throws IllegalArgumentException
     * @throws InvalidKeyException
     * @throws IllegalStateException
     */
    private static CloudBlobContainer createContainer(CloudBlobClient blobClient, String containerName) throws StorageException, RuntimeException, IOException, InvalidKeyException, IllegalArgumentException, URISyntaxException, IllegalStateException {

        // Create a new container
        container = blobClient.getContainerReference(containerName);
        try {
            if (container.createIfNotExists() == false) {
            	System.out.println("Container with name "+containerName+" already exists. Hence, connecting to same container.");
            }
        }
        catch (StorageException s) {
            if (s.getCause() instanceof java.net.ConnectException) {
                System.out.println("Caught connection exception from the client. If running with the default configuration please make sure you have started the storage emulator.");
            }
            throw s;
        }

        return container;
    }
    
    /**
     * List containers sample.
     * @param blobClient Azure Storage Blob Service
     */
    private ArrayList<String> listContainers(CloudBlobClient blobClient) throws URISyntaxException, StorageException {
    	ArrayList<String> containerList = new ArrayList<>();
    	for (final CloudBlobContainer container : blobClient.listContainers()) {
    		containerList.add(container.getName());
        }
    	return containerList;
    }

    /**
     * Manage the service properties including logging hour and minute metrics and default version.
     * @param blobClient Azure Storage Blob Service
     */
    private void serviceProperties(CloudBlobClient blobClient) throws StorageException {

        System.out.println("Get service properties");
        ServiceProperties originalProps = blobClient.downloadServiceProperties();

        try {
            System.out.println("Set service properties");
            // Change service properties
            ServiceProperties props = new ServiceProperties();
            props.setDefaultServiceVersion(Constants.HeaderConstants.TARGET_STORAGE_VERSION);

            props.setDefaultServiceVersion("2009-09-19");

            props.getLogging().setLogOperationTypes(EnumSet.allOf(LoggingOperations.class));
            props.getLogging().setRetentionIntervalInDays(2);
            props.getLogging().setVersion("1.0");

            final MetricsProperties hours = props.getHourMetrics();
            hours.setMetricsLevel(MetricsLevel.SERVICE_AND_API);
            hours.setRetentionIntervalInDays(1);
            hours.setVersion("1.0");

            final MetricsProperties minutes = props.getMinuteMetrics();
            minutes.setMetricsLevel(MetricsLevel.SERVICE);
            minutes.setRetentionIntervalInDays(1);
            minutes.setVersion("1.0");

            blobClient.uploadServiceProperties(props);

            System.out.println();
            System.out.printf("Default service version: %s%n", props.getDefaultServiceVersion());

            System.out.println("Logging");
            System.out.printf("version: %s%n", props.getLogging().getVersion());
            System.out.printf("retention interval: %d%n", props.getLogging().getRetentionIntervalInDays());
            System.out.printf("operation types: %s%n", props.getLogging().getLogOperationTypes());
            System.out.println();
            System.out.println("Hour Metrics");
            System.out.printf("version: %s%n", props.getHourMetrics().getVersion());
            System.out.printf("retention interval: %d%n", props.getHourMetrics().getRetentionIntervalInDays());
            System.out.printf("operation types: %s%n", props.getHourMetrics().getMetricsLevel());
            System.out.println();
            System.out.println("Minute Metrics");
            System.out.printf("version: %s%n", props.getMinuteMetrics().getVersion());
            System.out.printf("retention interval: %d%n", props.getMinuteMetrics().getRetentionIntervalInDays());
            System.out.printf("operation types: %s%n", props.getMinuteMetrics().getMetricsLevel());
            System.out.println();
        }
        finally{
            // Revert back to original service properties
            blobClient.uploadServiceProperties(originalProps);
        }
    }

   

    /**
     * Manage container metadata
     * @param blobClient Azure Storage Blob Service
     */
    private void containerMetadata(CloudBlobClient blobClient, CloudBlobContainer container) throws URISyntaxException, StorageException {
        // Get a reference to a container
        // The container name must be lower case
        CloudBlobContainer container1 = blobClient.getContainerReference(container.getName());
       
            
            container.getMetadata().put("key1", "value1");
            container.getMetadata().put("foo", "bar");
            container.uploadMetadata();

            System.out.println("Get container metadata:");
            HashMap<String, String> metadata = container.getMetadata();
            Iterator it = metadata.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();
                System.out.printf(" %s = %s%n", pair.getKey(), pair.getValue());
                it.remove();
            }
       
    }

    /**
     * Manage container access properties
     * @param blobClient Azure Storage Blob Service
     */
    private void containerAcl(CloudBlobClient blobClient) throws StorageException, URISyntaxException, InterruptedException {
        // Get a reference to a container
        // The container name must be lower case
        CloudBlobContainer container = blobClient.getContainerReference("blobadvancedcontainer"
                + UUID.randomUUID().toString().replace("-", ""));

        try {
            System.out.println("Create container");
            // Create the container if it does not exist
            container.createIfNotExists();

            // Get permissions
            BlobContainerPermissions permissions = container.downloadPermissions();

            System.out.println("Set container permissions");
            final Calendar cal = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
            final Date start = cal.getTime();
            cal.add(Calendar.MINUTE, 30);
            final Date expiry = cal.getTime();

            permissions.setPublicAccess(BlobContainerPublicAccessType.CONTAINER);
            SharedAccessBlobPolicy policy = new SharedAccessBlobPolicy();
            policy.setPermissions(EnumSet.of(SharedAccessBlobPermissions.LIST, SharedAccessBlobPermissions.CREATE));
            policy.setSharedAccessStartTime(start);
            policy.setSharedAccessExpiryTime(expiry);
            permissions.getSharedAccessPolicies().put("key1", policy);

            // Set container permissions
            container.uploadPermissions(permissions);
            System.out.println("Wait 30 seconds for the container permissions to take effect");
            Thread.sleep(30000);

            System.out.println("Get container permissions");
            // Get container permissions
            permissions = container.downloadPermissions();

            System.out.printf(" Public access: %s%n", permissions.getPublicAccess());
            HashMap<String, SharedAccessBlobPolicy> accessPolicies = permissions.getSharedAccessPolicies();
            Iterator it = accessPolicies.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();
                SharedAccessBlobPolicy value = (SharedAccessBlobPolicy) pair.getValue();
                System.out.printf(" %s: %n", pair.getKey());
                System.out.printf("  Permissions: %s%n", value.permissionsToString());
                System.out.printf("  Start: %s%n", value.getSharedAccessStartTime());
                System.out.printf("  Expiry: %s%n", value.getSharedAccessStartTime());
                it.remove();
            }

            System.out.println("Clear container permissions");
            // Clear permissions
            permissions.getSharedAccessPolicies().clear();
            container.uploadPermissions(permissions);
        }
        finally {
            if (container.deleteIfExists()) {
                System.out.println(String.format("Successfully deleted the container: %s", container.getName()));
            }
        }
    }

    /**
     * Mangage blob properties
     * @param blobClient Azure Storage Blob Service
     */
    private void blobProperties(CloudBlobClient blobClient) throws StorageException, URISyntaxException, IOException {
        // Get a reference to a container
        // The container name must be lower case
        CloudBlobContainer container = blobClient.getContainerReference("blobadvancedcontainer"
                + UUID.randomUUID().toString().replace("-", ""));

        try {
            System.out.println("Create container");
            // Create the container if it does not exist
            container.createIfNotExists();

            Random random = new Random();
            File tempFile = new File("change this path");
            System.out.println(String.format("Successfully created the file \"%s\"", tempFile.getAbsolutePath()));

            System.out.println("Use a sample file as a block blob");
            CloudBlockBlob blob = container.getBlockBlobReference("blockblob1.tmp");

            // Set blob properties
            System.out.println("Set blob properties");
            blob.getProperties().setContentType("text/plain");
            blob.getProperties().setContentEncoding("UTF8");
            blob.getProperties().setContentLanguage("en");

            // Upload the block blob
            blob.uploadFromFile(tempFile.getAbsolutePath());
            System.out.println("Successfully uploaded the blob");

            System.out.println("Get blob properties");
            BlobProperties properties = blob.getProperties();
            System.out.printf("Blob type: %s%n", properties.getBlobType());
            System.out.printf("Cache control: %s%n", properties.getCacheControl());
            System.out.printf("Content disposition: %s%n", properties.getContentDisposition());
            System.out.printf("Content encoding: %s%n", properties.getContentEncoding());
            System.out.printf("Content language: %s%n", properties.getContentLanguage());
            System.out.printf("Content type: %s%n", properties.getContentType());
            System.out.printf("Last modified: %s%n", properties.getLastModified());
            System.out.printf("Lease state: %s%n", properties.getLeaseState());
            System.out.printf("Lease status: %s%n", properties.getLeaseStatus());
        }
        finally {
            if (container.deleteIfExists()) {
                System.out.println(String.format("Successfully deleted the container: %s", container.getName()));
            }
        }
    }

    /**
     * Manage the blob metadata
     * @param blobClient Azure Storage Blob Service
     */
    private void blobMetadata(CloudBlobClient blobClient) throws URISyntaxException, StorageException, IOException {
        // Get a reference to a container
        // The container name must be lower case
        CloudBlobContainer container = blobClient.getContainerReference("blobadvancedcontainer"
                + UUID.randomUUID().toString().replace("-", ""));

        try {
            System.out.println("Create container");
            // Create the container if it does not exist
            container.createIfNotExists();

            Random random = new Random();
            File tempFile = new File("change this path");
            System.out.println(String.format("Successfully created the file \"%s\"", tempFile.getAbsolutePath()));

            // Use a sample file as a block blob
            System.out.println("Upload a sample file as a block blob");
            CloudBlockBlob blob = container.getBlockBlobReference("blockblob1.tmp");

            System.out.println("Set blob metadata");
            blob.getMetadata().put("key1", "value1");
            blob.getMetadata().put("foo", "bar");

            // Upload the block blob
            blob.uploadFromFile(tempFile.getAbsolutePath());
            System.out.println("Successfully uploaded the blob");

            System.out.println("Get blob metadata:");
            HashMap<String, String> metadata = blob.getMetadata();
            Iterator it = metadata.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();
                System.out.printf(" %s = %s%n", pair.getKey(), pair.getValue());
                it.remove();
            }
        }
        finally {
            if (container.deleteIfExists()) {
                System.out.println(String.format("Successfully deleted the container: %s", container.getName()));
            }
        }
    }

    /**
     * Retrieve statistics related to replication for the Blob service.
     * This operation is only available on the secondary location endpoint
     * when read-access geo-redundant replication is enabled for the storage account.
     * @param blobClient Azure Storage Blob Service
     */
    private void serviceStats(CloudBlobClient blobClient) throws StorageException {
        // Get service stats
        System.out.println("Service Stats:");
        ServiceStats stats = blobClient.getServiceStats();
        System.out.printf("- status: %s%n", stats.getGeoReplication().getStatus());
        System.out.printf("- last sync time: %s%n", stats.getGeoReplication().getLastSyncTime());
    }


}
