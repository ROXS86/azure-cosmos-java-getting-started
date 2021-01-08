// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

package com.azure.cosmos.sample.async;

import com.azure.cosmos.ChangeFeedProcessor;
import com.azure.cosmos.ChangeFeedProcessorBuilder;
import com.azure.cosmos.ConsistencyLevel;
import com.azure.cosmos.CosmosAsyncClient;
import com.azure.cosmos.CosmosAsyncContainer;
import com.azure.cosmos.CosmosAsyncDatabase;
import com.azure.cosmos.CosmosClientBuilder;
import com.azure.cosmos.CosmosException;
import com.azure.cosmos.models.ChangeFeedProcessorOptions;
import com.azure.cosmos.models.CosmosContainerProperties;
import com.azure.cosmos.models.CosmosContainerRequestOptions;
import com.azure.cosmos.models.CosmosContainerResponse;
import com.azure.cosmos.models.CosmosDatabaseResponse;
import com.azure.cosmos.models.CosmosItemResponse;
import com.azure.cosmos.models.PartitionKey;
import com.azure.cosmos.models.CosmosQueryRequestOptions;
import com.azure.cosmos.models.ThroughputProperties;
import com.azure.cosmos.sample.common.AccountSettings;
import com.azure.cosmos.sample.common.Families;
import com.azure.cosmos.sample.common.Family;
import com.azure.cosmos.util.CosmosPagedFlux;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.List;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

public class AsyncMain {

    
    private static final String databaseName = "crmuDBNoSql";
    private static final String containerName = "DossierContainerID";
    private static final String containerNameLease = "DossierContainer-leases";
    private static final String containerNameAsset = "AssetContainerID";
    private static final List<String> viewName = Arrays.asList("DossierContainerMainCustomerID","DossierContainerCFPIVA","CustomerContainerCFPIVA","AssetContainerCFPIVA","DossierContainerExternalKey","AssetContainerExternalKey","AssetContainerHistory");
    private static final List<String> viewPk = Arrays.asList("/mainCustomerId","/CF_PIVA","/CF_PIVA","/CF_PIVA","/exKey","/exKey","/id");
    
    private static ChangeFeedProcessor changeFeedProcessorIstance;
    private static AtomicBoolean isProcessorRunning = new AtomicBoolean(false);

    //private CosmosDatabase database;
    private static CosmosAsyncContainer containerDossier;
    private static CosmosAsyncContainer containerAsset;
    private static CosmosAsyncContainer containerLease;

    private static CosmosAsyncClient client;

    private static String idToDelete;

    protected static Logger logger = LoggerFactory.getLogger(AsyncMain.class.getSimpleName());

    public void close() {
        client.close();
    }

    /**
     * Run a Hello CosmosDB console application.
     *
     * @param args command line args.
     */
    // <Main>
    public static void main(String[] args) {
        AsyncMain p = new AsyncMain();

        try {
            // creazione client
            client = getCosmosClient();
            System.out.println("######################################################");
            System.out.println("Creato client Cosmos: "+client);
        
            /* ######### INZIALIZZA COLLECTION ########## */
            //creazione del DB se non presente
            CosmosAsyncDatabase cosmosDatabase = createNewDatabase(client, databaseName); 
            System.out.println("######################################################");
            System.out.println("Creato Database "+databaseName+"");

            //creazione del container di scrittura
            containerDossier = createNewCollection(client, databaseName, containerName, "/id"); 
            System.out.println("######################################################");
            System.out.println("Creato Container di scrittura: "+containerName+"");

            //creazione del container di lettura per asset
            containerAsset = createNewCollection(client, databaseName, containerNameAsset, "/type"); 
            System.out.println("######################################################");
            System.out.println("Creato Container di lettura asset: "+containerNameAsset+"");

            //creazione del container di lease
            containerLease = createNewLeaseCollection(client, databaseName, containerNameLease, "/id"); 
            System.out.println("######################################################");
            System.out.println("Creato Container di lease: "+containerNameLease+"");

            //creazione viste materializzate 
            createViewCollection(client,databaseName);
            System.out.println("######################################################");
            System.out.println("Create viste materializzate");
            /* #################### FINE ##################################### */

            //Inizializza Change feeed Processor su al tro thread
            changeFeedProcessorIstance = getChangeFeedProcessor("SampleHost_1", containerDossier, containerLease);
            changeFeedProcessorIstance.start()
                .subscribeOn(Schedulers.elastic())
                .doOnSuccess(aVoid -> {
                    isProcessorRunning.set(true);
                })
                .subscribe();
            while (!isProcessorRunning.get());
            System.out.println("######################################################");
            System.out.println("Creato Change Feed Processor");

            // Inserimento di 10 documenti Json di tipo dossier
            createNewDocumentsJSON(containerDossier, Duration.ofSeconds(3));

            Thread.sleep(3000);
            
            if (changeFeedProcessorIstance != null) {
                changeFeedProcessorIstance.stop().block();
            }

            

        } catch (Exception e) {
            logger.error("Cosmos getStarted failed with", e);
        } finally {
            logger.info("Closing the client");
            p.close();
        }
    }

    public static CosmosAsyncClient getCosmosClient(){

        return new CosmosClientBuilder()
            .endpoint(AccountSettings.HOST)
            .key(AccountSettings.MASTER_KEY)
            .consistencyLevel(ConsistencyLevel.EVENTUAL)
            .contentResponseOnWriteEnabled(true)
            .buildAsyncClient();
    }

    public static CosmosAsyncDatabase createNewDatabase(CosmosAsyncClient client, String databaseName){
        client.createDatabaseIfNotExists(databaseName).block();
        return client.getDatabase(databaseName);
    }

    public static CosmosAsyncContainer createNewCollection(CosmosAsyncClient client, String databaseName, String collectionName, String partitionKey){
        CosmosAsyncDatabase databaseLink = client.getDatabase(databaseName);
        CosmosAsyncContainer collectionLink = databaseLink.getContainer(collectionName);
        CosmosContainerResponse containerResponse = null;

        try {
            containerResponse = collectionLink.read().block();

            if (containerResponse != null) {
                throw new IllegalArgumentException(String.format("Collection %s already exists in database %s.", collectionName, databaseName));
            }
        } catch (RuntimeException ex) {
            if (ex instanceof CosmosException) {
                CosmosException cosmosClientException = (CosmosException) ex;

                if (cosmosClientException.getStatusCode() != 404) {
                    throw ex;
                }
            } else {
                throw ex;
            }
        }

        CosmosContainerProperties containerSettings = new CosmosContainerProperties(collectionName, partitionKey);
        containerSettings.setDefaultTimeToLiveInSeconds(-1);
        CosmosContainerRequestOptions requestOptions = new CosmosContainerRequestOptions();
        ThroughputProperties throughputProperties = ThroughputProperties.createManualThroughput(400);
        containerResponse = databaseLink.createContainer(containerSettings, throughputProperties, requestOptions).block();
       
        if (containerResponse == null) {
            throw new RuntimeException(String.format("Failed to create collection %s in database %s.", collectionName, databaseName));
        }

        return databaseLink.getContainer(collectionName);
    }

    public static void createViewCollection(CosmosAsyncClient client, String databaseName){

        CosmosAsyncDatabase databaseLink = client.getDatabase(databaseName);
        
        for (int i = 0; i < viewName.size(); i++) {
            
            CosmosAsyncContainer collectionLink = databaseLink.getContainer(viewName.get(i));
            CosmosContainerResponse containerResponse = null;

            try {
                containerResponse = collectionLink.read().block();

                if (containerResponse != null) {
                    throw new IllegalArgumentException(String.format("Collection %s already exists in database %s.", viewName.get(i), databaseName));
                }
            } catch (RuntimeException ex) {
                if (ex instanceof CosmosException) {
                    CosmosException cosmosClientException = (CosmosException) ex;

                    if (cosmosClientException.getStatusCode() != 404) {
                        throw ex;
                    }
                } else {
                    throw ex;
                }
            }

            CosmosContainerProperties containerSettings = new CosmosContainerProperties(viewName.get(i), viewPk.get(i));
            containerSettings.setDefaultTimeToLiveInSeconds(-1);
            CosmosContainerRequestOptions requestOptions = new CosmosContainerRequestOptions();
            ThroughputProperties throughputProperties = ThroughputProperties.createManualThroughput(400);
            containerResponse = databaseLink.createContainer(containerSettings, throughputProperties, requestOptions).block();

            if (containerResponse == null) {
                throw new RuntimeException(String.format("Failed to create collection %s in database %s.", viewName.get(i), databaseName));
            }

            System.out.println("######################################################");
            System.out.println("Creata vista materializzata: "+viewName.get(i)+"");

        }       
    }

    /*
        Creazione del container Lease, necessario per tenere 
        traccia dello stato dell'app durante la lettura del feed di modifiche.
    */
    public static CosmosAsyncContainer createNewLeaseCollection(CosmosAsyncClient client, String databaseName, String leaseCollectionName, String pKey) {
        CosmosAsyncDatabase databaseLink = client.getDatabase(databaseName);
        CosmosAsyncContainer leaseCollectionLink = databaseLink.getContainer(leaseCollectionName);
        CosmosContainerResponse leaseContainerResponse = null;

        try {
            leaseContainerResponse = leaseCollectionLink.read().block();

            if (leaseContainerResponse != null) {
                leaseCollectionLink.delete().block();

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ex) {
                    ex.printStackTrace();
                }
            }
        } catch (RuntimeException ex) {
            if (ex instanceof CosmosException) {
                CosmosException cosmosClientException = (CosmosException) ex;

                if (cosmosClientException.getStatusCode() != 404) {
                    throw ex;
                }
            } else {
                throw ex;
            }
        }

        CosmosContainerProperties containerSettings = new CosmosContainerProperties(leaseCollectionName, pKey);
        CosmosContainerRequestOptions requestOptions = new CosmosContainerRequestOptions();
        ThroughputProperties throughputProperties = ThroughputProperties.createManualThroughput(400);
        leaseContainerResponse = databaseLink.createContainer(containerSettings, throughputProperties,requestOptions).block();

        if (leaseContainerResponse == null) {
            throw new RuntimeException(String.format("Failed to create collection %s in database %s.", leaseCollectionName, databaseName));
        }

        return databaseLink.getContainer(leaseCollectionName);
    }
   

    /* Funzione di callback del Change Feed Processor 
       Esegue il mirroring dei documenti Json inserite nella collection Dossier di scrittra
       nella vista materializzata AssetID
    */

    public static ChangeFeedProcessor getChangeFeedProcessor(String hostName, CosmosAsyncContainer feedContainer, CosmosAsyncContainer leaseContainer){

        ChangeFeedProcessorOptions cfOptions = new ChangeFeedProcessorOptions();
        cfOptions.setFeedPollDelay(Duration.ofMillis(100)); // 100 millisecondi di latenza per effettuare il mirroring
        cfOptions.setStartFromBeginning(true);
        return new ChangeFeedProcessorBuilder()
            .options(cfOptions)
            .hostName(hostName)
            .feedContainer(feedContainer)
            .leaseContainer(leaseContainer)
            .handleChanges((List<JsonNode> docs) -> {
                for(JsonNode document : docs) {
                    // duplica ogni documento inserito nel DossierContainer all'interno della vista Asset
                    updateInventoryTypeMaterializedView(document);
                }
            })
            .buildChangeFeedProcessor();
    }

    private static void updateInventoryTypeMaterializedView(JsonNode document) {

        containerAsset.upsertItem(document).subscribe();
    }

    public static void createNewDocumentsJSON(CosmosAsyncContainer containerClient, Duration delay) {

        List<String> brands = Arrays.asList("Jerry's","Baker's Ridge Farms","Exporters Inc.","WriteSmart","Stationary","L. Alfred","Haberford's","Drink-smart","Polaid","Choice Dairy");
        List<String> types = Arrays.asList("plums","ice cream","espresso","pens","stationery","cheese","cheese","kool-aid","water","milk");
        List<String> quantities = Arrays.asList("50","15","5","10","5","6","4","50","100","20");

        for (int i = 0; i < brands.size(); i++) {

            String id = UUID.randomUUID().toString();
            if (i==0) idToDelete=id;

            String jsonString =    "{\"id\" : \"" + id + "\""
                                 + ","
                                 + "\"brand\" : \"" + brands.get(i) + "\""
                                 + ","
                                 + "\"type\" : \"" + types.get(i) + "\""
                                 + ","
                                 + "\"quantity\" : \"" + quantities.get(i) + "\""
                                 + "}";

            ObjectMapper mapper = new ObjectMapper();
            JsonNode document = null;

            try {
                document = mapper.readTree(jsonString);
            } catch (Exception e) {
                e.printStackTrace();
            }

            containerClient.createItem(document).subscribe(doc -> {
                System.out.println(".\n");
            });

            long remainingWork = delay.toMillis();
            try {
                while (remainingWork > 0) {
                    Thread.sleep(100);
                    remainingWork -= 100;
                }
            } catch (InterruptedException iex) {
                // exception caught
                break;
            }
        }
    }
}
