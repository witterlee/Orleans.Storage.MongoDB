# Orleans.Storage.MongoDB
   Orleans Storage MongoDB Provider

## USE CASE 

###### 1. ServerConfiguration.xml
```xml
<?xml version="1.0" encoding="utf-8"?>
<OrleansConfiguration xmlns="urn:orleans">
  <Globals>
    <StorageProviders>
      <Provider Type="Orleans.Storage.MemoryStorage" Name="MemoryStore" />
     <Provider Type="Orleans.Storage.MongoDB.MongoDBStorage" Name="MongoDBStorage" UseGuidAsStorageKey="True/False" ConfigSectionName="couchbaseClients/couchbaseDataStore" />
    </StorageProviders>
    <SeedNode Address="localhost" Port="11111" />
  </Globals>
</OrleansConfiguration>

```
###### 2. Code
```csharp
 [StorageProvider(ProviderName = "MongoDBStorage")]
 public class BankAccount : IGrain,IBankAccount
 {
 }
```
