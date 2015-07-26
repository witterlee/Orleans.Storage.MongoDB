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
      <Provider Type="Orleans.Storage.MongoDB.MongoDBStorage" Name="MongoDBStorage" Database="Orleans" ConnectionString="mongodb://localhost:27017/" />
    </StorageProviders>
    <SeedNode Address="localhost" Port="11111" />
  </Globals>
</OrleansConfiguration>
```

If you have MongoDB running on your local machine, then this should work with no modifications.

If you have MongoDB running on a remote machine, then you will have to update "localhost" to match the machine name.

###### 2. Code
```csharp
 [StorageProvider(ProviderName = "MongoDBStorage")]
 public class BankAccount : IGrain,IBankAccount
 {
 }
```
