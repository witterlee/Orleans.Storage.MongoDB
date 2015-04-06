using System;
using System.Collections;
using System.Linq;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using MongoDB.Driver.Linq;
using Newtonsoft.Json;
using Orleans.Providers;
using Orleans.Runtime;
using MongoDBBson = MongoDB.Bson;
using System.Collections.Concurrent;


namespace Orleans.Storage.MongoDB
{
    /// <summary>
    /// A MongoDB storage provider.
    /// </summary>
    /// <remarks>
    /// The storage provider should be included in a deployment by adding this line to the Orleans server configuration file:
    /// 
    ///      <Provider Type="Orleans.Storage.MongoDB.MongoDBStorage" Name="MongoDBStore" Database="db-name" ConnectionString="mongodb://YOURHOSTNAME:27017/" />
    /// and this line to any grain that uses it:
    /// 
    ///     [StorageProvider(ProviderName = "MongoDBStore")]
    /// 
    /// The name 'MongoDBStore' is an arbitrary choice.
    /// </remarks>
    public class MongoDBStorage : IStorageProvider
    {
        private const string DATA_CONNECTION_STRING = "ConnectionString";
        private const string DATABASE_NAME_PROPERTY = "Database";
        //private const string DELETE_ON_CLEAR_PROPERTY = "DeleteStateOnClear";
        private const string USE_GUID_AS_STORAGE_KEY = "UseGuidAsStorageKey";

        /// <summary>
        /// Logger object
        /// </summary>
        public Logger Log { get; protected set; }
        /// <summary>
        /// Database name
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Database connection string
        /// </summary>
        public string ConnectionString { get; set; }

        /// <summary>
        /// Database name
        /// </summary>
        public string Database { get; set; }

        /// <summary>
        /// use grain's guid key as the storage key,default is true
        /// <remarks>default is true,use guid as the storeage key, else false use like GrainReference=40011c8c7bcc4141b3569464533a06a203ffffff9c20d2b7 as the key </remarks>
        /// </summary>
        public bool UseGuidAsStorageKey { get; protected set; }

        /// <summary>
        /// Initializes the storage provider.
        /// </summary>
        /// <param name="name">The name of this provider instance.</param>
        /// <param name="providerRuntime">A Orleans runtime object managing all storage providers.</param>
        /// <param name="config">Configuration info for this provider instance.</param>
        /// <returns>Completion promise for this operation.</returns>
        public Task Init(string name, IProviderRuntime providerRuntime, IProviderConfiguration config)
        {
            Log = providerRuntime.GetLogger(this.GetType().FullName);

            this.Name = name;

            if (!config.Properties.ContainsKey(DATA_CONNECTION_STRING) ||
                !config.Properties.ContainsKey(DATABASE_NAME_PROPERTY))
            {
                throw new ArgumentException("ConnectionString Or Database property not set");
            }

            this.ConnectionString = config.Properties[DATA_CONNECTION_STRING];
            this.Database = config.Properties[DATABASE_NAME_PROPERTY];

            this.UseGuidAsStorageKey = !config.Properties.ContainsKey(USE_GUID_AS_STORAGE_KEY) ||
                                       "true".Equals(config.Properties[USE_GUID_AS_STORAGE_KEY],
                                           StringComparison.OrdinalIgnoreCase);

            DataManager = new GrainStateMongoDataManager(Database, ConnectionString);

            return TaskDone.Done;
        }
        private GrainStateMongoDataManager DataManager { get; set; }
        /// <summary>
        /// Closes the storage provider during silo shutdown.
        /// </summary>
        /// <returns>Completion promise for this operation.</returns>
        public Task Close()
        {
            DataManager = null;
            return TaskDone.Done;
        }

        /// <summary>
        /// Reads persisted state from the backing store and deserializes it into the the target
        /// grain state object.
        /// </summary>
        /// <param name="grainType">A string holding the name of the grain class.</param>
        /// <param name="grainReference">Represents the long-lived identity of the grain.</param>
        /// <param name="grainState">A reference to an object to hold the persisted state of the grain.</param>
        /// <returns>Completion promise for this operation.</returns>
        public async Task ReadStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            if (DataManager == null) throw new ArgumentException("DataManager property not initialized");

            string extendKey;

            var key = this.UseGuidAsStorageKey ? grainReference.GetPrimaryKey(out extendKey).ToString() : grainReference.ToKeyString();

            var entityData = await DataManager.ReadAsync(grainType, key);

            if (!string.IsNullOrEmpty(entityData))
            {
                ConvertFromStorageFormat(grainState, entityData);
            }
        }

        /// <summary>
        /// Writes the persisted state from a grain state object into its backing store.
        /// </summary>
        /// <param name="grainType">A string holding the name of the grain class.</param>
        /// <param name="grainReference">Represents the long-lived identity of the grain.</param>
        /// <param name="grainState">A reference to an object holding the persisted state of the grain.</param>
        /// <returns>Completion promise for this operation.</returns>
        public Task WriteStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            if (DataManager == null) throw new ArgumentException("DataManager property not initialized");

            string extendKey;

            var key = this.UseGuidAsStorageKey ? grainReference.GetPrimaryKey(out extendKey).ToString() : grainReference.ToKeyString();

            return DataManager.WriteAsync(grainType, key, grainState);
        }

        /// <summary>
        /// Removes grain state from its backing store, if found.
        /// </summary>
        /// <param name="grainType">A string holding the name of the grain class.</param>
        /// <param name="grainReference">Represents the long-lived identity of the grain.</param>
        /// <param name="grainState">An object holding the persisted state of the grain.</param>
        /// <returns></returns>
        public Task ClearStateAsync(string grainType, GrainReference grainReference, IGrainState grainState)
        {
            if (DataManager == null) throw new ArgumentException("DataManager property not initialized");

            string extendKey;

            var key = this.UseGuidAsStorageKey ? grainReference.GetPrimaryKey(out extendKey).ToString() : grainReference.ToKeyString();

            return DataManager.DeleteAsync(grainType, key);
        }

        /// <summary>
        /// Constructs a grain state instance by deserializing a JSON document.
        /// </summary>
        /// <param name="grainState">Grain state to be populated for storage.</param>
        /// <param name="entityData">JSON storage format representaiton of the grain state.</param>
        protected static void ConvertFromStorageFormat(IGrainState grainState, string entityData)
        {
            object data = JsonConvert.DeserializeObject(entityData, grainState.GetType());
            var dict = ((IGrainState)data).AsDictionary();
            grainState.SetAll(dict);
        }
    }

    /// <summary>
    /// Interfaces with a MongoDB database driver.
    /// </summary>
    internal class GrainStateMongoDataManager
    {
        private static ConcurrentDictionary<string, bool> registerIndexMap = new ConcurrentDictionary<string, bool>();
        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="connectionString">A database name.</param>
        /// <param name="databaseName">A MongoDB database connection string.</param>
        public GrainStateMongoDataManager(string databaseName, string connectionString)
        {
            MongoClient client = new MongoClient(connectionString);
            _database = client.GetDatabase(databaseName);
        }

        /// <summary>
        /// Deletes a file representing a grain state object.
        /// </summary>
        /// <param name="collectionName">The type of the grain state object.</param>
        /// <param name="key">The grain id string.</param>
        /// <returns>Completion promise for this operation.</returns>
        public Task DeleteAsync(string collectionName, string key)
        {
            var collection = _database.GetCollection<BsonDocument>(collectionName);
            if (collection == null)
                return TaskDone.Done;

            var query = BsonDocument.Parse("{key:\"" + key + "\"}");
            collection.FindOneAndDeleteAsync(query);

            return TaskDone.Done;
        }

        /// <summary>
        /// Reads a file representing a grain state object.
        /// </summary>
        /// <param name="collectionName">The type of the grain state object.</param>
        /// <param name="key">The grain id string.</param>
        /// <returns>Completion promise for this operation.</returns>
        public async Task<string> ReadAsync(string collectionName, string key)
        {
            var collection = await GetCollection(collectionName);

            if (collection == null)
                return null;

            var query = BsonDocument.Parse("{__key:\"" + key + "\"}");
            using (var cursor = await collection.FindAsync(query))
            {
                var existing = (await cursor.ToListAsync()).FirstOrDefault();

                if (existing == null)
                    return null;

                existing.Remove("_id");
                existing.Remove("__key");

                return existing.ToJson();
            }
        }

        /// <summary>
        /// Writes a file representing a grain state object.
        /// </summary>
        /// <param name="collectionName">The type of the grain state object.</param>
        /// <param name="key">The grain id string.</param>
        /// <param name="entityData">The grain state data to be stored./</param>
        /// <returns>Completion promise for this operation.</returns>
        public async Task WriteAsync(string collectionName, string key, IGrainState entityData)
        {
            var collection = await GetCollection(collectionName);

            var query = BsonDocument.Parse("{__key:\"" + key + "\"}");

            using (var cursor = await collection.FindAsync(query))
            {
                var existing = (await cursor.ToListAsync()).FirstOrDefault();

                var json = JsonConvert.SerializeObject(entityData);

                var doc = BsonSerializer.Deserialize<BsonDocument>(json);
                doc["__key"] = key;

                if (existing != null)
                {
                    doc["_id"] = existing["_id"];
                    await collection.ReplaceOneAsync(query, doc);

                }
                else
                {
                    await collection.InsertOneAsync(doc);
                }
            }
        }

        private async Task<IMongoCollection<MongoDBBson.BsonDocument>> GetCollection(string name)
        {
            var collection = _database.GetCollection<MongoDBBson.BsonDocument>(name);

            if (!registerIndexMap.ContainsKey(name))
            {
                using (var cursor = await collection.Indexes.ListAsync())
                {
                    var indexes = await cursor.ToListAsync();
                    if (indexes.Count(index => index["name"] == "__key_1") == 0)
                    {
                        var keys = Builders<MongoDBBson.BsonDocument>.IndexKeys.Ascending("__key");
                        await collection.Indexes.CreateOneAsync(keys,
                            new CreateIndexOptions() { Unique = true, Version = 1 });
                    }
                    registerIndexMap.TryAdd(name, true);
                }
            }
            return collection;
        }

        private readonly IMongoDatabase _database;
    }
}
