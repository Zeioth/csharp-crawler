using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using MongoDB.Driver.Builders;
using SharedLibrary.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SharedLibrary.MongoDB
{
    public class MongoDBWrapper
    {
        #region ** Private Attributes **

        private string           _connString;
        private string           _collectionName;
        private MongoServer      _server;
        private MongoDatabase    _database;

        private string           _entity;

        #endregion

        /// <summary>
        /// Executes the configuration needed in order to start using MongoDB
        /// </summary>
        /// <param name="username">Username</param>
        /// <param name="password">User Password</param>
        /// <param name="authSrc">Database used as Authentication Provider. Default is 'admin'</param>
        /// <param name="serverAddress">IP Address of the DB Server. Format = ip:port</param>
        /// <param name="timeout">Connection Timeout</param>
        /// <param name="databaseName">Database Name</param>
        /// <param name="collectionName">Collection Name</param>
        /// <param name="entity">Not Used. Use Empty</param>
        public void ConfigureDatabase (string username, string password, string authSrc, string serverAddress, int timeout, string databaseName, string collectionName, string entity = "")
        {
            // Reading App Config for config data            
            _connString     = MongoDbContext.BuildConnectionString (username, password, authSrc, true, false, serverAddress, timeout, timeout);                        
            _server         = new MongoClient (_connString).GetServer ();
            _database       = _server.GetDatabase (databaseName);
            _collectionName = collectionName;
            _entity         = entity;          
        }

        /// <summary>
        /// Inserts an element to the MongoDB.
        /// The type T must match the type of the target collection
        /// </summary>
        /// <typeparam name="T">Type of the object to be inserted</typeparam>
        /// <param name="record">Record that will be inserted in the database</param>
        public bool Insert<T> (T record, string collection = "")
        {
            collection = String.IsNullOrEmpty (collection) ? _collectionName : collection;

            return _database.GetCollection<T> (collection).SafeInsert (record);
        }

        public bool UpdateRecord (AppModel record, string attribute, string key, string collection = "")
        {
            collection = String.IsNullOrEmpty (collection) ? _collectionName : collection;

            // Find Query
            var mongoQuery = Query.EQ (attribute, key);

            // Finding Record to retrieve it's old Object ID
            AppModel oldRecord = FindMatch<AppModel> (mongoQuery).FirstOrDefault();

            // Re-setting Object ID of the object
            record._id = oldRecord._id;

            // Update Query
            var updateQuery = Update.Replace (record);

            return _database.GetCollection<AppModel> (collection).Update (mongoQuery, updateQuery).Ok;
        }

        /// <summary>
        /// Finds all the record of a certain collection, of a certain type T.
        /// </summary>
        /// <typeparam name="T">Type of the model that maps this collection</typeparam>
        /// <returns>IEnumerable of the type selected</returns>
        public IEnumerable<T> FindAll<T> ()
        {
            return _database.GetCollection<T> (_collectionName).FindAll ().SetFlags(QueryFlags.NoCursorTimeout);
        }

        public IEnumerable<T> FindMatch<T> (IMongoQuery mongoQuery, int limit = -1, int skip = 0, string collectionName = "")
        {
           collectionName = String.IsNullOrEmpty (collectionName) ? _collectionName : collectionName;

           if (limit != -1)
           {
               return _database.GetCollection<T> (collectionName).Find (mongoQuery).SetFlags (QueryFlags.NoCursorTimeout).SetLimit (limit).SetSkip (skip);
           }
           else
           {
               return _database.GetCollection<T> (collectionName).Find (mongoQuery).SetFlags (QueryFlags.NoCursorTimeout).SetSkip (skip);
           }
        }

        public IEnumerable<String> FindPeopleUrls ()
        {
            return _database.GetCollection<AppReview> (Consts.REVIEWS_COLLECTION).FindAll ().Select (t => t.authorUrl);
        }

        /// <summary>
        /// Checks whether an app with the same URL
        /// already exists into the database
        /// </summary>
        /// <param name="appUrl">Url of the App</param>
        /// <returns>True if the app exists into the database, false otherwise</returns>
        public bool AppProcessed (string appUrl)
        {
            var mongoQuery = Query.EQ ("Url", appUrl);

            var queryResponse = _database.GetCollection<AppModel> (_collectionName).FindOne (mongoQuery);

            return queryResponse == null ? false : true;
        }

        /// <summary>
        /// Checks whether the received app url is on the queue collection
        /// to be processed or not
        /// </summary>
        /// <param name="appUrl">Url of the app</param>
        /// <returns>True if it is on the queue collection, false otherwise</returns>
        public bool AppQueued (string appUrl)
        {
            var mongoQuery = Query.EQ ("Url", appUrl);

            var queryResponse = _database.GetCollection<QueuedApp> (Consts.QUEUED_APPS_COLLECTION).FindOne (mongoQuery);

            return queryResponse == null ? false : true;
        }

        /// <summary>
        /// Checks whether the received URL is on the queue to be processed already
        /// or not
        /// </summary>
        /// <param name="appUrl">URL (Key for the search)</param>
        /// <returns>True if the app is on the queue collection, false otherwise</returns>
        public bool IsAppOnQueue (string appUrl)
        {
            var mongoQuery    = Query.EQ ("Url", appUrl);

            var queryResponse = _database.GetCollection<QueuedApp> (Consts.QUEUED_APPS_COLLECTION).FindOne (mongoQuery);

            return queryResponse == null ? false : true;
        }

        public bool IsReviewerOnDatabase (string reviewerUrl)
        {
            var mongoQuery = Query.EQ ("reviewerUrl", reviewerUrl);

            var queryResponse = _database.GetCollection<ReviewerPageData> (Consts.REVIEWERS_COLLECTION).FindOne (mongoQuery);

            return queryResponse == null ? false : true;
        }

        /// <summary>
        /// Adds the received url to the collection
        /// of queued apps
        /// </summary>
        /// <param name="appUrl">Url of the app</param>
        /// <returns>Operation status. True if worked, false otherwise</returns>
        public bool AddToQueue (string appUrl)
        {
            return _database.GetCollection<QueuedApp> (Consts.QUEUED_APPS_COLLECTION).SafeInsert (new QueuedApp { Url = appUrl, IsBusy = false});
        }

        /// <summary>
        /// Finds an app that is "Not Busy" and modifies it's status
        /// to "Busy" atomically so that no other worker will try to process it
        /// on the same time
        /// </summary>
        /// <returns>Found app, if any</returns>
        public QueuedApp FindAndModify ()
        {
            // Mongo Query
            var mongoQuery      = Query.EQ ("IsBusy", false);
            var updateStatement = Update.Set ("IsBusy", true);

            // Finding a Not Busy App, and updating its state to busy
            var mongoResponse = _database.GetCollection<QueuedApp> (Consts.QUEUED_APPS_COLLECTION).FindAndModify (mongoQuery, null, updateStatement, false);

            // Checking for query error or no app found
            if (mongoResponse == null || mongoResponse.Response == null)
            {
                return null;
            }

            // Returns the app
            return BsonSerializer.Deserialize<QueuedApp> (mongoResponse.ModifiedDocument);
        }

        /// <summary>
        /// Toggles the status of the "IsBusy" attribute of the queued app
        /// </summary>
        /// <param name="app">App to be found in the collection</param>
        /// <param name="busyStatus">New Busy status</param>
        public void ToggleBusyApp (QueuedApp app, bool busyStatus)
        {
            // Mongo Query
            var mongoQuery      = Query.EQ ("Url", app.Url);
            var updateStatement = Update.Set ("IsBusy", busyStatus);

            _database.GetCollection<QueuedApp> (Consts.QUEUED_APPS_COLLECTION).Update (mongoQuery, updateStatement);
        }

        /// <summary>
        /// Removes the received app from the collection
        /// of queued apps
        /// </summary>
        /// <param name="url">App document to be removed</param>
        public void RemoveFromQueue (string url)
        {
            var mongoQuery = Query.EQ ("Url", url);
            _database.GetCollection<QueuedApp> (Consts.QUEUED_APPS_COLLECTION).Remove (mongoQuery);
        }

        public void EnsureIndex (string fieldName, string collectionName = null)
        {
            string collection = collectionName == null ? _collectionName : collectionName;
            _database.GetCollection (collection).CreateIndex (IndexKeys.Ascending (fieldName), IndexOptions.SetBackground (true));
        }

        public void SetUpdated (string url)
        {
            var query = Query.EQ("Url", url);

            _database.GetCollection (_collectionName).Update (query, Update.Set("Uploaded", true));
        }

        /// <summary>
        /// Finds a game that is "Not Busy" and modifies it's status.
        /// to "Busy" atomically so that no other worker will try to process it
        /// on the same time
        /// </summary>
        /// <returns>Found game, if any</returns>
        public QueuedApp GetFromGamesQueue()
        {
            // Mongo Query
            var mongoQuery = Query.EQ("IsBusy", false);
            var updateStatement = Update.Set("IsBusy", true);

            // Finding a Not Busy Game, and updating its state to busy
            var mongoResponse = _database.GetCollection<QueuedApp>(Consts.QUEUED_GAMES_COLLECTION).FindAndModify(mongoQuery, null, updateStatement, false);

            // Checking for query error or no app found
            if (mongoResponse == null || mongoResponse.Response == null)
            {
                return null;
            }
            try
            {
                // Returns the app
                return BsonSerializer.Deserialize<QueuedApp>(mongoResponse.ModifiedDocument);
            }
            catch
            { 
                throw new Exception("ERROR -> QUEUE IS EMPTY. Fill it before lauch the application."); 
            }
        }

        /// <summary>
        /// Checks whether an Game with the same URL
        /// already exists into the GamesData collection and have been updated.
        /// </summary>
        /// <param name="appUrl">Url of the App</param>
        /// <returns>True if the app exists into GamesData and have not been updated yet, false otherwise</returns>
        public bool GameIsUpdated(string appUrl)
        {
            // QUERY POR TESTEAR
            var mongoQuery = Query.And(Query.EQ("Url", appUrl), Query.EQ("ReferenceDate", DateTime.Today));

            var queryResponse = _database.GetCollection<AppModel>(_collectionName).FindOne(mongoQuery);

            // Si la consulta devuelve algo, esque ha sido procesada
            return queryResponse == null ? false : true;
        }

        /// <summary>
        /// Por cada juego de AppsData cuya categoria esté en la constante QUEUED_GAMES_CATEGORIES
        /// e comprueba que no haya sido procesado y no exista ya en la cola. 
        /// Si pasa la comprobacion, insertamos en QueuedGames.
        /// </summary>
        /// <returns>Number of games added to the queue.</returns>
        public int PopulateQueuedGames()
        {
            int nQueuedGames = 0;
            QueuedApp qa = new QueuedApp();

            foreach (string category in Consts.QUEUED_GAMES_CATEGORIES)
            {


                // Get all Apps in the category
                var mongoQuery = Query.EQ("Category", category);
                var response = _database.GetCollection<AppModel>(Consts.MONGO_COLLECTION).Find(mongoQuery);

                foreach (var document in response)
                {


                    // check if document is already queued
                    var mongoQuery2 = Query.EQ("Url", document.Url);
                    var response2 = _database.GetCollection<QueuedApp>(Consts.QUEUED_GAMES_COLLECTION).Find(mongoQuery2);
                    bool isAlreadyQueued = (response2 == null ? true : false);

                    Console.WriteLine(isAlreadyQueued);





                    // Inser a game if does not exists in GamesQueue or GamesData.
                    // Since URL is our collection index, it will not add duplicates.
                    if(!GameIsUpdated(document.Url) && !isAlreadyQueued)
                    {
                         qa.IsBusy = false;
                         qa.Url = document.Url;

                         _database.GetCollection<QueuedApp>(Consts.QUEUED_GAMES_COLLECTION).SafeInsert(qa);
                         nQueuedGames++;
                    }
                }
            }
            return nQueuedGames;
        }
    }
}
