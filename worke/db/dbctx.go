package worke

const dbName = "rabitmq"
const collectionName = "user2" 
var UserCollection, _ = GetMongoDbCollection(dbName, collectionName)  
  
