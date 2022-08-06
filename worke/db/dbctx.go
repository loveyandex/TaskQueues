package worke

const dbName = "rabitmq"
const collectionName = "user" 
var UserCollection, _ = GetMongoDbCollection(dbName, collectionName)  
  
