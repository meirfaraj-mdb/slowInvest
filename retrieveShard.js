global=sh.getShardedDataDistribution().toArray();
fs.writeFileSync(
   'inputs/getShardedDataDistribution.json',
   EJSON.stringify(global, null, 2 )
)

global.forEach(ns => {
    // Log the namespace being processed
    console.log(`Processing namespace: ${ns.ns}`);

     const [dbName, collectionName] = ns.ns.split('.');
     // skip sys and test DB
     if (dbName === "local"||dbName === "config"||dbName === "admin"||dbName === "test") return;
     // Run getShardDistribution on the current collection
     const distribution = db.getSiblingDB(dbName)[collectionName].getShardDistribution();

     fs.writeFileSync(
         `inputs/getShardedDataDistribution_${ns.ns}.json`,
          EJSON.stringify(distribution, null, 2 )
     )

  });

fs.writeFileSync(
   'inputs/serverStatus.json',
   EJSON.stringify(db.serverStatus(), null, 2 )
)
