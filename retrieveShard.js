global=sh.getShardedDataDistribution().toArray();
shard={}
shard["globalDistribution"]=global
fs.writeFileSync(
   'inputs/getShardedDataDistribution.json',
   EJSON.stringify(global, null, 2 )
)
shard["distribution_per_ns"]={}
distribution_per_ns=shard["distribution_per_ns"]
global.forEach(ns => {
    // Log the namespace being processed
    console.log(`Processing namespace: ${ns.ns}`);

     const [dbName, collectionName] = ns.ns.split('.');
     // skip sys and test DB
     if (dbName === "local"||dbName === "config"||dbName === "admin"||dbName === "test") return;
     // Run getShardDistribution on the current collection
     const distribution = db.getSiblingDB(dbName)[collectionName].getShardDistribution();
     distribution_per_ns[ns.ns]=distribution
     fs.writeFileSync(
         `inputs/getShardedDataDistribution_${ns.ns}.json`,
          EJSON.stringify(distribution, null, 2 )
     )

  });

fs.writeFileSync(
   'inputs/serverStatus.json',
   EJSON.stringify(db.serverStatus(), null, 2 )
)

fs.writeFileSync(
   'inputs/Sharding_distribution.json',
   EJSON.stringify(shard, null, 2 )
)

