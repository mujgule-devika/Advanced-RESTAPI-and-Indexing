const express = require("express");
const app = express();
const fs = require("fs");
// Constants
const WORK_QUEUE = "WORK_QUEUE";
const BACKUP_QUEUE = "BACKUP_QUEUE";
const REDIS_PORT = 6379;
const INDEX_NAME = "planindex";

var Redis = require('ioredis');
var redisClient1 = new Redis();
var Redis = require('redis');
const redisClient = Redis.createClient(REDIS_PORT);

const { promisify } = require("util");
const hgetallAsync = promisify(redisClient.hgetall).bind(redisClient);
const typeAsync = promisify(redisClient.type).bind(redisClient);
const smembersAsync = promisify(redisClient.smembers).bind(redisClient);
const CERT_PATH = process.env.CERT_PATH || "/Users/devikamujgule/INFO7255/elastic/elasticsearch-8.5.2/config/certs"

var elasticsearch = require("@elastic/elasticsearch");
// Elastic Search
var elasticClient = new elasticsearch.Client({
  node: "https://localhost:9200",
  log: 'info',
  auth: {
    username: 'elastic',
    password: 'yNeLJ_guf8uC0RseeJ_Q'
  },
  tls: {
    ca: fs.readFileSync(CERT_PATH + '/http_ca.crt'),
    rejectUnauthorized: false
  }

}, (err, res, status) => {
  if (err) {
    console.log(`errored out`, err);
  } else {
    return res.status(200).send({
      message: 'aOK',
      body: body
    })
  }
});

const setup = async () => {

  // deleting the index from previous run
  try {
    const promise_delete = await elasticClient.indices.delete({
      index: INDEX_NAME,
    })
    console.log("Deleted index ", promise_delete)
  } catch (error) {
    console.log("Error deleting index", error)
  }
  // creating the index for new run with mapping and settings
  try {
    const promise_create = await elasticClient.indices.create({
      index: INDEX_NAME,
      settings: {
        index: {
          number_of_shards: 1,
          number_of_replicas: 1
        }
      },
      body: {
        mappings: {
          properties: {
            plan_service: {
              type: 'join',
              relations: {
                plan: ["planCostShares", "linkedPlanServices"],
                linkedPlanServices: ["linkedService", "planserviceCostShares"]
              }
            }
          }
        }
      }
    })
    console.log("Created index", promise_create)
  } catch (error) {
    console.log("Error creating index", error)
  }
}

const delete_data = async () => {
  let del_queue_size = 0
  let del_id;
  try {
    const size = await elasticClient.LLEN("queue-del");
    del_queue_size = size;
  } catch (error) {
    console.log("Error getting del-queue size", error);
  }
  if (del_queue_size > 0) {
    console.log("Delete queue size", del_queue_size)
    try {
      let el_id = await pop_del_queue();
      if (el_id != null || el_id != undefined) {
        el_id = el_id.replace(/^"|"$/g, '')
        del_id = el_id;
        if (/^\d+$/.test(el_id)) {
          return;
        }
        // console.log("el_id",el_id)
        const elastic_delete = await elastic_client.delete({
          index: 'plan',
          id: el_id
        })
        // console.log("Deleted from elastic", del_id)
      }
    } catch (error) {
      console.log("Error deleting from elastic", error)
      if (!/^\d+$/.test(del_id)) {
        const promise = await redis_utils.add_del_queue(del_id);
      }
    }
  }
}

var redisLoop = function () {

  redisClient1.brpoplpush(WORK_QUEUE, BACKUP_QUEUE, 0).then(function (plan_id) {
      // because we are using BRPOPLPUSH, the client promise will not resolve
      // until a 'result' becomes available
      processJob(plan_id);
      // delete the item from the working channel, and check for another item
      redisClient1.lrem(BACKUP_QUEUE, 1, plan_id).then(redisLoop());
  });
};
redisLoop();



async function processJob(planId) {
  let id = planId.split("_")[1];
  console.log(`inside process job`, planId, id);
  console.log(`planId.includes('delete') `, planId.includes('delete'));

  if (planId.includes('delete')) {
    await setup(); 
  } else if (!planId.includes('delete')) {
    let resJSON = {};
    await recreateJSON(planId, resJSON);
    console.log(`value of resJSON`, resJSON);
    indexPlan(id, resJSON);
  }
}

async function indexPlan(id, plan) {
  console.log("Starting to index plan id ==> " + id);
  console.log(`value of plan data`, plan);

  plan['plan_service'] = { name: "plan" };
  console.log(`value of plan['plan_service']`, plan['plan_service']);


  let planCostShares = plan['planCostShares'];
  let linkedPlanServices = plan['linkedPlanServices'];

  delete plan['planCostShares'];
  delete plan['linkedPlanServices'];
try {
  await indexPartialPlan(id, plan);
  console.log(`planCostShares['plan_service']`, planCostShares['plan_service']);

  planCostShares['plan_service'] = {
    name: "planCostShares",
    parent: plan.objectId
  };
  console.log(`after planCostShares['plan_service']`, planCostShares['plan_service']);
} catch(err) {
  console.log(`errored out at 175`);
  
}
  await indexPartialPlan(planCostShares.objectId, planCostShares, plan.objectId);

  for (let i = 0; i < linkedPlanServices.length; i++) {
    let planservice = linkedPlanServices[i];

    let linkedService = planservice['linkedService'];
    let planserviceCostShares = planservice['planserviceCostShares'];

    delete planservice['linkedService'];
    delete planservice['planserviceCostShares'];

    planservice['plan_service'] = {
      name: "linkedPlanServices",
      parent: plan.objectId
    };

    await indexPartialPlan(planservice.objectId, planservice, plan.objectId);

    linkedService['plan_service'] = {
      name: "linkedService",
      parent: planservice.objectId
    };

    await indexPartialPlan(linkedService.objectId, linkedService, planservice.objectId);


    planserviceCostShares['plan_service'] = {
      name: "planserviceCostShares",
      parent: planservice.objectId
    };

    await indexPartialPlan(planserviceCostShares.objectId, planserviceCostShares, planservice.objectId);
    console.log("Indexed successfully", planservice, linkedService, planserviceCostShares);
  }

}

async function indexPartialPlan(id, body, route) {
  console.log(`inside index partial`);
  console.log(`plan id before indextin`, id);

  await elasticClient.index({
    index: INDEX_NAME,
    id: id,
    body: body,
    routing: route,
    document: body
  }
    , (err, res, status) => {
      if (err) {
        return console.log(`errored out`, err);
      } else {
        return res.status(200).send({
          message: 'OK',
          body: body
        })
      }
    });
}


const listening = async () => {
  let queue_1_size = 0
  let queue_2_size = 0
  try {
    const size = await redis_client.LLEN("queue-1");
    queue_1_size = size;
  } catch (err) {
    console.log("Error getting queue-1 size", err);
  }
  try {
    const size = await redis_client.LLEN("queue-2");
    queue_2_size = size;
  } catch (err) {
    console.log("Error getting queue-2 size", err);
  }
  // checking if any element is left in queue-2 because of any error
  for (let i = 0; i < queue_2_size; i++) {
    console.log("Entering queue 2(backup queue) and queue 2 size is: " + `${queue_2_size}`)
    const data = await redis_utils.pop_queue_2();
    // adding the element back to queue-1
    const promise = await redis_utils.add_queue(data);
    // let parsed_data = JSON.parse(data);
    // parsed_data = JSON.parse(parsed_data);
    // delete parsed_data["children"];
    // const elastic_result = await elastic_client.index({
    //   index: 'plan',
    //   id: parsed_data.Objectid,
    //   document: parsed_data
    // })
    console.log("processed element from queue-2 and pushed to elastic")
  }
  if (queue_1_size > 0) {
    console.log("Queue size: ", queue_1_size);
    // removing the element from queue-1 and pushing it to queue-2 using BRPOPLPUSH
    try {
      const data = await redis_utils.pop_queue_1();
      if (data != null) {
        let parsed_data = JSON.parse(data);
        parsed_data = JSON.parse(parsed_data);
        delete parsed_data["children"];
        // not to process list with digit id
        if (/^\d+$/.test(parsed_data["objectId"])) {
          const data_2 = await redis_utils.pop_queue_2();
          return;
        }
        const parent_id = parsed_data["parent_id"];
        delete parsed_data["parent_id"];
        const object_name = parsed_data["object_name"];
        delete parsed_data["object_name"];
        if (object_name == "plan") {
          parsed_data["mapping"] = "plan";
        } else if (object_name == "planserviceCostShares") {
          parsed_data["mapping"] = {
            name: "planserviceCostShares",
            parent: parent_id
          }
        } else if (object_name == "linkedService") {
          parsed_data["mapping"] = {
            name: "linkedService",
            parent: parent_id
          }
        } else if (object_name == "planCostShares") {
          parsed_data["mapping"] = {
            name: "planCostShares",
            parent: parent_id
          }
        } else if (object_name == "linkedPlanServices") {
          parsed_data["mapping"] = {
            name: "linkedPlanServices",
            parent: parent_id
          }
        } else if (object_name == "planCostShares") {
          parsed_data["mapping"] = {
            name: "planCostShares",
            parent: parent_id
          }
        } else if (object_name == null) {
          parsed_data["mapping"] = "plan";
        }

        // console.log(parsed_data)
        const elastic_result = await elastic_client.index({
          index: 'plan',
          id: parsed_data["objectId"],
          routing: parent_id,
          document: parsed_data
        })
        // console.log(elastic_result)
        // removing the element from queue-2 since it has been successfully processed
        const data_2 = await redis_utils.pop_queue_2();
      } else {
        console.log("data is null")
      }
    } catch (error) {
      console.log("Error pushing to elastic search", error)
    }
  }
}


// async function deletePlan(id) {
//   console.log("Starting to delete plan id ==> " + id);
//   try {
//     const elastic_delete = await elasticClient.indices.delete({
//       index: INDEX_NAME,
//     //  id: id
//     })
//   } catch (error) {
//     if (error) {
//       console.log("error Deleting");
//     } else if (!error) {
//       console.log("Deleted successfully");
//     }
//     else {
//       console.log("Deletion failed");
//       console.log("error", error);
//     }
//   };
// }

async function recreateJSON(id, resJSON) {
  console.log(`inside recreate json`);

  await typeAsync(id).then(async (res) => {
    if (res === "hash") {
      await hgetallAsync(id).then(async (response) => {
        for (key in response) {
          let value = response[key];
          let currentKey = key;
          await typeAsync(value).then(async (res) => {
            if (res === "hash") {
              newHashJSON = {};
              resJSON[currentKey] = newHashJSON;
              await recreateJSON(value, newHashJSON);
            } else if (res === "set") {
              newArr = [];
              resJSON[currentKey] = newArr;
              await recreateJSON(value, newArr);
            } else {
              // when type is null it means it has a simple property
              resJSON[currentKey] = isNaN(value) ? value : parseInt(value);
            }
          });
        }
      });
    } else if (res === "set") {
      await smembersAsync(id).then(async (result) => {
        for (let i = 0; i < result.length; i++) {
          await typeAsync(result[i]).then(async (res) => {
            if (res === "hash") {
              newHashJSON = {};
              resJSON.push(newHashJSON);
              await recreateJSON(result[i], newHashJSON);
            } else if (res === "set") {
              newArr = [];
              resJSON.push(newArr)
              await recreateJSON(result[i], newArr);
            } else {
              // when type is null it means it has a simple property
              resJSON.push(result[i]);
            }
          })
        }
      });
    }
  });
  return resJSON;
}
setup();
module.exports = redisClient;
module.exports = app;
