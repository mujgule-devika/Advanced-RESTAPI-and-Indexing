import { Client } from '@elastic/elasticsearch';
import fs from 'fs';
import { createClient } from 'redis';
import * as redis_utils from './redis_commands.js';

const redis_client = createClient();
redis_client.on('error', (err) => console.log('Redis Client Error', err));
await redis_client.connect();

const elastic_client = new Client({
  node: 'https://localhost:9200',
  auth: {
    username: 'elastic',
    password: 'fhhRd9XZmluVr69a15v2'
  },
  tls: {
    ca: fs.readFileSync('./http_ca.crt'),
    rejectUnauthorized: false
  }
})

const setup = async () => {

  // deleting the index from previous run
  try {
    const promise_delete = await elastic_client.indices.delete({
      index: 'plan',
    })
    console.log("Deleted index ", promise_delete)
  } catch (error) {
    console.log("Error deleting index", error)
  }
  // creating the index for new run with mapping and settings
  try {
    const promise_create = await elastic_client.indices.create({
      index: 'plan',
      settings: {
        index: {
          number_of_shards: 1,
          number_of_replicas: 1
        }
      },
      body: {
        mappings: {
          properties: {
            mapping: {
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
    const size = await redis_client.LLEN("queue-del");
    del_queue_size = size;
  } catch (error) {
    console.log("Error getting del-queue size", error);
  }
  if (del_queue_size > 0) {
    console.log("Delete queue size", del_queue_size)
    try {
      let el_id = await redis_utils.pop_del_queue();
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

await setup();
while (true) {
  await listening();
  await delete_data();
} 