const express = require("express");
const app = express();
const bodyParser = require("body-parser");
const redis = require("redis");
const schema = require("./schema");
const { v4: uuidv4 } = require("uuid");
const crypto = require("crypto");
var md5 = require('md5');
const REDIS_PORT = 6379;
const client = redis.createClient(REDIS_PORT);
const { promisify } = require("util");
const hgetallAsync = promisify(client.hgetall).bind(client);
const typeAsync = promisify(client.type).bind(client);
const smembersAsync = promisify(client.smembers).bind(client);
const VerifyToken = require('./VerifyToken');
const TokenGenerator = require('./TokenGenerator');
// var elasticsearch = require('elasticsearch');

const WORK_QUEUE = "WORK_QUEUE";
const BACKUP_QUEUE = "BACKUP_QUEUE";



var Validator = require("jsonschema").Validator;
var v = new Validator();

app.use('/auth', TokenGenerator);

app.use(bodyParser.json());

app.use(function (error, req, res, next) {
  if (error instanceof SyntaxError) {
    //Handle SyntaxError here.
    return res.status(400).send({ data: "Syntax Error" });
  } else {
    next();
  }
});

function parseJson(id, jsonData) {
  for (let key in jsonData) {
    // check also if property is not inherited from prototype
    if (jsonData.hasOwnProperty(key)) {
      let value = jsonData[key];
      if (typeof value == "object") {
        if (value instanceof Array) {
          listId = id + "_" + key;
          client.hmset(id, key, listId);

          for (let i = 0; i < value.length; i++) {
            let eachValue = value[i];
            if (typeof eachValue === "object") {
              listInnerId =
                id + "_" + eachValue.objectType + "_" + eachValue.objectId;
              client.sadd(listId, listInnerId);
              parseJson(listInnerId, eachValue);
            } else {
              client.sadd(listId, eachValue);
            }
          }
        } else {
          innerId = id + "_" + value.objectType + "_" + value.objectId;
          client.hmset(id, key, innerId);
          parseJson(innerId, value);
        }
      } else {
        client.hmset(id, key, value);
      }
    }
  }
}


// creating plan
app.post("/v1/plan", VerifyToken, function (req, res) {
  let plan = req.body;
  let validationResult = v.validate(plan, schema);

  if (validationResult.errors.length >= 1) {
    var errors = validationResult.errors.map((error) => {
      const errorResponse = {};
      errorResponse.property = error.property;
      errorResponse.message = error.message;
      errorResponse.name = error.name;
      errorResponse.argument = error.argument;
      errorResponse.stack = error.stack;
      return errorResponse;
    });

    var responseJson = { errors: errors };
    res.status(400).send(responseJson);
  } else {
    let plan_id = plan.objectType + "_" + plan.objectId;

    client.get(plan_id, function (err, response) {
      // generating etag by hashing the request body
      const etag = md5(JSON.stringify(plan));

      if (response === null) {
        plan = parseJson(plan_id, plan);
        client.set("etag_" + plan_id, etag);
        res.setHeader("etag", etag);

        addToQueue(plan_id);

        res.status(201).send({ message: "Plan created successfully with id : " + plan_id });
      } else {
        res
          .status(409)
          .send({ message: "Plan with id : " + plan_id + " already exists" });
      }
    });
  }
});

async function recreateJSON(id, resJSON) {
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

// getting plan
app.get("/v1/plan/:id", VerifyToken, function (req, res) {
  const id = req.params.id;
  const plan_id = "plan_" + id;
  const request_etag = req.headers["if-none-match"];
  console.log(`reques etag`, request_etag);

  client.get("etag_plan_" + id, function (err, response) {
    if (request_etag === response) {
      res.status(304).send();
      // console.log('etag?? value??', response); 
    } else {
      client.get(plan_id, async function (err, response) {
        if (response === null) {
          res.status(404).send({ message: "Plan with id " + id + " doesn't exist!" });
          return;
        } else {
          resJSON = {};
          await recreateJSON(plan_id, resJSON, plan_id);
          // const eTag = "etag_plan_" + id;
          // let newEtag;
          // client.get(eTag, (err, val) => {
          //   val ? 
          //   console.log(val) : err;
          //   newEtag = val;
          //   res.setHeader("eTag", newEtag );
          // });
          // res.setHeader("etag", response);
          res.status(200).send(resJSON);

        }
      });
      console.log(`new etag`, response);
      //set new etag
      res.setHeader("eTag", response);
    }
  });
});

//updating plan with PUT
app.put("/v1/plan/:id", VerifyToken, function (req, res) {
  let plan = req.body;
  let validationResult = v.validate(plan, schema);

  const id = req.params.id;
  const plan_id = "plan_" + id;
  const request_etag = req.headers["if-match"];

  if (validationResult.errors.length >= 1) {
    var errors = validationResult.errors.map((error) => {
      const errorResponse = {};
      errorResponse.property = error.property;
      errorResponse.message = error.message;
      errorResponse.name = error.name;
      errorResponse.argument = error.argument;
      errorResponse.stack = error.stack;
      return errorResponse;
    });

    var responseJson = { errors: errors };
    res.status(400).send(responseJson);
  } else {
    client.get("etag_plan_" + id, function (err, response) {
      if (request_etag === response) {
        client.get(plan_id, async function (err, response) {
          if (response === null) {
            res.status(404).send({ message: "No Plan with id " + id + " found" });
            return;
          } else {

            parseJson(plan_id, plan);
            const newEtag = md5(JSON.stringify(plan));
            client.set("etag_" + plan_id, newEtag);

            addToQueue(plan_id);

            res.setHeader("etag", newEtag);
            res
              .status(200)
              .send({ message: "Plan updated successfully with id : " + plan_id });

          }
        });
      } else {
        res.status(412).send();
      }
    });

  }
})


//patching
app.patch("/v1/plan/:id", VerifyToken, function (req, res) {
  let plan = req.body;

  const id = req.params.id;
  const plan_id = "plan_" + id;
  console.log(`PLAN ID FETCHED?`, plan_id);

  const request_etag = req.headers["if-match"];


  client.get("etag_plan_" + id, function (err, response) {
    if (request_etag === response) {
      client.get(plan_id, async function (err, response) {
        if (response === null) {
          res.status(404).send({ message: "Plan with id " + id + " doesn't exist" });
          return;
        } else {
          parseJson(plan_id, plan);
          const newEtag = md5(JSON.stringify(plan));
          client.set("etag_" + plan_id, newEtag);
          addToQueue(plan_id);
          resJSON = {};
          await recreateJSON(plan_id, resJSON, plan_id);
          res.setHeader("etag", newEtag);
          res
            .status(200)
            .send({ message: "Plan updated successfully with id : " + plan_id, updatedObj: resJSON });
        }
      });
    } else {
      client.get(plan_id, async function (err, response) {
        if (response === null) {
          res.status(404).send({ message: "Plan with id " + id + " doesn't exist" });
          return;
        } else {
          res.status(412).send();
        }
      });
    }
  });

})

//deleting plan
app.delete("/v1/plan/:id", VerifyToken, function (req, res) {
  const id = req.params.id;
  const plan_id = "plan_" + id;
  const etag = "etag_" + id;
  const request_etag = req.headers["if-match"];
  //first get the id requested to delete
  client.keys("*" + plan_id + "*", function (err, result) {
    if (result.length === 0) {
      res.status(404).send({ message: "Plan Id " + id + " does not exists" });
    } else {
      //check if resource has changed or not 
      client.get("etag_plan_" + id, (err, etagValue) => {
        //if etag matched, process to delete
        if (request_etag === etagValue) {
          client.del(result, function (err, response) {
            console.log(`plan_id + "_delete"`, plan_id + "_delete");

            addToQueue(plan_id + "_delete");
            res.status(200).send({ message: "Plan with id " + id + " deleted" });
          })
        } else if (request_etag !== etagValue) {
          res.status(412).send();
          //.send({ message: "Plan with id " + id + " has been modified" });
        }
      })
    }
  })
});


  async function addToQueue(planId) {
  console.log(`queue called`, planId);
   await client.LPUSH(WORK_QUEUE, planId);
   await client.LPUSH(BACKUP_QUEUE, planId);
}

app.listen(3001);
