# RESTful API Design for Insurance Domain

Project for Class Advance Big Data Application / Indexing

Implemented Rest API in NodeJS that can handle any structured data in Json, validate the same and save the Json Schema in Redis and Elastic Search.

## Features

• Implemented Rest API that can handle any structured data in Json using Node and Redis Storage with the ability to GET, PUT, POST and DELETE.

• JSON document will be validated with the defined JSON Schema before storing it in Redis

• Secured Rest endpoints with bearer token generated by JWT

• Implemented PATCH/MERGE/UPDATE at the granular field level to update a particulate field without having to get the entire JSON document thereby saving network bandwidth.

• Involved the concept of ETags for conditional GET and PUT requests using Hashing technique.

• Utilized the REDIS queuing mechanism to enque and deque requests will full backup in a seperate queue before INDEXING the data in Elastic Search.

• Utilized Kibana Server and Sense Console for full text searching of indexed documents in Elastic Search.

### API Endpoints

(A Sample JSON Object for the plan can be found  [here](https://github.com/jayashree1992/Advanced_Big_Data_Indexing/blob/master/usecase.json))

-   GET  `/token`  - This generates a RSA-signed JWT token used to authenticate future requests.
-   POST  `/plan`  - Creates a new plan provided in the request body
-   PUT  `/plan/{id}`  - Updates an existing plan provided by the id
    -   A valid Etag for the object should also be provided in the  `If-Match`  HTTP Request Header
-   PATCH  `/plan/{id}`  - Patches an existing plan provided by the id
    -   A valid Etag for the object should also be provided in the  `If-Match`  HTTP Request Header
-   GET  `/plan/{id}`  - Fetches an existing plan provided by the id
    -   An Etag for the object can be provided in the  `If-None-Match`  HTTP Request Header
    -   If the request is successful, a valid Etag for the object is returned in the  `ETag`  HTTP Response Header
-   DELETE  `/plan/{id}`  - Deletes an existing plan provided by the id
    -   A valid Etag for the object should also be provided in the  `If-Match`  HTTP Request Header

### [](https://github.com/shah-tejas/BigDataIndexing#sample-queries-to-search-the-indexed-data)Sample Queries to search the indexed data:

Sample queries can be fired on the Kibana console to retrieve the indexed data

1.  Query all plans
    
    ```
    GET planindex/_search
    {
      "query": {
        "match_all": {}
      }
    }
    
    ```
    
2.  Query based on a nested object's objectId, alongwith the nested object
    
    ```
    GET planindex/_search
    {
      "query": {
        "nested": {
          "path": "linkedPlanServices.linkedService",
          "query": {
            "match": {
                  "linkedPlanServices.linkedService.objectId": "1234520xvc30asdf-502"
            }
          },
          "inner_hits": {}
        }
      }
    }
    
    ```
    
3.  Query wildcard text fields
    
    ```
    GET planindex/_search
    {
      "query": {
        "wildcard": {
          "_org": {
            "value": "example*"
          }
        }
      }
    }
    
    ```
    
4.  Query nested object's wildcard text fields
    
    ```
    GET planindex/_search
    {
      "query": {
        "nested": {
          "path": "linkedPlanServices.linkedService",
          "query": {
            "wildcard": {
              "linkedPlanServices.linkedService.name.keyword": {
                "value": "Year*"
              }
            }
          },
          "inner_hits": {}
        }
      }
    }
    
    ```
    
5.  Range query on a numeric field
    
    ```
    GET planindex/_search
    {
      "query": {
        "nested": {
          "path": "planCostShares",
          "query": {
            "range": {
              "planCostShares.copay": {
                "gte": 20,
                "lte": 35
              }
            }
          },
          "inner_hits": {}
        }
      }
    }
    ```
    
## Technology Stack

-   Node.js
-   Express.js
-   Redis
-   REST API
-   JSON Web Tokens
-   Elastic Search
-   Kibana
-   Postman (Tool)