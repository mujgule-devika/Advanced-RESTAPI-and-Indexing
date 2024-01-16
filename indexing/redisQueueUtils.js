import {redis_client} from "../app.js";
import { Client } from '@elastic/elasticsearch';
import fs from 'fs';

const elastic_client = new Client({
    node: 'https://localhost:9200',
    auth: {
      username: 'elastic',
      password: 'fhhRd9XZmluVr69a15v2'
    },
    tls: {
      ca: fs.readFileSync('/Users/viveksharma/Documents/Northeastern/Study/Big-Data Indexing/INFO7255-Big-Data-Indexing/Server/API/utils/http_ca.crt'),
      rejectUnauthorized: false
    }
  })


export const update_and_get_new_id = async() =>{
    try {
        const promise = await redis_client.incr("id");
        return String(promise);
    } catch (error) {
        console.log("Error getting new id",error);
    }
}

export const create = async(id,input) => {
    try {
        const promise = await redis_client.set(id,JSON.stringify(input));
        try{
            const queue_promise = await add_queue(JSON.stringify(input))
            // console.log(queue_promise)
        } catch(err){
            console.log("Error pushing to queue",err);
        }
        return promise;
    } catch (error) {
        console.log("Error posting data:",error)
    }

}

export const get = async(id) => {
    try {
        const promise = await redis_client.get(id);
        return promise;
    } catch (error) {
        console.log("Error fetching data: " +`${id}`,error)
    }
}

export const del = async(id) =>{
    try {
        const promise = await redis_client.del(id);
        try {
            const del_promise = await add_del_queue(id);
        } catch (error) {
            console.log("Error pushing to del queue",error);
        }
        // try {
        //     const elastic_delete = await elastic_client.delete({
        //         index: 'plan',
        //         id: id
        //     })
        //     console.log("Deleted from elastic",elastic_delete)
        // } catch (error) {
        //     console.log("Error deleting from elastic",error)
        // }
        return promise;
    } catch (error) {
        console.log("Error deleting the id: "+`${id}`,error)
    }
}

export const add_queue = async(data) => {
    try {
        const promise = await redis_client.LPUSH("queue-1",JSON.stringify(data));
        return promise;
    } catch (error) {
        console.log("Error adding to queue",error)
    }
}

export const pop_queue_1 = async() => {
    try {
        const promise = await redis_client.BRPOPLPUSH("queue-1","queue-2",0);
        return promise;
    } catch (error) {
        console.log("Error popping from queue-1",error)
    }
}

export const pop_queue_2 = async() => {
    try {
        const promise = await redis_client.RPOP("queue-2");
        return promise;
    } catch (error) {
        console.log("Error popping from queue-2",error)
    }
}

export const add_del_queue = async(id) => {
    try {
        const promise = await redis_client.LPUSH("queue-del",JSON.stringify(id));
        return promise;
    } catch (error) {
        console.log("Error adding to queue",error)
    }
}

export const pop_del_queue = async() => {
    try {
        const promise = await redis_client.RPOP("queue-del");
        return promise;
    } catch (error) {
        console.log("Error popping from queue-2",error)
    }
}