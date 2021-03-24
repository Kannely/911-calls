//const elasticsearch = require('elasticsearch');
const csv = require('csv-parser');
const fs = require('fs');
const { Client } = require('@elastic/elasticsearch');

const ELASTIC_SEARCH_URI = 'http://localhost:9200';
const INDEX_NAME = '911-calls';

async function run() {
  const client = new Client({ node: ELASTIC_SEARCH_URI});

  // Drop index if exists
  await client.indices.delete({
    index: INDEX_NAME,
    ignore_unavailable: true
  });

  await client.indices.create({
    index: INDEX_NAME,
    body : { // TODO configurer l'index https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping.html
		"mappings": {
			"properties": {
			  "time_stamp":{
				"type": "date" 
			  },		
			  "point": {
				"type": "geo_point"
			  }
			}
		}
    }
  });

  let calls = [];
  fs.createReadStream('../911.csv')
    .pipe(csv())
    .on('data', data => {
      const call = {  // Créer l'objet call à partir de la ligne
                "imdb_id": data.imdb_id,
				"point": [parseFloat(data.lng), parseFloat(data.lat)],
				"desc": data.desc,
                "zip": data.zip,
                "title": data.title,
				"category": data.title.match(/^(.*):/)[1],
                "time_stamp": new Date(data.timeStamp),
                "twp": data.twp,
                "addr": data.addr 
      };
      calls.push(call);
    })
    .on('end', async () => {
      // TODO insérer les données dans ES en utilisant l'API de bulk https://www.elastic.co/guide/en/elasticsearch/reference/7.x/docs-bulk.html
		client.bulk(createBulkInsertQuery(calls), (err, resp) => {
		  if (err) console.trace(err.message);
		  else console.log(`Inserted ${resp.body.items.length} calls`);
		  client.close();
		});	
   });
  

}

run().catch(console.log);

// Fonction utilitaire permettant de formatter les données pour l'insertion "bulk" dans elastic
function createBulkInsertQuery(calls) {
  const body = calls.reduce((acc, call) => {
    const { point, desc, zip, title, category, time_stamp, twp, addr } = call;
    acc.push({ index: { _index: INDEX_NAME, _type: '_doc', _id: call.imdb_id } })
    acc.push({ point, desc, zip, title, category, time_stamp, twp, addr })
    return acc
  }, []);

  return { body };
}


