const express = require("express");
const http = require("http");
const mongodb = require("mongodb")

const app = express();
const PORT = process.env.PORT;
const VIDEO_STORAGE_HOST = process.env.VIDEO_STORAGE_HOST;
const VIDEO_STORAGE_PORT = parseInt(process.env.VIDEO_STORAGE_PORT);
const DBHOST=process.env.DBHOST
const DBNAME=process.env.DBNAME
const swaggerUi = require('swagger-ui-express')
const jsYaml = require('js-yaml')
const fs = require('fs')

    


function main() {
    return mongodb.MongoClient.connect(DBHOST) // Connect to the database.
        .then(client => {

            const db = client.db(DBNAME);
            const videosCollection = db.collection("videos");
            // Our document is YAML, if yours is JSON, then you can just
            // `const openApiDocument = require('spec/openapi.json')`
            // instead.
            
            // Our document is YAML, if yours is JSON, then you can just
    // `const openApiDocument = require('spec/openapi.json')`
    // instead.
    const openApiDocument = jsYaml.load(
        fs.readFileSync('oas.yaml', 'utf-8'),
    )

    // We can enable the explorer also!
    const options = { explorer: true }

    app.use('/api-docs', swaggerUi.serve,
       swaggerUi.setup(openApiDocument, options));
            
            app.get("/video", (req, res) => {
                if(req.query.id && req.query.id.length === 24) {


                const videoId = new mongodb.ObjectID(req.query.id);

                videosCollection.findOne({ _id: videoId })
                    .then(videoRecord => {
                        if (!videoRecord) {
                            res.sendStatus(404);
                            return;
                        }

                        console.log(`Translated id ${videoId} to path ${videoRecord.videoPath}.`);
        
                        const forwardRequest = http.request( // Forward the request to the video storage microservice.
                            {
                                host: VIDEO_STORAGE_HOST,
                                port: VIDEO_STORAGE_PORT,
                                path:`/video?path=${videoRecord.videoPath}`, // Video path now retrieved from the database.
                                method: 'GET',
                                headers: req.headers
                            }, 
                            forwardResponse => {
                                res.writeHeader(forwardResponse.statusCode, forwardResponse.headers);
                                forwardResponse.pipe(res);
                            }
                        );
                        
                        req.pipe(forwardRequest);
                    })
                    .catch(err => {
                        console.error("Database query failed.");
                        console.error(err && err.stack || err);
                        res.sendStatus(404);
                    });
                } else if (req.query.id && req.query.id.length !== 24) {
                    res.sendStatus(400)
                    res.send('ID must be 24 character!')
                }
                else {
                    res.sendStatus(400)
                    res.send('No ID was provided!')
                }
            });

            //
            // Starts the HTTP server.
            //
            app.listen(PORT, () => {
                console.log(`Microservice listening, please load the data file db-fixture/videos.json into your database before testing this microservice.`);
            });
        });
}

main().then(()=> console.log("Microservice online")).catch(err=> {
    console.error('Microservice failed to start');
    console.error(err && err.stack || err)
})

