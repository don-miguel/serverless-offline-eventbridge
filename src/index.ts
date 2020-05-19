import * as express from "express";
import * as cors from "cors";
import * as bodyParser from "body-parser";
import * as _ from 'lodash';
import * as AWS from "aws-sdk";
import {
    EventBridgeServer
} from "./eventbridge-server";
import fetch from "node-fetch";
import { waterfall } from "promise-async";


class ServerlessOfflineEventbridge {
    commands: any;
    hooks: any;
    app: any;
    config: any;
    region: string;
    localPort: number;
    accountId: string;
    eventbridgeServer: EventBridgeServer;
    private server: any;
    subscriptions: any[] = []

    constructor(private serverless: any, private options: any) {
        this.app = express();
        this.app.use(cors());
        this.app.use((req, res, next) => {
            // fix for https://github.com/s12v/sns/issues/45 not sending content-type
            req.headers["content-type"] = req.headers["content-type"] || "text/plain";
            next();
        });
        this.app.use(bodyParser.json({
            type: ["application/x-amz-json-1.1", "application/json", "text/plain"],
            limit: "10mb"
        }));
        this.commands = {
            "offline-eventbridge": {
                usage: "Listens to offline EventBridge events and passes them to configured Lambda fns",
                lifecycleEvents: [
                    "start",
                    "cleanup",
                ],
                commands: {
                    start: {
                        lifecycleEvents: [
                            "init",
                            "end",
                        ],
                    },
                    cleanup: {
                        lifecycleEvents: [
                            "init",
                        ],
                    },
                },
            },
        };
        this.hooks = {
            "before:offline:start": () => this.start(),
            "before:offline:start:init": () => this.start(),
            "before:offline:start:end": async () => this.stop(),
            "offline-eventbridge:start:init": () => {
                this.start();
                return this.waitForSigint();
            },
            "offline-eventbridge:cleanup:init": async () => {
                this.init();
                return this.unsubscribeAll();
            },
            "offline-eventbridge:start:end": () => this.stop(),
        };
    }

    async start() {
        this.log("Starting EventBridge Server")
        this.init();
        this.serve()
        waterfall([
            (next) => {
                this.listen().then(next).catch(next)
            },
            (next) => this.subscribeAll().then(next).catch(next)
        ]).then(res => this.onReady()).catch(err => this.log(err))
    }

    onReady() {
        this.log("EventBridge Server Started");
        process.send && process.send("DEPENDENCY:READY")
    }

    async stop() {
        if (this.server) {
            this.server.close();
        }
        await this.unsubscribeAll()
    }

    init() {
        process.env = _.extend({}, this.serverless.service.provider.environment, process.env);
        this.config = {host: "localhost", port: 4002, ...this.serverless.service.custom["serverless-offline-eventbridge"]};
        this.region = this.serverless.service.provider.region;
        this.localPort = this.config.port;
        this.accountId = this.config.accountId || "123456789012";

        AWS.config.eventbridge = {
            endpoint: "http://127.0.0.1:" + this.localPort,
            region: this.region,
        };
    }

    public serve() {
        this.eventbridgeServer = new EventBridgeServer((msg, ctx) => this.debug(msg, ctx), this.app, this.region, this.accountId);
    }

    public async listen() {
        let host = this.config.host;
        return new Promise(res => {
            this.server = this.app.listen({
                port: this.localPort,
                host
            }, () => {
                this.log(`Listening on ${host}:${this.localPort}`);
                res();
            });
            this.server.setTimeout(0);
            this.server.on('error', (e) => {
                if (e.code === 'EADDRINUSE') {
                //   this.log(`ADDRESS IN USE: ${JSON.stringify(e, null, 2)}`)
                  res()
                }
            });

        })
    }

    public async subscribeAll() {
        await Promise.all(Object.keys(this.serverless.service.functions).map(fnName => {
            const fn = this.serverless.service.functions[fnName];
            return Promise.all(fn.events.filter(event => event.eventBridge != null).map(event => {
                return this.subscribe(fnName);
            }));
        }));
    }

    public async subscribe(fnName) {
        const lambdaPort = this.serverless.service.custom['serverless-offline'].lambdaPort
        if(!lambdaPort) throw "lambdaPort not defined for serverless-offline. EventBridge won't know where to call it"
        const fn = this.serverless.service.functions[fnName];

        const result = await fetch(`http://${this.config.host}:${this.localPort}/subscriptions`, {
            method: "POST",
            body: JSON.stringify({
                ...fn,
                lambdaPort: lambdaPort
            }),
            timeout: 0,
            headers: {
                "Content-Type": "application/json; charset=UTF-8",
                "Content-Length": Buffer.byteLength(JSON.stringify(fn)),
            },
        }).then(res => res.json())
        this.subscriptions.push(...result)
        this.debug(`Subscribed Function: ${fn.name} @ port ${lambdaPort}`)
    }

    async unsubscribeAll() {
        await Promise.all(_.map(_.flatten(this.subscriptions), subscription => {
            return fetch(`http://${this.config.host || "localhost"}:${this.localPort}/subscriptions/${subscription}`, {
                method: "DELETE",
                timeout: 0
            })
        })).catch(err => {
            Promise.resolve({err})
        })
    }

    public async waitForSigint() {
        return new Promise(res => {
            process.on("SIGINT", () => {
                this.log("Halting offline-eventbridge server");
                res();
            });
        });
    }

    public log(msg, prefix = "INFO[serverless-offline-eventbridge]: ") {
        if(msg instanceof Object) {
            msg = JSON.stringify(msg, null, 2)
        }
        this.serverless.cli.log.call(this.serverless.cli, prefix + msg);
    }

    public debug(msg, context ? : string) {
        if (this.config.debug) {
            if (context) {
                this.log(msg, `DEBUG[serverless-offline-eventbridge][${context}]: `);
            } else {
                this.log(msg, "DEBUG[serverless-offline-eventbridge]: ");
            }
        }
    }
}

module.exports = ServerlessOfflineEventbridge