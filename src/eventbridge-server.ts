import * as _ from "lodash";
import { v4 as uuid } from "uuid";
import { Lambda } from 'aws-sdk';


export class EventBridgeServer {
    private subscriptions: {[subscriptionId: string]: any} = {}
    private pluginDebug: (msg, context) => void;
    private port: number;
    private server: any;
    private app: any;
    private region: string;
    private accountId: string;
    constructor(debug, app, region, accountId) {
        this.pluginDebug = debug;
        this.app = app;
        this.region = region;
        this.routes();
        this.accountId = accountId;
    }

    public routes() {
        this.debug("configuring routes");
        this.app.use((req, res, next) => {
            res.header("Access-Control-Allow-Origin", "*");
            res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
            next();
        });
        this.app.get("/subscriptions", (req, res) => {
            res.json({status: 200, body: _.values(this.subscriptions)})
        })
        this.app.delete("/subscriptions/:subscriptionId", (req, res) => {
            this.debug("Removing Subscription: " + JSON.stringify(req.params.subscriptionId))
            delete this.subscriptions[req.params.subscriptionId]
            res.json({status: 200, body: "OK"})
        })
        this.app.post("/subscriptions", (req, res) => {
            this.debug("New Subscription: " + JSON.stringify(req.body.events))

            const events = req.body.events.filter(event => !!event.eventBridge).map(event => {
                return {
                    function: req.body.name, 
                    eventBus: event.eventBridge.eventBus, 
                    pattern: event.eventBridge.pattern,
                    lambdaPort: req.body.lambdaPort
                }
            })
            const results = events.map((event) => this.subscribe(event));
            res.json(results)
        })
        this.app.all("/", async (req, res) => {
            const results = await Promise.all(_.map(req.body.Entries, async (event) => this.publishEvent(event)))
            this.debug(JSON.stringify(results))

            res.json({
                Entries: results.map(result => ({EventId: uuid(), triggeredInvocations: result})),
                FailedEntryCount: 0
            })
            
        });
    }

    publishEvent(event) {
        const matchedHandlers = _.filter(this.subscriptions, sub => this.matchForSubscription(sub)(event))
        return Promise.all(matchedHandlers.map(subscription => {
            const lambda = new Lambda({endpoint: "http://localhost:" + subscription.lambdaPort})
            return lambda.invoke({
                FunctionName: subscription.function,
                InvocationType: 'RequestResponse',
                Payload: JSON.stringify(event),
            }).promise().then(res => ({body: JSON.parse(res.$response.httpResponse.body.toString()), function: subscription.function}))
        }))
    }

    matchForSubscription(subscription) {
        const p: any = {..._.reduce(subscription.pattern, (result, obj, key) => {
            return {...result, [_.capitalize(key)]: obj}
        }, {}), EventBusName: subscription.eventBus}
        return (event) => {
            return p.Source.includes(event.Source) && p.EventBusName === event.EventBusName && (!p["Detail-type"] || p["Detail-type"].includes(event.DetailType))
        }
    }

    subscribe(subscriptionConfig: any) {
        const uid = uuid();
        this.subscriptions[uid] = subscriptionConfig
        return uid
    }

    public debug(msg) {
        if (msg instanceof Object) {
          try {
              msg = JSON.stringify(msg);
          } catch (ex) {}
        }
        this.pluginDebug(msg, "server");
      }
}