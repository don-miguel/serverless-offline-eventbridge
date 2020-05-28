import * as _ from "lodash";
import { v4 as uuid } from "uuid";
import { Lambda, EventBridge } from 'aws-sdk';


export class EventBridgeServer {
    private rules: {[ruleId: string]: any} = {}
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
        this.debug("Configuring routes");
        this.app.use((req, res, next) => {
            res.header("Access-Control-Allow-Origin", "*");
            res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
            next();
        });
        this.app.get("/rules", (req, res) => {
            res.json({status: 200, body: _.values(this.rules)})
        })
        this.app.delete("/rules/:ruleId", (req, res) => {
            this.debug("Deleting Rule: " + JSON.stringify(req.params.ruleId))
            delete this.rules[req.params.ruleId]
            res.json({status: 200, body: "OK"})
        })
        this.app.post("/rules", (req, res) => {

            const events = req.body.events.filter(event => !!event.eventBridge).map(event => {
                return {
                    function: req.body.name, 
                    eventBus: event.eventBridge.eventBus, 
                    pattern: event.eventBridge.pattern,
                    lambdaPort: req.body.lambdaPort
                }
            })
            const results = events.map((event) => this.putRule(event));
            res.json(results)
        })
        this.app.all("/", async (req, res) => {
            const results = await Promise.all(_.map(req.body.Entries, async (event) => this.publishEvent(event)))

            res.json({
                Entries: results.map(result => ({EventId: uuid(), triggeredInvocations: result})),
                FailedEntryCount: 0
            })
            
        });
    }

    publishEvent(event) {
        const transformedKeys = _.reduce(event, (res, item, key) => {
            return {...res, [_.kebabCase(key)]: item}
        }, {})

        const matchedHandlers = _.filter(this.rules, rule => this.shouldTrigger(rule, transformedKeys))
        return Promise.all(matchedHandlers.map(rule => {
            const lambda = new Lambda({endpoint: "http://localhost:" + rule.lambdaPort})
            return lambda.invoke({
                FunctionName: rule.function,
                InvocationType: 'RequestResponse',
                Payload: JSON.stringify(event),
            }).promise().then(res => ({...JSON.parse(res.$response.httpResponse.body.toString()), function: rule.function}))
        }))
    }

    shouldTrigger(rule, event) {
        const formatted = {...event, detail: JSON.parse(event.detail)}
        const pattern: any = {...rule.pattern, "event-bus-name": rule.eventBus}

        const recursiveMatch = (o, s) => {
            if(Array.isArray(s)) {
                return s.includes(o)
            }else if(typeof s == 'string'){
                return o == s
            }else {
                return _.isMatchWith(o, s, recursiveMatch)
            }
        }

        return _.isMatchWith(formatted, pattern, recursiveMatch)
    }

    putRule(ruleConfig: any) {
        const uid = uuid();
        this.rules[uid] = ruleConfig
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