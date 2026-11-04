const EventEmitter = require('events');

class EventEmitterBridge extends EventEmitter {
    constructor(api, adapter) {
        // Simulate a singleton
        if (EventEmitterBridge._instance) {
            return EventEmitterBridge._instance;
        }
        super();
        EventEmitterBridge._instance = this;
        this.homeIds = [];

        if (!adapter.config.iotInstance) {
            adapter.log.warn('Disable Realtime Events because no iot instance configured. Please see Adapter Readme for Details!');
            return;
        }
        this.adapter = adapter;
        this.apiInstance = api;
        adapter.sendTo(adapter.config.iotInstance, 'getServiceEndpoint', {serviceName: 'netatmo'}, result => {
            if (result && result.error) {
                adapter.log.error(`Cannot get service endpoint for callbacks: ${result.error}`);
            }
            if (!result || !result.url || !result.stateID) {
                adapter.log.error(`Cannot get service endpoint for callbacks: ${JSON.stringify(result)}`);
                return;
            }
            this.callbackUrl = result.url;
            this.stateId = result.stateID;
            adapter.subscribeForeignStates(this.stateId);

            this.stateChangeHandler = (id, state) => {
                if (id === this.stateId && state && state.val && state.ack) {
                    try {
                        this.adapter.log.debug(`Websocket incoming alert: ${state.val}`)
                        const obj = JSON.parse(state.val);
                        // @KI_HINWEIS: Test neuer Webhook-Dienst – Ereignisse des neuen Formats sichtbar machen
                        if (obj && ['device_event', 'webhook_activation', 'webhook_deletion', 'topology_changed', 'home_event_changed'].includes(obj.push_type)) {
                            this.adapter.log.info(`[webhook-v1 test] ${state.val}`);
                        }
                        if (obj && this.homeIds.includes(obj.home_id)) {
                            this.emit('alert', obj);
                        }
                    } catch (e) {
                        adapter.log.error(`Cannot parse callback data: ${e.message}`);
                    }
                }
            };
            adapter.on('stateChange', this.stateChangeHandler);

            this.apiInstance.addWebHook(this.callbackUrl, (err, body, qs) => {
                if (err) {
                    this.adapter.log.error(`Error while adding webhook: ${err}`);
                } else {
                    this.adapter.log.info(`Webhook added: ${JSON.stringify(body.toString())}`);
                }
                this.registerWebhookV1();
            });
        });
    }

    /**
     * Test: registers the iot callback URL also at the new Netatmo webhook service (/webhooks/v1).
     */
    registerWebhookV1() {
        if (!this.apiInstance || !this.callbackUrl) {
            return;
        }
        const masked = this.callbackUrl.replace(/([?&](key|token)=)[^&]+/gi, '$1***');
        this.apiInstance.webhooksV1('GET', null, (err, code, body) => {
            this.adapter.log.info(`[webhook-v1 test] GET /webhooks/v1 -> ${err ? err.message : code} ${JSON.stringify(body || '').replace(this.callbackUrl, masked).substring(0, 500)}`);
            const list = body && Array.isArray(body.body) ? body.body : [];
            if (list.some(w => w && w.url === this.callbackUrl)) {
                this.adapter.log.info('[webhook-v1 test] iot URL already registered at /webhooks/v1');
                return;
            }
            this.apiInstance && this.apiInstance.webhooksV1('POST', this.callbackUrl, (err2, code2, body2) => {
                this.adapter.log.info(`[webhook-v1 test] POST /webhooks/v1 (${masked}) -> ${err2 ? err2.message : code2} ${JSON.stringify(body2 || '').replace(this.callbackUrl, masked).substring(0, 500)}`);
            });
        });
    }

    joinHome(id) {
        if (!this.homeIds.includes(id)) {
            this.homeIds.push(id);
        }
    }

    destructor() {
        // @KI_HINWEIS: Singleton zuruecksetzen, sonst liefert der naechste Konstruktor-Aufruf
        // (Re-Init nach erneuter Authentifizierung) die zerstoerte Instanz ohne Webhook zurueck.
        if (EventEmitterBridge._instance === this) {
            EventEmitterBridge._instance = null;
        }
        if (this.stateChangeHandler && this.adapter) {
            this.adapter.removeListener('stateChange', this.stateChangeHandler);
            this.stateChangeHandler = null;
        }
        this.removeAllListeners();
        if (this.stateId && this.adapter) {
            this.adapter.unsubscribeForeignStates(this.stateId);
        }
        this.apiInstance = null;
        this.stateId = null;
        this.callbackUrl = null;
    }
}

module.exports = EventEmitterBridge;
