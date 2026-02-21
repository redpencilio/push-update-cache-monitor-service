# push-update-cache-monitor-service

Microservice to monitor cache invalidation events and construct a push message of that.

Clients (browser tabs) can subscribe to specific URL paths. When the cache layer clears a cache entry, the services pushes notifications to the subscribed tabs via delta messages and the [polling-push-updates-service](https://github.com/mu-semtech/polling-push-updates-service).

## Getting started
### Add the service to your stack
This service assumes a push-updates enabled semantic.works stack including mu-authorization, delta-notifier and the polling-push-updates-service.

Add the following snippet to your `docker-compose.yml`

``` yaml
services:
  push-update-resource-monitor:
    image: redpencil/push-update-cache-monitor
    environment:
      DEFAULT_MU_AUTH_SCOPE: "http://services.semantic.works/push-updates-monitor"
```

Next, add the following config to mu-authorization in `./config/authorization/config.lisp`

``` common-lisp
(define-prefixes
  :mu "http://mu.semte.ch/vocabularies/core/"
  :service "http://services.semantic.works/"
  :push "http://mu.semte.ch/vocabularies/push/"
  :rdf "http://www.w3.org/1999/02/22-rdf-syntax-ns#")

(define-graph push-updates ("http://mu.semte.ch/graphs/push-updates")
  ("push:Update"
    -> "push:target"
    -> "push:message"
    -> "push:channel"
    -> "mu:uuid"
    -> "rdf:type"))

(supply-allowed-group "public")

(with-scope "service:push-updates-monitor"
  (grant (write)
         :to push-updates
         :for "public"))
```

Add a new delta rule to `./config/delta/rules.js`

``` javascript
export default [
  {
    match: {
      // we expect the full body to be sent in this case
      object: { value: "http://mu.semte.ch/vocabularies/cache/Clear" },
    },
    callback: {
      url: "http://push-update-cache-monitor/delta",
      method: "POST",
    },
    options: {
      resourceFormat: "v0.0.1",
      gracePeriod: 100,
      foldEffectiveChanges: false,
      ignoreFromSelf: false,
    },
  },
  {
    match: {
      // we expect the full body to be sent in this case
      object: { value: "http://mu.semte.ch/vocabularies/push/Tab" },
    },
    callback: {
      url: "http://push-update-cache-monitor/delta",
      method: "POST",
    },
    options: {
      resourceFormat: "v0.0.1",
      gracePeriod: 100,
      foldEffectiveChanges: false,
      ignoreFromSelf: false,
    },
  }
]
```

Add a dispatcher rule to `./config/dispatcher/dispatcher.ex`

``` elixir
  @json %{ accept: %{ json: true } }

  match "/cache-monitor/*path", @json do
    Proxy.forward conn, path, "http://push-update-cache-monitor/"
  end
```

Restart database, delta-notifier and dispatcher to pick up their new configuration and start up the new service

``` javascript
docker compose restart database delta-notifier dispatcher
docker compose up -d
```

## Reference
### REST API
#### POST /monitor?path=path&tab=tab
Subscribe a tab (identified via `tab` query param) to monitor a URL path.

Returns 204 No Content on success.

#### DELETE /monitor?path=path&tab=tab
Unsubscribe a tab (identified via `tab` query param) from monitoring a URL path.

Returns 204 No Content on success.

#### POST /delta
Delta handling endpoint listening for cache clear notifications that match tab subscriptions. For each match a `push:Update` is written to the store with the subscribed tab as target.

The endpoint also handles tab disconnections (delete of `push:Tab` resource).

Returns 200 OK on success.


### Data model
#### Prefixes
| Prefix  | URI                                      |
|---------|------------------------------------------|
| `push`  | `http://mu.semte.ch/vocabularies/push/`  |
| `cache` | `http://mu.semte.ch/vocabularies/cache/` |

#### Tabs
##### Class
`push:Tab`
##### Properties
| Name    | Predicate      | Range           | Definition                        |
|---------|----------------|-----------------|-----------------------------------|
| session | `push:session` | `rdfs:Resource` | User's session related to the tab |

#### Push update
##### Class
`push:Update`
##### Properties
| Name    | Predicate      | Range           | Definition                                                                                                            |
|---------|----------------|-----------------|-----------------------------------------------------------------------------------------------------------------------|
| target  | `push:target`  | `push:Tab`      | Target tab of the push update                                                                                         |
| channel | `push:channel` | `rdfs:Resource` | Channel on which push update is published. Always `http://services.semantic.works/cache-monitor` for this service. |
| message | `push:message` | `xsd:string`    | Message that is pushed                                                                                                |

#### Cache clear
##### Class
`cache:Clear`
##### Properties
| Name           | Predicate             | Range        | Definition                                |
|----------------|-----------------------|--------------|-------------------------------------------|
| path           | `cache:path`          | `xsd:string` | URL path of the cleared cache entry       |
| allowed groups | `cache:allowedGroups` | `xsd:string` | Allowed groups of the cleared cache entry |

