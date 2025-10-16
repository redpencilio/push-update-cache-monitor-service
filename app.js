import { app, query, errorHandler, sparqlEscapeUri, sparqlEscapeString, uuid, update } from 'mu';
import bodyParser from "body-parser";

app.use(bodyParser.json());

const monitored = {};

async function ensuredAllowedGroups(req,res) {
  if (req.get('mu-auth-allowed-groups')) {
    return req.get('mu-auth-allowed-groups');
  } else {
    await query(`ASK { ?s ?p ?o }`); // any query will do
    return res.get('mu-auth-allowed-groups');
  }
}

app.post('/monitor', async function( req, res ) {
  const fullUrl = req.query.path;
  const allowedGroups = await ensuredAllowedGroups(req, res);

  monitored[allowedGroups] ||= {};
  monitored[allowedGroups][fullUrl] ||= [];
  monitored[allowedGroups][fullUrl].push(req.query.tab);

  res.status(204).send();
});

app.delete('/monitor', async function( req, res ) {
  const fullUrl = req.query.path;
  const allowedGroups = await ensuredAllowedGroups(req, res);
  const tabUri = req.query.tab;

  if (monitored[allowedGroups] && monitored[allowedGroups][fullUrl])
    monitored[allowedGroups][fullUrl] =
      monitored[allowedGroups][fullUrl]
        .filter( (monitoringTab) => monitoringTab !== tabUri );

  res.status(204).send();
});

async function handleCacheClears(insertedQuads) {
  const clearEvents = [];
  const quadsBySubject = {};

  // Make sure each interesting quadsBySubject has an empty array
  insertedQuads
    .filter( (quad) => quad.predicate.value === "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" )
    .filter( (quad) => quad.object.value === "http://mu.semte.ch/vocabularies/cache/Clear" )
    .forEach( (quad) => quadsBySubject[quad.subject.value] = {} );

  // Fill in interesting quadsBySubject
  insertedQuads
    .filter( (quad) => quad.subject.value in quadsBySubject )
    .forEach( (quad) => {
      quadsBySubject[quad.subject.value][quad.predicate.value] = quad.object;
    });

  Object.entries(quadsBySubject)
    .forEach(([_subject,predicates]) => {
    const path = predicates["http://mu.semte.ch/vocabularies/cache/path"]?.value;
    const allowedGroups = predicates["http://mu.semte.ch/vocabularies/cache/allowedGroups"]?.value;

    if( path && allowedGroups ) {
      const allowedGroupsMonitor = monitored[allowedGroups];
      const monitoringTabs = (allowedGroupsMonitor && allowedGroupsMonitor[path]) || [];
      monitoringTabs.forEach( (tab) => clearEvents.push({ tab, path }) );
    } else {
      console.warn(`Received incomplete event path: ${path} allowedGroups: ${allowedGroups}`);
    }
  })

  if( clearEvents.length ) {
    const clearEventTriples =
          clearEvents
          .map( ({path, tab}) => {
      const pushUuid = uuid();
      const pushUri = `http://services.semantic.works/push-updates/${pushUuid}`;

      return `${sparqlEscapeUri(pushUri)}
        a push:Update;
        mu:uuid ${sparqlEscapeString(pushUuid)};
        push:channel <http://services.semantic.works/cache-monitor>;
        push:target ${sparqlEscapeUri(tab)};
        push:message ${sparqlEscapeString(path)}.`
    })
                        .join("\n");
    try {
      await update(`PREFIX push: <http://mu.semte.ch/vocabularies/push/>
      PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
      INSERT DATA { ${clearEventTriples} }`);
    } catch (e) {
      console.error(`Something went wrong processing updates`);
    }
  }
}
async function handleClientDisconnects(deletedQuads) {
  // Make sure each interesting quadsBySubject has an empty array
  deletedQuads
    .filter( (quad) => quad.predicate.value === "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" )
    .filter( (quad) => quad.object.value === "http://mu.semte.ch/vocabularies/push/Tab" );

  // TODO: improve datastructure for detecting what is being monitored by a tab

  // if each tab also knows the allowedGroups fullUri combinations it is monitoring, then we need much much less looping

  const tabUris = deletedQuads.map( (quad) => quad.subject.value );

  for (let allowedGroups of Object.keys(monitored)) {
    for (let fullUrl of Object.keys(monitored[allowedGroups]))
      monitored[allowedGroups][fullUrl] =
        monitored[allowedGroups][fullUrl].filter( (x) => ! tabUris.includes(x) );
  }

  console.log(`Removed tabs`, {tabUris});
}

app.post('/delta', async function( req, res ) {
  // TODO: also support push:Disconnect

  // Two parts:
  // a. when a tab disconnects, we should remove it from the tabs which are monitoring
  // b. we should check the cache:Clear messages and convert them if they match what we need.

  // We look for anything that is a cache:Clear
  const body = req.body;
  console.log(body);

  const insertedQuads = body.map( (delta) => delta.inserts ).flat();
  await handleCacheClears(insertedQuads);
  const deletedQuads = body.map( (delta) => delta.deletes ).flat();
  console.log({deletedQuads});
  await handleClientDisconnects(deletedQuads);

  res.status(204).send();
});

app.use(errorHandler);
