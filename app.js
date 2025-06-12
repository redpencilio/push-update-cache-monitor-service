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

app.post('/delta', async function( req, res ) {
  // TODO: also support push:Disconnect

  // We look for anything that is a cache:Clear
  const quadsBySubject = {};
  const insertedQuads = req.body.map( (delta) => delta.inserts ).flat();
  const clearEvents = [];

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
    await update(`PREFIX push: <http://mu.semte.ch/vocabularies/push/>
      PREFIX mu: <http://mu.semte.ch/vocabularies/core/>
      INSERT DATA { ${clearEventTriples} }`);
  }

  res.status(204).send();
});

app.use(errorHandler);
